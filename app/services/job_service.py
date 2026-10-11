from __future__ import annotations  # a method named `list` would shadow list[...] in later annotations

import logging
from datetime import timedelta

from sqlalchemy import func
from sqlalchemy.exc import IntegrityError

from ..exceptions import ConflictError, NotFoundError, ValidationError
from ..extensions import db
from ..models.job import Job, JobStatus
from ..repositories.job_repository import JobRepository
from ..schemas.job import (
    JobAttemptOut,
    JobCreate,
    JobDetailOut,
    JobFilter,
    JobOut,
    QueueCounts,
    QueueStats,
    RetryManyOut,
    TaskOut,
)
from ..worker.registry import TaskRegistry

logger = logging.getLogger(__name__)


class JobService:
    def __init__(self, registry: TaskRegistry) -> None:
        self.jobs = JobRepository()
        self.registry = registry

    def enqueue(self, data: JobCreate) -> tuple[JobOut, bool]:
        """Returns (job, created). A repeated idempotency key returns the original job."""
        known = self.registry.describe()
        if data.task not in known:
            raise ValidationError("Unknown task", {"task": data.task, "known_tasks": list(known)})
        with db.session() as s:
            if data.idempotency_key:
                existing = self.jobs.get_by_idempotency_key(s, data.idempotency_key)
                if existing:
                    return JobOut.model_validate(existing), False
            job = Job(
                queue=data.queue,
                task=data.task,
                payload=data.payload,
                max_attempts=data.max_attempts,
                run_at=func.now() + timedelta(seconds=data.delay_seconds),
                idempotency_key=data.idempotency_key,
            )
            self.jobs.add(s, job)
            try:
                s.flush()  # surfaces a duplicate key from a parallel request
            except IntegrityError:
                s.rollback()
                existing = self.jobs.get_by_idempotency_key(s, data.idempotency_key or "")
                if existing is None:
                    raise ConflictError("Could not enqueue job, please retry") from None
                return JobOut.model_validate(existing), False
            logger.info("job_enqueued", extra={"job_id": job.id, "code": job.task})
            return JobOut.model_validate(job), True

    def get(self, job_id: int) -> JobDetailOut:
        with db.session() as s:
            job = self.jobs.get(s, job_id)
            if job is None:
                raise NotFoundError("Job not found", {"job_id": job_id})
            history = [JobAttemptOut.model_validate(a) for a in self.jobs.attempts_for(s, job_id)]
            return JobDetailOut(**JobOut.model_validate(job).model_dump(), attempt_history=history)

    def list(self, f: JobFilter) -> list[JobOut]:
        with db.session() as s:
            jobs = self.jobs.list(s, f.status, f.task, f.queue, f.limit, f.offset)
            return [JobOut.model_validate(j) for j in jobs]

    def retry(self, job_id: int) -> JobOut:
        """Move a dead job back to queued with a fresh attempt budget."""
        with db.session() as s:
            job = self.jobs.get_for_update(s, job_id)
            if job is None:
                raise NotFoundError("Job not found", {"job_id": job_id})
            if job.status != JobStatus.DEAD.value:
                raise ConflictError(f"Only dead jobs can be retried; this job is {job.status}")
            self._requeue(job)
            s.flush()
            logger.info("job_retried", extra={"job_id": job.id})
            return JobOut.model_validate(job)

    def retry_many(self, job_ids: list[int]) -> RetryManyOut:
        retried, skipped = [], []
        with db.session() as s:
            for job_id in sorted(set(job_ids)):  # sorted: consistent lock order, no deadlocks
                job = self.jobs.get_for_update(s, job_id)
                if job is None or job.status != JobStatus.DEAD.value:
                    skipped.append(job_id)
                    continue
                self._requeue(job)
                retried.append(job_id)
        return RetryManyOut(retried=retried, skipped=skipped)

    def stats(self) -> QueueStats:
        with db.session() as s:
            rows = self.jobs.counts_by_queue_and_status(s)
            done_last_minute = self.jobs.done_in_last_seconds(s, 60)
        per_queue: dict[str, QueueCounts] = {}
        totals = QueueCounts(queue="all")
        for queue, status, count in rows:
            counts = per_queue.setdefault(queue, QueueCounts(queue=queue))
            setattr(counts, status, count)
            setattr(totals, status, getattr(totals, status) + count)
        return QueueStats(queues=list(per_queue.values()), totals=totals, done_last_minute=done_last_minute)

    def tasks(self) -> list[TaskOut]:
        return [TaskOut(name=name, description=desc) for name, desc in self.registry.describe().items()]

    @staticmethod
    def _requeue(job: Job) -> None:
        job.status = JobStatus.QUEUED.value
        job.attempts = 0
        job.run_at = func.now()
        job.locked_by = None
        job.heartbeat_at = None

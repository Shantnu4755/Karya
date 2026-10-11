from __future__ import annotations  # a method named `list` would shadow list[...] in later annotations

from dataclasses import dataclass

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from ..models.job import Job, JobAttempt, JobStatus


@dataclass(frozen=True)
class ClaimedJob:
    id: int
    task: str
    payload: dict
    attempts: int
    max_attempts: int


CLAIM_SQL = text("""
    UPDATE jobs
       SET status = 'running', locked_by = :worker, heartbeat_at = now(),
           attempts = attempts + 1
     WHERE id = (
            SELECT id FROM jobs
             WHERE queue = :queue AND status = 'queued' AND run_at <= now()
             ORDER BY run_at
             LIMIT 1
             FOR UPDATE SKIP LOCKED
     )
 RETURNING id, task, payload, attempts, max_attempts
""")

START_ATTEMPT_SQL = text("""
    INSERT INTO job_attempts (job_id, attempt, worker, started_at)
    VALUES (:id, :attempt, :worker, now())
""")

# Every write below checks locked_by: a worker that lost its lease (reaped, then re-claimed by
# another worker) must not overwrite the new owner's state.
DONE_SQL = text("""
    UPDATE jobs SET status = 'done', locked_by = NULL
     WHERE id = :id AND locked_by = :worker AND status = 'running'
""")

FAIL_SQL = text("""
    UPDATE jobs
       SET status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
           run_at = now() + make_interval(secs => LEAST(power(2, attempts) * 5, 3600)),
           last_error = :error, locked_by = NULL
     WHERE id = :id AND locked_by = :worker AND status = 'running'
""")

FINISH_ATTEMPT_SQL = text("""
    UPDATE job_attempts SET finished_at = now(), outcome = :outcome, error = :error
     WHERE job_id = :id AND worker = :worker AND finished_at IS NULL
""")

HEARTBEAT_SQL = text("""
    UPDATE jobs SET heartbeat_at = now()
     WHERE id = :id AND locked_by = :worker AND status = 'running'
""")

# A job whose attempts are used up goes to dead, so a job that crashes its worker every
# time cannot be re-queued forever.
REAP_SQL = text("""
    UPDATE jobs
       SET status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
           locked_by = NULL, last_error = 'worker stopped sending heartbeats'
     WHERE status = 'running' AND heartbeat_at < now() - interval '60 seconds'
 RETURNING id
""")

LOSE_ATTEMPTS_SQL = text("""
    UPDATE job_attempts
       SET finished_at = now(), outcome = 'lost', error = 'worker stopped sending heartbeats'
     WHERE job_id = ANY(:ids) AND finished_at IS NULL
""")


class JobRepository:
    # --- worker side ---------------------------------------------------------------
    def claim_next(self, s: Session, queue: str, worker: str) -> ClaimedJob | None:
        row = s.execute(CLAIM_SQL, {"queue": queue, "worker": worker}).mappings().first()
        if not row:
            return None
        s.execute(START_ATTEMPT_SQL, {"id": row["id"], "attempt": row["attempts"], "worker": worker})
        return ClaimedJob(**row)

    def mark_done(self, s: Session, job_id: int, worker: str) -> bool:
        """Returns False when the worker no longer holds the job (its lease was reaped)."""
        held = s.execute(DONE_SQL, {"id": job_id, "worker": worker}).rowcount == 1
        if held:
            s.execute(FINISH_ATTEMPT_SQL, {"id": job_id, "worker": worker, "outcome": "done", "error": None})
        return held

    def mark_failed(self, s: Session, job_id: int, worker: str, error: str) -> bool:
        error = error[:1000]
        held = s.execute(FAIL_SQL, {"id": job_id, "worker": worker, "error": error}).rowcount == 1
        if held:
            params = {"id": job_id, "worker": worker, "outcome": "failed", "error": error}
            s.execute(FINISH_ATTEMPT_SQL, params)
        return held

    def heartbeat(self, s: Session, job_id: int, worker: str) -> bool:
        return s.execute(HEARTBEAT_SQL, {"id": job_id, "worker": worker}).rowcount == 1

    def reap_stale(self, s: Session) -> int:
        ids = list(s.execute(REAP_SQL).scalars())
        if ids:
            s.execute(LOSE_ATTEMPTS_SQL, {"ids": ids})
        return len(ids)

    # --- API side ------------------------------------------------------------------
    def add(self, s: Session, job: Job) -> None:
        s.add(job)

    def get(self, s: Session, job_id: int) -> Job | None:
        return s.get(Job, job_id)

    def get_for_update(self, s: Session, job_id: int) -> Job | None:
        return s.execute(select(Job).where(Job.id == job_id).with_for_update()).scalar_one_or_none()

    def get_by_idempotency_key(self, s: Session, key: str) -> Job | None:
        return s.execute(select(Job).where(Job.idempotency_key == key)).scalar_one_or_none()

    def list(
        self,
        s: Session,
        status: JobStatus | None,
        task: str | None,
        queue: str | None,
        limit: int,
        offset: int,
    ) -> list[Job]:
        stmt = select(Job).order_by(Job.id.desc()).limit(limit).offset(offset)
        if status:
            stmt = stmt.where(Job.status == status.value)
        if task:
            stmt = stmt.where(Job.task == task)
        if queue:
            stmt = stmt.where(Job.queue == queue)
        return list(s.execute(stmt).scalars())

    def attempts_for(self, s: Session, job_id: int) -> list[JobAttempt]:
        stmt = select(JobAttempt).where(JobAttempt.job_id == job_id).order_by(JobAttempt.id)
        return list(s.execute(stmt).scalars())

    def counts_by_queue_and_status(self, s: Session) -> list[tuple[str, str, int]]:
        stmt = select(Job.queue, Job.status, func.count()).group_by(Job.queue, Job.status).order_by(Job.queue)
        return [(queue, status, count) for queue, status, count in s.execute(stmt).all()]

    def done_in_last_seconds(self, s: Session, seconds: int) -> int:
        stmt = select(func.count()).where(
            JobAttempt.outcome == "done",
            JobAttempt.finished_at >= func.now() - func.make_interval(0, 0, 0, 0, 0, 0, seconds),
        )
        return s.execute(stmt).scalar_one()

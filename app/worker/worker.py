import logging
import os
import signal
import socket
import threading

from ..extensions import db
from ..liveness import mark_alive
from ..repositories.job_repository import ClaimedJob, JobRepository
from .registry import TaskRegistry

logger = logging.getLogger(__name__)


class Heartbeat(threading.Thread):
    def __init__(self, repo: JobRepository, job_id: int, worker_id: str, interval: float = 15.0) -> None:
        super().__init__(daemon=True)
        self.repo, self.job_id, self.worker_id, self.interval = repo, job_id, worker_id, interval
        self._stop_event = threading.Event()  # not _stop: Thread uses that name internally

    def run(self) -> None:
        while not self._stop_event.wait(self.interval):
            mark_alive()
            try:
                with db.session() as s:
                    self.repo.heartbeat(s, self.job_id, self.worker_id)
            except Exception:
                logger.exception("heartbeat_failed", extra={"job_id": self.job_id})

    def stop(self) -> None:
        self._stop_event.set()


class Worker:
    def __init__(
        self,
        queue: str,
        registry: TaskRegistry,
        poll_interval: float = 1.0,
        worker_id: str | None = None,
        heartbeat_interval: float = 15.0,
    ) -> None:
        self.queue = queue
        self.registry = registry
        self.poll_interval = poll_interval
        self.worker_id = worker_id or f"{socket.gethostname()}-{os.getpid()}"
        self.heartbeat_interval = heartbeat_interval
        self.repo = JobRepository()
        self._stopping = threading.Event()

    def run(self) -> None:
        signal.signal(signal.SIGTERM, self._request_stop)
        signal.signal(signal.SIGINT, self._request_stop)
        logger.info("worker_started", extra={"service": self.worker_id})
        loops = 0
        while not self._stopping.is_set():
            mark_alive()
            loops += 1
            if loops % 30 == 0:
                self._reap()
            job = self._claim()
            if job is None:
                self._stopping.wait(self.poll_interval)
                continue
            self._execute(job)
        logger.info("worker_stopped", extra={"service": self.worker_id})

    def _claim(self) -> ClaimedJob | None:
        try:
            with db.session() as s:
                return self.repo.claim_next(s, self.queue, self.worker_id)
        except Exception:
            logger.exception("claim_failed")
            self._stopping.wait(5)  # database hiccup: back off, do not crash
            return None

    def _execute(self, job: ClaimedJob) -> None:
        heartbeat = Heartbeat(self.repo, job.id, self.worker_id, self.heartbeat_interval)
        heartbeat.start()
        try:
            self.registry.get(job.task)(job.payload)
            with db.session() as s:
                held = self.repo.mark_done(s, job.id, self.worker_id)
            if held:
                logger.info("job_done", extra={"job_id": job.id})
            else:
                logger.warning("job_lease_lost", extra={"job_id": job.id})
        except Exception as exc:
            logger.exception("job_failed", extra={"job_id": job.id})
            with db.session() as s:
                self.repo.mark_failed(s, job.id, self.worker_id, f"{type(exc).__name__}: {exc}")
        finally:
            heartbeat.stop()

    def _reap(self) -> None:
        try:
            with db.session() as s:
                count = self.repo.reap_stale(s)
            if count:
                logger.warning("reaped_stale_jobs", extra={"code": count})
        except Exception:
            logger.exception("reap_failed")

    def _request_stop(self, *_: object) -> None:
        logger.info("shutdown_requested")
        self._stopping.set()  # current job finishes, then the loop exits

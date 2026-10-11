from sqlalchemy import text

from app.extensions import db
from app.schemas.job import JobCreate, JobOut
from app.services.job_service import JobService
from app.worker import registry as default_registry
from app.worker.registry import TaskRegistry


def enqueue(task: str = "demo.sleep", registry: TaskRegistry | None = None, **fields) -> JobOut:
    fields.setdefault("payload", {"seconds": 0} if task == "demo.sleep" else {})
    job, _ = JobService(registry or default_registry).enqueue(JobCreate(task=task, **fields))
    return job


def job_row(job_id: int) -> dict:
    with db.session() as s:
        row = s.execute(text("SELECT * FROM jobs WHERE id = :id"), {"id": job_id}).mappings().one()
        return dict(row)


def seconds_until_run(job_id: int) -> float:
    with db.session() as s:
        sql = text("SELECT extract(epoch FROM run_at - now()) FROM jobs WHERE id = :id")
        return float(s.execute(sql, {"id": job_id}).scalar_one())


def make_due(job_id: int) -> None:
    """Skip the backoff wait in tests."""
    with db.session() as s:
        s.execute(text("UPDATE jobs SET run_at = now() WHERE id = :id"), {"id": job_id})


def stop_heartbeats(job_id: int, seconds: int = 61) -> None:
    """Simulate a worker that died mid-job: no heartbeat for `seconds`."""
    with db.session() as s:
        sql = text("UPDATE jobs SET heartbeat_at = now() - make_interval(secs => :secs) WHERE id = :id")
        s.execute(sql, {"id": job_id, "secs": seconds})

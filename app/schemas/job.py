from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from ..models.job import JobStatus

QUEUE_PATTERN = r"^[a-z0-9][a-z0-9_.-]{0,63}$"


class JobCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    queue: str = Field(default="default", pattern=QUEUE_PATTERN)
    task: str = Field(min_length=1, max_length=128)
    payload: dict = Field(default_factory=dict)
    delay_seconds: int = Field(default=0, ge=0, le=86_400)
    max_attempts: int = Field(default=5, ge=1, le=20)
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=64)


class JobFilter(BaseModel):
    status: JobStatus | None = None
    task: str | None = Field(default=None, max_length=128)
    queue: str | None = Field(default=None, pattern=QUEUE_PATTERN)
    limit: int = Field(default=20, ge=1, le=100)
    offset: int = Field(default=0, ge=0)


class JobRetryMany(BaseModel):
    model_config = ConfigDict(extra="forbid")
    job_ids: list[int] = Field(min_length=1, max_length=100)


class JobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    queue: str
    task: str
    payload: dict
    status: JobStatus
    attempts: int
    max_attempts: int
    run_at: datetime
    locked_by: str | None
    heartbeat_at: datetime | None
    last_error: str | None
    idempotency_key: str | None
    created_at: datetime


class JobAttemptOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    attempt: int
    worker: str
    started_at: datetime
    finished_at: datetime | None
    outcome: str | None
    error: str | None


class JobDetailOut(JobOut):
    attempt_history: list[JobAttemptOut]


class RetryManyOut(BaseModel):
    retried: list[int]
    skipped: list[int]  # not found or not dead


class QueueCounts(BaseModel):
    queue: str
    queued: int = 0
    running: int = 0
    done: int = 0
    dead: int = 0


class QueueStats(BaseModel):
    queues: list[QueueCounts]
    totals: QueueCounts
    done_last_minute: int


class TaskOut(BaseModel):
    name: str
    description: str

from ..schemas.job import JobCreate, JobFilter, JobRetryMany
from ..services.job_service import JobService
from ..worker import registry
from .base import BaseView


class JobListView(BaseView):
    def __init__(self) -> None:
        self.service = JobService(registry)

    def get(self):
        jobs = self.service.list(self.parse_query(JobFilter))
        return {"data": [j.model_dump(mode="json") for j in jobs]}, 200

    def post(self):
        job, created = self.service.enqueue(self.parse_body(JobCreate))
        return {"data": job.model_dump(mode="json")}, (201 if created else 200)


class JobDetailView(BaseView):
    def __init__(self) -> None:
        self.service = JobService(registry)

    def get(self, job_id: int):
        return {"data": self.service.get(job_id).model_dump(mode="json")}, 200


class JobRetryView(BaseView):
    def __init__(self) -> None:
        self.service = JobService(registry)

    def post(self, job_id: int):
        return {"data": self.service.retry(job_id).model_dump(mode="json")}, 200


class JobBulkRetryView(BaseView):
    def __init__(self) -> None:
        self.service = JobService(registry)

    def post(self):
        result = self.service.retry_many(self.parse_body(JobRetryMany).job_ids)
        return {"data": result.model_dump(mode="json")}, 200


class QueueStatsView(BaseView):
    def __init__(self) -> None:
        self.service = JobService(registry)

    def get(self):
        return {"data": self.service.stats().model_dump(mode="json")}, 200


class TaskListView(BaseView):
    def __init__(self) -> None:
        self.service = JobService(registry)

    def get(self):
        return {"data": [t.model_dump(mode="json") for t in self.service.tasks()]}, 200

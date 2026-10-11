from flask import Flask

from .health import HealthView
from .jobs import JobBulkRetryView, JobDetailView, JobListView, JobRetryView, QueueStatsView, TaskListView


def register_routes(app: Flask) -> None:
    app.add_url_rule("/health", view_func=HealthView.as_view("health"))
    # project-specific routes are added below this line
    app.add_url_rule("/jobs", view_func=JobListView.as_view("jobs"))
    app.add_url_rule("/jobs/retry", view_func=JobBulkRetryView.as_view("jobs_retry"))
    app.add_url_rule("/jobs/<int:job_id>", view_func=JobDetailView.as_view("job_detail"))
    app.add_url_rule("/jobs/<int:job_id>/retry", view_func=JobRetryView.as_view("job_retry"))
    app.add_url_rule("/queues/stats", view_func=QueueStatsView.as_view("queue_stats"))
    app.add_url_rule("/tasks", view_func=TaskListView.as_view("tasks"))

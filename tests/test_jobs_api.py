import threading
import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select

from app.extensions import db
from app.models.job import Job
from app.schemas.job import JobCreate
from app.services.job_service import JobService
from app.worker import registry
from app.worker.worker import Worker
from tests.helpers import enqueue, job_row


def _post(client, **body):
    body.setdefault("task", "demo.sleep")
    return client.post("/jobs", json=body)


def _job_count() -> int:
    with db.session() as s:
        return s.execute(select(func.count()).select_from(Job)).scalar_one()


def _kill(job_id: int) -> None:
    worker = Worker("default", registry, worker_id="w1")
    while (job := worker._claim()) is not None:
        worker._execute(job)
    assert job_row(job_id)["status"] == "dead"


def test_enqueue_and_fetch(client):
    resp = _post(client, payload={"seconds": 0}, max_attempts=2)
    assert resp.status_code == 201
    job = resp.get_json()["data"]
    assert (job["status"], job["attempts"], job["max_attempts"], job["queue"]) == ("queued", 0, 2, "default")
    detail = client.get(f"/jobs/{job['id']}").get_json()["data"]
    assert detail["attempt_history"] == []


def test_enqueue_is_idempotent(client):
    first = _post(client, idempotency_key="kavach:evt-1")
    second = _post(client, idempotency_key="kavach:evt-1")
    assert (first.status_code, second.status_code) == (201, 200)
    assert first.get_json()["data"]["id"] == second.get_json()["data"]["id"]
    assert _job_count() == 1


def test_parallel_enqueue_with_one_key_creates_one_job(app):
    """Request A inserted the key but has not committed; B inserts the same key and must get A's job."""
    a_flushed = threading.Event()
    result: dict[str, object] = {}

    def request_a() -> None:
        with db.session() as s:
            job = Job(queue="default", task="demo.sleep", payload={}, idempotency_key="race")
            s.add(job)
            s.flush()
            result["winner"] = job.id
            a_flushed.set()
            threading.Event().wait(0.5)

    def request_b() -> None:
        a_flushed.wait()
        result["b"] = JobService(registry).enqueue(JobCreate(task="demo.sleep", idempotency_key="race"))

    threads = [threading.Thread(target=request_a), threading.Thread(target=request_b)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    job, created = result["b"]
    assert (job.id, created) == (result["winner"], False)
    assert _job_count() == 1


def test_delay_schedules_the_job_later(client):
    job = _post(client, delay_seconds=120).get_json()["data"]
    run_at = datetime.fromisoformat(job["run_at"])
    assert run_at > datetime.now(UTC) + timedelta(seconds=110)


def test_enqueue_validation(client):
    unknown = _post(client, task="does.not.exist")
    assert unknown.status_code == 422
    assert "demo.sleep" in unknown.get_json()["error"]["details"]["known_tasks"]
    assert _post(client, queue="Bad Queue!").status_code == 422
    assert _post(client, max_attempts=0).status_code == 422
    assert _post(client, delay_seconds=-1).status_code == 422
    assert _post(client, priority=1).status_code == 422  # extra fields are rejected


def test_list_filters_and_pagination(client):
    sleep = enqueue("demo.sleep")
    fail = enqueue("demo.fail", max_attempts=1)
    worker = Worker("default", registry, worker_id="w1")
    while (job := worker._claim()) is not None:
        worker._execute(job)

    def ids(query: str) -> list[int]:
        return [j["id"] for j in client.get(f"/jobs{query}").get_json()["data"]]

    assert ids("?status=dead") == [fail.id]
    assert ids("?status=done") == [sleep.id]
    assert ids("?task=demo.sleep") == [sleep.id]
    assert ids("?limit=1") == [fail.id]
    assert ids("?limit=1&offset=1") == [sleep.id]
    assert ids("?queue=other") == []
    assert client.get("/jobs?status=lost").status_code == 422


def test_retry_dead_job(client):
    job = enqueue("demo.fail", max_attempts=1)
    _kill(job.id)
    resp = client.post(f"/jobs/{job.id}/retry")
    assert resp.status_code == 200
    data = resp.get_json()["data"]
    assert (data["status"], data["attempts"]) == ("queued", 0)
    again = client.post(f"/jobs/{job.id}/retry")
    assert again.status_code == 409
    assert client.post("/jobs/999999/retry").status_code == 404


def test_bulk_retry(client):
    dead = [enqueue("demo.fail", max_attempts=1) for _ in range(2)]
    for job in dead:
        _kill(job.id)
    alive = enqueue()
    resp = client.post("/jobs/retry", json={"job_ids": [dead[0].id, dead[1].id, alive.id, 999999]})
    result = resp.get_json()["data"]
    assert result == {"retried": sorted(j.id for j in dead), "skipped": [alive.id, 999999]}
    assert client.post("/jobs/retry", json={"job_ids": []}).status_code == 422


def test_unknown_job_is_404(client):
    assert client.get("/jobs/424242").status_code == 404


def test_queue_stats(client):
    enqueue()
    enqueue(queue="emails")
    third = enqueue()
    worker = Worker("default", registry, worker_id="w1")
    worker._execute(worker._claim())  # oldest first: the first job
    stats = client.get("/queues/stats").get_json()["data"]
    by_queue = {q["queue"]: q for q in stats["queues"]}
    assert by_queue["default"]["done"] == 1 and by_queue["default"]["queued"] == 1
    assert by_queue["emails"]["queued"] == 1
    assert stats["totals"] == {"queue": "all", "queued": 2, "running": 0, "done": 1, "dead": 0}
    assert stats["done_last_minute"] == 1
    assert job_row(third.id)["status"] == "queued"


def test_task_list(client):
    names = [t["name"] for t in client.get("/tasks").get_json()["data"]]
    assert names == ["demo.fail", "demo.flaky", "demo.sleep", "kavach.claim_event"]


def test_kavach_relay_contract(client):
    """The exact body Kavach's outbox relay sends must be accepted, and a resend de-duplicated."""
    event_id = str(uuid.uuid4())
    body = {
        "queue": "default",
        "task": "kavach.claim_event",
        "payload": {
            "event_id": event_id,
            "event_type": "claim.submitted",
            "claim_id": "c1",
            "amount": "10.00",
        },
        "idempotency_key": f"kavach:{event_id}",
    }
    assert client.post("/jobs", json=body).status_code == 201
    assert client.post("/jobs", json=body).status_code == 200
    worker = Worker("default", registry, worker_id="w1")
    job = worker._claim()
    worker._execute(job)
    assert job_row(job.id)["status"] == "done"

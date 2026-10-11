import os
import signal
import threading
import time
from unittest.mock import patch

import pytest

from app.extensions import db
from app.models.job import Job
from app.repositories.job_repository import JobRepository
from app.services.job_service import JobService
from app.worker import registry
from app.worker.registry import TaskRegistry
from app.worker.worker import Heartbeat, Worker
from tests.helpers import enqueue, job_row, make_due, seconds_until_run, stop_heartbeats


def _worker(worker_id: str, reg: TaskRegistry = registry) -> Worker:
    return Worker("default", reg, poll_interval=0.01, worker_id=worker_id)


# --- PDF "Tests to write" ---------------------------------------------------------------


def test_two_workers_claiming_at_once_never_get_the_same_job(app):
    ids = {enqueue().id for _ in range(40)}
    workers = [_worker("w1"), _worker("w2")]
    claimed: dict[str, list[int]] = {"w1": [], "w2": []}
    barrier = threading.Barrier(2)

    def drain(w: Worker) -> None:
        barrier.wait()
        while (job := w._claim()) is not None:
            claimed[w.worker_id].append(job.id)

    threads = [threading.Thread(target=drain, args=(w,)) for w in workers]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert set(claimed["w1"]).isdisjoint(claimed["w2"])
    assert set(claimed["w1"]) | set(claimed["w2"]) == ids


def test_failing_job_backs_off_then_goes_dead(app):
    job = enqueue("demo.fail", payload={"message": "boom"}, max_attempts=3)
    worker = _worker("w1")
    expected_delays = {1: 10, 2: 20}  # 2^attempts * 5 seconds
    for attempt in (1, 2, 3):
        claimed = worker._claim()
        assert (claimed.id, claimed.attempts) == (job.id, attempt)
        worker._execute(claimed)
        row = job_row(job.id)
        if attempt < 3:
            assert row["status"] == "queued"
            assert seconds_until_run(job.id) == pytest.approx(expected_delays[attempt], abs=1.5)
            assert worker._claim() is None  # not due yet
            make_due(job.id)
    row = job_row(job.id)
    assert (row["status"], row["attempts"], row["last_error"]) == ("dead", 3, "RuntimeError: boom")
    history = JobService(registry).get(job.id).attempt_history
    assert [(a.attempt, a.outcome) for a in history] == [(1, "failed"), (2, "failed"), (3, "failed")]


def test_job_whose_heartbeat_stops_is_requeued_by_the_reaper(app):
    job = enqueue()
    crashed = _worker("w1")
    assert crashed._claim().id == job.id
    stop_heartbeats(job.id)

    survivor = _worker("w2")
    with db.session() as s:
        assert JobRepository().reap_stale(s) == 1
    row = job_row(job.id)
    assert (row["status"], row["locked_by"]) == ("queued", None)
    assert row["last_error"] == "worker stopped sending heartbeats"

    reclaimed = survivor._claim()
    assert (reclaimed.id, reclaimed.attempts) == (job.id, 2)
    history = JobService(registry).get(job.id).attempt_history
    assert [(a.worker, a.outcome) for a in history] == [("w1", "lost"), ("w2", None)]


def test_sigterm_lets_the_current_job_finish(app):
    reg = TaskRegistry()
    events: list[str] = []

    @reg.register("test.deploy")
    def long_job(payload: dict) -> None:
        os.kill(os.getpid(), signal.SIGTERM)  # a deploy stops the container mid-job
        time.sleep(0.2)
        events.append(f"finished {payload['n']}")

    first = enqueue("test.deploy", registry=reg, payload={"n": 1})
    second = enqueue("test.deploy", registry=reg, payload={"n": 2})
    previous = signal.getsignal(signal.SIGTERM), signal.getsignal(signal.SIGINT)
    try:
        _worker("w1", reg).run()  # returns only after the stop request
    finally:
        signal.signal(signal.SIGTERM, previous[0])
        signal.signal(signal.SIGINT, previous[1])
    assert events == ["finished 1"]
    assert job_row(first.id)["status"] == "done"
    assert job_row(second.id)["status"] == "queued"  # never started


# --- leases, poison jobs, heartbeats --------------------------------------------------------


def test_worker_that_lost_its_lease_cannot_overwrite_the_new_owner(app):
    job = enqueue()
    slow, fast = _worker("slow"), _worker("fast")
    slow_job = slow._claim()
    stop_heartbeats(job.id)
    fast._reap()
    assert fast._claim().id == job.id
    slow._execute(slow_job)  # finishes late: must not mark fast's run as done
    row = job_row(job.id)
    assert (row["status"], row["locked_by"]) == ("running", "fast")


def test_reaper_sends_a_job_with_no_attempts_left_to_dead(app):
    job = enqueue(max_attempts=1)
    _worker("w1")._claim()
    stop_heartbeats(job.id)
    _worker("w2")._reap()
    assert job_row(job.id)["status"] == "dead"


def test_fresh_heartbeats_are_not_reaped(app):
    job = enqueue()
    _worker("w1")._claim()
    stop_heartbeats(job.id, seconds=30)
    with db.session() as s:
        assert JobRepository().reap_stale(s) == 0
    assert job_row(job.id)["status"] == "running"


def test_heartbeat_thread_keeps_the_lease_fresh(app):
    job = enqueue()
    _worker("w1")._claim()
    stop_heartbeats(job.id, seconds=50)
    beat = Heartbeat(JobRepository(), job.id, "w1", interval=0.02)
    beat.start()
    time.sleep(0.15)
    beat.stop()
    beat.join()
    with db.session() as s:
        assert JobRepository().reap_stale(s) == 0
        assert JobRepository().heartbeat(s, job.id, "someone-else") is False


def test_many_workers_execute_every_job_exactly_once(app):
    reg = TaskRegistry()
    executed: list[int] = []
    lock = threading.Lock()

    @reg.register("test.record")
    def record(payload: dict) -> None:
        with lock:
            executed.append(payload["n"])

    for n in range(120):
        enqueue("test.record", registry=reg, payload={"n": n})
    workers = [_worker(f"w{i}", reg) for i in range(4)]

    def drain(w: Worker) -> None:
        while (job := w._claim()) is not None:
            w._execute(job)

    threads = [threading.Thread(target=drain, args=(w,)) for w in workers]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(executed) == list(range(120))  # no job twice, none missed


def test_job_for_a_removed_task_fails_with_a_clear_error(app):
    with db.session() as s:
        job = Job(queue="default", task="gone.task", payload={}, max_attempts=1)
        s.add(job)
        s.flush()
        job_id = job.id
    worker = _worker("w1")
    worker._execute(worker._claim())
    row = job_row(job_id)
    assert (row["status"], row["last_error"]) == ("dead", "NotFoundError: No handler for task 'gone.task'")


def test_claim_survives_a_database_error(app):
    worker = _worker("w1")
    worker._stopping.set()  # makes the 5 s back-off return immediately
    with patch.object(worker.repo, "claim_next", side_effect=RuntimeError("db down")):
        assert worker._claim() is None


def test_reap_survives_a_database_error(app):
    worker = _worker("w1")
    with patch.object(worker.repo, "reap_stale", side_effect=RuntimeError("db down")):
        worker._reap()  # logged, not raised


def test_run_loop_reaps_periodically(app):
    worker = _worker("w1")
    calls = {"claims": 0}

    def fake_claim():
        calls["claims"] += 1
        if calls["claims"] >= 31:
            worker._stopping.set()
        return None

    with patch.object(worker, "_claim", side_effect=fake_claim), patch.object(worker, "_reap") as reap, \
            patch("app.worker.worker.signal.signal"):
        worker.run()
    assert reap.call_count == 1


def test_worker_entry_point(monkeypatch):
    from app.worker import __main__ as entry

    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://u:p@localhost/db")
    monkeypatch.setenv("WORKER_QUEUE", "emails")
    with patch.object(entry, "db") as fake_db, patch.object(entry, "Worker") as fake_worker:
        entry.main()
    fake_db.init_app.assert_called_once()
    assert fake_worker.call_args.kwargs["queue"] == "emails"
    fake_worker.return_value.run.assert_called_once()


def test_worker_loop_and_heartbeat_mark_the_process_alive(app, tmp_path):
    alive = tmp_path / "alive"
    worker = _worker("w1")
    worker._stopping.set()
    with patch("app.liveness.LIVENESS_FILE", alive), patch("app.worker.worker.signal.signal"):
        worker._stopping.clear()
        with patch.object(worker, "_claim", side_effect=lambda: worker._stopping.set()):
            worker.run()
    assert alive.exists()


def test_liveness_touch_failure_is_logged_not_raised(tmp_path):
    from app.liveness import mark_alive

    with patch("app.liveness.LIVENESS_FILE", tmp_path / "missing-dir" / "alive"):
        mark_alive()

import pytest
from pydantic import ValidationError

from app.exceptions import NotFoundError
from app.worker import registry
from app.worker.registry import TaskRegistry


def test_duplicate_registration_is_rejected():
    reg = TaskRegistry()
    reg.register("a")(lambda payload: None)
    with pytest.raises(ValueError):
        reg.register("a")(lambda payload: None)


def test_unknown_task_raises_not_found():
    with pytest.raises(NotFoundError):
        TaskRegistry().get("missing")


def test_demo_tasks():
    registry.get("demo.sleep")({"seconds": 0})
    registry.get("demo.flaky")({"fail_rate": 0})
    with pytest.raises(RuntimeError, match="boom"):
        registry.get("demo.fail")({"message": "boom"})
    with pytest.raises(RuntimeError):
        registry.get("demo.flaky")({"fail_rate": 1})
    with pytest.raises(ValidationError):
        registry.get("demo.sleep")({"seconds": 999})


def test_claim_event_task_validates_its_payload():
    registry.get("kavach.claim_event")({"event_id": "e1", "event_type": "claim.paid", "claim_id": "c1"})
    with pytest.raises(ValidationError):
        registry.get("kavach.claim_event")({"event_type": "claim.paid"})

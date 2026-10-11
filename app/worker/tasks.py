"""Built-in task handlers. Handlers must be idempotent: delivery is at least once."""

import logging
import random
import time

from pydantic import BaseModel, ConfigDict, Field

from .registry import registry

logger = logging.getLogger(__name__)


class ClaimEventPayload(BaseModel):
    model_config = ConfigDict(extra="allow")
    event_id: str
    event_type: str
    claim_id: str


class SleepPayload(BaseModel):
    seconds: float = Field(default=1.0, ge=0, le=30)


class FailPayload(BaseModel):
    message: str = Field(default="demo failure", max_length=200)


class FlakyPayload(BaseModel):
    fail_rate: float = Field(default=0.5, ge=0, le=1)


@registry.register("kavach.claim_event", "Handle a Kavach outbox event (claim submitted, approved, ...)")
def handle_claim_event(payload: dict) -> None:
    event = ClaimEventPayload.model_validate(payload)
    # Stand-in for a real side effect (notify the customer, post to a ledger), keyed by event_id.
    logger.info("claim_event_handled", extra={"code": event.event_type, "service": event.claim_id})


@registry.register("demo.sleep", "Sleep for `seconds` (0 to 30), then succeed")
def demo_sleep(payload: dict) -> None:
    time.sleep(SleepPayload.model_validate(payload).seconds)


@registry.register("demo.fail", "Always fail; shows retries with backoff, then dead-letter")
def demo_fail(payload: dict) -> None:
    raise RuntimeError(FailPayload.model_validate(payload).message)


@registry.register("demo.flaky", "Fail with probability `fail_rate` (0 to 1)")
def demo_flaky(payload: dict) -> None:
    if random.random() < FlakyPayload.model_validate(payload).fail_rate:
        raise RuntimeError("flaky task failed this time")

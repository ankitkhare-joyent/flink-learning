"""The EC2 state-change event contract: parse what AWS sends, reject what does not fit.

EventBridge delivers this JSON to SQS for every EC2 state transition (real AWS shape):

    {"version": "0", "id": "7bf73129-...", "detail-type": "EC2 Instance State-change Notification",
     "source": "aws.ec2", "account": "123456789012", "time": "2026-10-04T07:00:00Z",
     "region": "us-east-1", "resources": ["arn:aws:ec2:us-east-1:123456789012:instance/i-0abc..."],
     "detail": {"instance-id": "i-0abc...", "state": "running"}}

This module is pure (no I/O) so the contract is unit-tested without Kafka or SQS.
"""
import json
import re
from datetime import UTC, datetime

STATES = ("pending", "running", "stopping", "stopped", "shutting-down", "terminated")
DETAIL_TYPE = "EC2 Instance State-change Notification"

_INSTANCE = re.compile(r"^i-[0-9a-f]{8,17}$")
_ACCOUNT = re.compile(r"^\d{12}$")
_REGION = re.compile(r"^[a-z]{2}(-gov)?-[a-z]+-\d$")


class InvalidEvent(ValueError):
    """The message can never be processed (bad JSON, wrong shape). Route it to the DLQ; do not retry."""


def parse_time(value: str) -> datetime:
    try:
        ts = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        raise InvalidEvent(f"bad time: {value!r}") from None
    if ts.tzinfo is None:
        raise InvalidEvent(f"time has no timezone: {value!r}")
    return ts.astimezone(UTC)


def normalize(body: str, *, trace_id: str | None = None, now: datetime | None = None) -> dict:
    """EventBridge JSON -> the Avro record in schemas/ec2_state_change.avsc."""
    try:
        evt = json.loads(body)
    except json.JSONDecodeError as e:
        raise InvalidEvent(f"not JSON: {e.msg}") from None
    if not isinstance(evt, dict):
        raise InvalidEvent("not a JSON object")
    if evt.get("detail-type") != DETAIL_TYPE or evt.get("source") != "aws.ec2":
        raise InvalidEvent(f"unexpected event type: {evt.get('source')}/{evt.get('detail-type')}")
    detail = evt.get("detail")
    if not isinstance(detail, dict):
        raise InvalidEvent("missing detail")

    instance_id, state = detail.get("instance-id"), detail.get("state")
    account, region, event_id = evt.get("account"), evt.get("region"), evt.get("id")
    if not isinstance(event_id, str) or not event_id:
        raise InvalidEvent("missing id")
    if not isinstance(instance_id, str) or not _INSTANCE.match(instance_id):
        raise InvalidEvent(f"bad instance-id: {instance_id!r}")
    if state not in STATES:
        raise InvalidEvent(f"bad state: {state!r}")
    if not isinstance(account, str) or not _ACCOUNT.match(account):
        raise InvalidEvent(f"bad account: {account!r}")
    if not isinstance(region, str) or not _REGION.match(region):
        raise InvalidEvent(f"bad region: {region!r}")

    return {
        "event_id": event_id,
        "event_time": parse_time(evt.get("time")),
        "account_id": account,
        "region": region,
        "instance_id": instance_id,
        "state": state,
        "ingest_time": now or datetime.now(UTC),
        "source": "aws.ec2",
        "trace_id": trace_id,
    }


def eventbridge_event(event_id: str, ts: datetime, account: str, region: str, instance_id: str, state: str) -> dict:
    """Build an event exactly as EventBridge would (used by the simulator and tests)."""
    return {
        "version": "0",
        "id": event_id,
        "detail-type": DETAIL_TYPE,
        "source": "aws.ec2",
        "account": account,
        "time": ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "region": region,
        "resources": [f"arn:aws:ec2:{region}:{account}:instance/{instance_id}"],
        "detail": {"instance-id": instance_id, "state": state},
    }

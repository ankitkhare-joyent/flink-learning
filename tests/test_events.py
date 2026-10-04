"""The event contract: what the bridge accepts, what it sends to the DLQ, and that it fits the Avro schema."""
import json
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path

import fastavro
import pytest

from services.ingest.events import InvalidEvent, eventbridge_event, normalize

SCHEMA = fastavro.parse_schema(json.loads((Path(__file__).parents[1] / "schemas/ec2_state_change.avsc").read_text()))
T = datetime(2026, 10, 4, 7, 0, 0, 123000, tzinfo=UTC)


def body(**overrides) -> str:
    evt = eventbridge_event("e-1", T, "123456789012", "us-east-1", "i-0123456789abcdef0", "running")
    for k, v in overrides.items():
        if k == "detail":
            evt["detail"] = v
        else:
            evt[k.replace("_", "-")] = v
    return json.dumps(evt)


def test_normalizes_a_real_eventbridge_event():
    rec = normalize(body(), trace_id="m-1", now=T)
    assert rec == {"event_id": "e-1", "event_time": T, "account_id": "123456789012", "region": "us-east-1",
                   "instance_id": "i-0123456789abcdef0", "state": "running", "ingest_time": T, "source": "aws.ec2",
                   "trace_id": "m-1"}


def test_record_round_trips_through_the_avro_schema():
    rec = normalize(body(), now=T)
    buf = BytesIO()
    fastavro.schemaless_writer(buf, SCHEMA, rec)
    buf.seek(0)
    back = fastavro.schemaless_reader(buf, SCHEMA)
    assert back["event_time"] == T and back["instance_id"] == rec["instance_id"]


def test_time_is_normalised_to_utc():
    rec = normalize(body(time="2026-10-04T09:00:00+02:00"))
    assert rec["event_time"] == datetime(2026, 10, 4, 7, 0, tzinfo=UTC)


@pytest.mark.parametrize("bad", [
    "{not json",
    "[]",
    body(detail_type="EC2 Spot Instance Interruption Warning"),
    body(source="aws.s3"),
    body(detail={"instance-id": "i-0123456789abcdef0", "state": "rebooting"}),
    body(detail={"instance-id": "vol-123", "state": "running"}),
    body(detail="nope"),
    body(account="12345"),
    body(region="mars-1"),
    body(time="yesterday"),
    body(time="2026-10-04T07:00:00"),      # no timezone: ambiguous, rejected
    body(time=None),
    body(id=""),
])
def test_rejects_what_cannot_be_processed(bad):
    with pytest.raises(InvalidEvent):
        normalize(bad)

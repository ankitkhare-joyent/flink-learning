"""SQS (ElasticMQ) from your laptop, for the labs.

    mise run -q lab:sqs -- depth          # visible / in-flight / delayed messages, main queue and its DLQ
    mise run -q lab:sqs -- peek [n]       # show n messages without consuming them (visibility timeout 0)
    mise run -q lab:sqs -- send-junk      # send one poison message (truncated JSON)
    mise run -q lab:sqs -- send-event i-0abc... stopped [SECONDS_AGO]   # one real-shaped event, optionally back-dated
"""
import json
import sys
import uuid
from datetime import UTC, datetime, timedelta

import boto3
import psycopg

from services.ingest.events import eventbridge_event

ENDPOINT = "http://localhost:19324"
sqs = boto3.client("sqs", endpoint_url=ENDPOINT, region_name="us-east-1",
                   aws_access_key_id="local", aws_secret_access_key="local")


def url(name: str) -> str:
    return sqs.get_queue_url(QueueName=name)["QueueUrl"]


def depth() -> None:
    for q in ("ec2-state-change", "ec2-state-change-dlq"):
        a = sqs.get_queue_attributes(QueueUrl=url(q), AttributeNames=["All"])["Attributes"]
        print(f"{q:<22} visible={a['ApproximateNumberOfMessages']:>5}  "
              f"in_flight={a['ApproximateNumberOfMessagesNotVisible']:>4}  delayed={a['ApproximateNumberOfMessagesDelayed']}")


def peek(n: int) -> None:
    msgs = sqs.receive_message(QueueUrl=url("ec2-state-change"), MaxNumberOfMessages=min(n, 10), VisibilityTimeout=0,
                               WaitTimeSeconds=1, MessageSystemAttributeNames=["ApproximateReceiveCount"]).get("Messages", [])
    for m in msgs:
        body = json.loads(m["Body"]) if m["Body"].startswith("{") and m["Body"].endswith("}") else m["Body"]
        if isinstance(body, dict):
            print(json.dumps({"id": body.get("id"), "time": body.get("time"), "detail": body.get("detail"),
                              "receive_count": m["Attributes"]["ApproximateReceiveCount"]}))
        else:
            print("non-JSON body:", m["Body"][:80])
    if not msgs:
        print("queue is empty right now (the bridge keeps up): stop it to see messages pile up")


def send_junk() -> None:
    out = sqs.send_message(QueueUrl=url("ec2-state-change"), MessageBody='{"version":"0","id":"lab-junk","detail-ty')
    print("sent poison message", out["MessageId"])


def send_event(instance_id: str, state: str, seconds_ago: float) -> None:
    # Account and region come from the CMDB row, as AWS would report them (superuser: a lab shortcut).
    with psycopg.connect("postgresql://postgres:postgres-local@localhost:15432/cmdb") as c:
        row = c.execute("SELECT account_id, region FROM ec2_ci WHERE instance_id = %s", (instance_id,)).fetchone()
    if row is None:
        raise SystemExit(f"{instance_id} is not in the CMDB")
    ts = datetime.now(UTC) - timedelta(seconds=seconds_ago)
    evt = eventbridge_event(f"lab-{uuid.uuid4()}", ts, row[0], row[1], instance_id, state)
    sqs.send_message(QueueUrl=url("ec2-state-change"), MessageBody=json.dumps(evt))
    print(f"sent {state} for {instance_id} with event time {evt['time']} (id {evt['id']})")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "depth"
    {"depth": depth, "peek": lambda: peek(int(sys.argv[2]) if len(sys.argv) > 2 else 3), "send-junk": send_junk,
     "send-event": lambda: send_event(sys.argv[2], sys.argv[3], float(sys.argv[4]) if len(sys.argv) > 4 else 0)}[cmd]()

"""SQS -> Kafka bridge for EC2 state-change events.

    EventBridge rule -> SQS queue -> [this service] -> Kafka topic ec2.state-changes (Avro, key = instance_id)

Why a bridge at all? SQS is a *queue*: a message is gone once one consumer deletes it, and there is no
replay. Kafka is a *log*: many independent consumers (Flink, the lakehouse, an agent) each read at their
own pace, and you can rewind. AWS integrations (EventBridge, S3 notifications) speak SQS, so production
systems commonly land SQS into Kafka exactly like this (or with a Kafka Connect SQS source connector).

Delivery guarantee: at-least-once, end to end.
  1. Receive up to 10 messages (long poll). They become invisible to other bridge replicas.
  2. Produce each to Kafka with an idempotent producer (acks=all).
  3. Only after Kafka acknowledges a record is its SQS message deleted.
  4. If we crash between 2 and 3, SQS re-delivers after the visibility timeout -> a duplicate in Kafka.
     That is expected: the Flink job de-duplicates on event_id. Never "fix" this by deleting first;
     that turns duplicates into data loss.
Poison messages (bad JSON, wrong shape) go to a Kafka DLQ topic with the reason in headers, then are
deleted. Transient failures (Kafka down) are *not* deleted, so SQS retries them and, after
maxReceiveCount, moves them to the SQS dead-letter queue.
"""
import time
from pathlib import Path

import boto3
from botocore.config import Config
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer
from prometheus_client import Counter, Histogram
from pydantic_settings import BaseSettings

from services.common.observability import Shutdown, configure_logging, start_metrics_server
from services.common.topics import STATE_CHANGES, STATE_CHANGES_DLQ
from services.ingest.events import InvalidEvent, normalize

SCHEMA = Path(__file__).resolve().parents[2] / "schemas" / "ec2_state_change.avsc"


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:19092"
    schema_registry_url: str = "http://localhost:18085"
    sqs_endpoint: str | None = "http://localhost:19324"   # unset in AWS: boto3 uses the regional endpoint
    sqs_queue_name: str = "ec2-state-change"
    visibility_timeout_seconds: int = 60
    wait_time_seconds: int = 20
    metrics_port: int = 9400
    log_level: str = "INFO"


RECEIVED = Counter("bridge_sqs_messages_received_total", "Messages received from SQS")
PRODUCED = Counter("bridge_kafka_records_produced_total", "Records acknowledged by Kafka", ["topic"])
FAILED = Counter("bridge_kafka_produce_failures_total", "Records Kafka failed to accept (left in SQS)")
INVALID = Counter("bridge_invalid_events_total", "Messages rejected by the contract (sent to DLQ)")
DELETED = Counter("bridge_sqs_messages_deleted_total", "Messages deleted from SQS after Kafka ack")
EVENT_AGE = Histogram("bridge_event_age_seconds", "Seconds from AWS event time to Kafka write",
                      buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60, 300, 900))


class Bridge:
    def __init__(self, s: Settings, log):
        self.s, self.log = s, log
        self.sqs = boto3.client("sqs", endpoint_url=s.sqs_endpoint,
                                config=Config(retries={"mode": "adaptive", "max_attempts": 10}))
        self.queue_url = self.sqs.get_queue_url(QueueName=s.sqs_queue_name)["QueueUrl"]
        registry = SchemaRegistryClient({"url": s.schema_registry_url})
        # Producers never register schemas in production: CI registers them (scripts/init_streaming.py here),
        # and the registry enforces compatibility. A producer with an unregistered schema fails fast.
        self.serialize = AvroSerializer(registry, SCHEMA.read_text(),
                                        conf={"auto.register.schemas": False, "use.latest.version": False})
        self.key = StringSerializer("utf_8")
        self.producer = Producer({
            "bootstrap.servers": s.kafka_bootstrap_servers,
            "client.id": "sqs-bridge",
            "enable.idempotence": True,      # no duplicates or reordering from producer retries
            "acks": "all",
            "compression.type": "zstd",
            "linger.ms": 20,                 # small batching window: throughput for ~20 ms of latency
            "delivery.timeout.ms": 60000,
        })

    # ------------------------------------------------------------------ one receive -> produce -> delete cycle
    def run_once(self) -> int:
        resp = self.sqs.receive_message(
            QueueUrl=self.queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=self.s.wait_time_seconds,
            VisibilityTimeout=self.s.visibility_timeout_seconds, MessageSystemAttributeNames=["ApproximateReceiveCount"])
        messages = resp.get("Messages", [])
        if not messages:
            return 0
        RECEIVED.inc(len(messages))

        acked: dict[str, str] = {}     # MessageId -> ReceiptHandle, filled by delivery callbacks

        def on_delivery(msg_id: str, receipt: str, topic: str):
            def cb(err, _msg):
                if err is None:
                    acked[msg_id] = receipt
                    PRODUCED.labels(topic).inc()
                else:
                    FAILED.inc()
                    self.log.warning("kafka produce failed; leaving message in SQS for retry",
                                     sqs_message_id=msg_id, error=str(err))
            return cb

        for m in messages:
            msg_id, receipt = m["MessageId"], m["ReceiptHandle"]
            try:
                record = normalize(m["Body"], trace_id=msg_id)
            except InvalidEvent as e:
                INVALID.inc()
                self.log.warning("invalid event -> DLQ", sqs_message_id=msg_id, reason=str(e))
                self.producer.produce(STATE_CHANGES_DLQ.name, value=m["Body"].encode(), key=msg_id.encode(),
                                      headers={"error": str(e), "source.queue": self.s.sqs_queue_name,
                                               "receive.count": m["Attributes"]["ApproximateReceiveCount"]},
                                      on_delivery=on_delivery(msg_id, receipt, STATE_CHANGES_DLQ.name))
                continue
            ctx = SerializationContext(STATE_CHANGES.name, MessageField.VALUE)
            self.producer.produce(STATE_CHANGES.name, key=self.key(record["instance_id"]),
                                  value=self.serialize(record, ctx),
                                  on_delivery=on_delivery(msg_id, receipt, STATE_CHANGES.name))
            EVENT_AGE.observe((record["ingest_time"] - record["event_time"]).total_seconds())

        remaining = self.producer.flush(30)   # wait for acks; callbacks run inside flush()
        if remaining:
            self.log.error("kafka did not ack in time; messages stay in SQS", unacked=remaining)

        if acked:
            entries = [{"Id": str(i), "ReceiptHandle": r} for i, r in enumerate(acked.values())]
            out = self.sqs.delete_message_batch(QueueUrl=self.queue_url, Entries=entries)
            DELETED.inc(len(out.get("Successful", [])))
            for f in out.get("Failed", []):   # harmless: the message comes back and is de-duplicated downstream
                self.log.warning("sqs delete failed", detail=f)
        return len(messages)

    def run(self, shutdown: Shutdown) -> None:
        self.log.info("bridge started", queue=self.queue_url, topic=STATE_CHANGES.name)
        backoff = 1.0
        while not shutdown.requested:
            try:
                self.run_once()
                backoff = 1.0
            except Exception:
                self.log.exception("cycle failed; backing off", backoff_seconds=backoff)
                if shutdown.wait(backoff):
                    break
                backoff = min(backoff * 2, 30)
        self.producer.flush(30)
        self.log.info("bridge stopped")


def main() -> None:
    s = Settings()
    log = configure_logging("sqs-bridge", s.log_level)
    start_metrics_server(s.metrics_port)
    shutdown = Shutdown()
    for attempt in range(30):   # the queue and schema may not exist for a few seconds on a cold start
        try:
            bridge = Bridge(s, log)
            break
        except Exception as e:  # noqa: BLE001
            log.info("waiting for dependencies", error=str(e), attempt=attempt)
            time.sleep(2)
    else:
        raise SystemExit("dependencies never became ready")
    bridge.run(shutdown)


if __name__ == "__main__":
    main()


"""Idempotent bootstrap for the streaming stack. Safe to run on every `docker compose up`.

  1. S3 buckets: `lakehouse` (Iceberg data) and `flink` (checkpoints, savepoints)
  2. Lakekeeper: bootstrap + the `lakehouse` warehouse (same steps as ../learn-ai-agent)
  3. Iceberg namespace `ec2_stream` and its tables, with hidden day partitioning
  4. Kafka topics, created explicitly with partitions, retention and compaction chosen on purpose
  5. The Avro schema for ec2.state-changes, registered with BACKWARD compatibility
  6. The Debezium connector that streams the CMDB table into Kafka

In production each step belongs to a different owner: Terraform/OpenTofu for buckets and topics
(e.g. the Mongey/kafka provider), CI for schema registration, a GitOps-managed KafkaConnector resource
(Strimzi) for Debezium, and migrations for table DDL.
"""
import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path

import pyarrow.fs as pafs
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import IntegerType, LongType, NestedField, StringType, TimestamptzType

from services.common import topics
from services.common.secrets import load_secrets

ROOT = Path(__file__).resolve().parents[1]
LK = os.environ.get("ICEBERG_REST_URI", "http://localhost:18181/catalog").rsplit("/catalog", 1)[0]
S3 = os.environ.get("S3_ENDPOINT", "http://localhost:19000")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
REGISTRY = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:18085")
CONNECT = os.environ.get("CONNECT_URL", "http://localhost:18083")
NAMESPACE = "ec2_stream"


def http(method: str, url: str, body=None):
    req = urllib.request.Request(url, method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except urllib.error.URLError as e:   # not listening yet
        return 0, str(e.reason)


def retry(what: str, fn, attempts: int = 30, delay: float = 2.0):
    for i in range(attempts):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            print(f"{what}: retry {i + 1} ({e})")
            time.sleep(delay)
    raise SystemExit(f"{what}: gave up")


def buckets(key: str, secret: str) -> None:
    fs = pafs.S3FileSystem(access_key=key, secret_key=secret, endpoint_override=S3.split("://")[1],
                           scheme="http", region="us-east-1", allow_bucket_creation=True)
    for b in ("lakehouse", "flink"):
        retry(f"bucket {b}", lambda b=b: fs.create_dir(b))
        print(f"bucket {b}: ok")


def lakekeeper(key: str, secret: str) -> None:
    def info_or_raise():
        code, body = http("GET", f"{LK}/management/v1/info")
        if code != 200:
            raise RuntimeError(f"lakekeeper not ready ({code})")
        return body
    info = retry("lakekeeper", info_or_raise)
    if not (isinstance(info, dict) and info.get("bootstrapped")):
        code, out = http("POST", f"{LK}/management/v1/bootstrap", {"accept-terms-of-use": True})
        assert 200 <= code < 300, out
    _, whs = http("GET", f"{LK}/management/v1/warehouse")
    if any(w["name"] == "lakehouse" for w in whs.get("warehouses", [])):
        print("warehouse lakehouse: exists")
        return
    code, out = http("POST", f"{LK}/management/v1/warehouse", {
        "warehouse-name": "lakehouse",
        "storage-profile": {"type": "s3", "bucket": "lakehouse", "key-prefix": "warehouse", "endpoint": S3,
                            "region": "us-east-1", "path-style-access": True, "flavor": "s3-compat",
                            "sts-enabled": False, "remote-signing-enabled": False},
        "storage-credential": {"type": "s3", "credential-type": "access-key",
                               "access-key-id": key, "secret-access-key": secret},
        "delete-profile": {"type": "hard"},
    })
    assert 200 <= code < 300, out
    print("warehouse lakehouse: created")


def iceberg_tables(key: str, secret: str) -> None:
    cat = load_catalog("lake", type="rest", uri=f"{LK}/catalog", warehouse="lakehouse", **{
        "s3.endpoint": S3, "s3.access-key-id": key, "s3.secret-access-key": secret,
        "s3.region": "us-east-1", "s3.path-style-access": "true"})
    cat.create_namespace_if_not_exists(NAMESPACE)

    def field(i, name, typ, required=False, doc=None):
        return NestedField(i, name, typ, required=required, doc=doc)

    # Raw: every record Kafka delivered, duplicates included, with its Kafka coordinates (bronze).
    raw = IcebergSchema(
        field(1, "event_id", StringType(), True), field(2, "event_time", TimestamptzType(), True),
        field(3, "account_id", StringType(), True), field(4, "region", StringType(), True),
        field(5, "instance_id", StringType(), True), field(6, "state", StringType(), True),
        field(7, "ingest_time", TimestamptzType(), True), field(8, "trace_id", StringType()),
        field(9, "kafka_partition", IntegerType(), True), field(10, "kafka_offset", LongType(), True),
    )
    # Clean: de-duplicated and joined with the CMDB version valid *at event time* (silver).
    clean = IcebergSchema(
        field(1, "event_id", StringType(), True), field(2, "event_time", TimestamptzType(), True),
        field(3, "account_id", StringType(), True), field(4, "region", StringType(), True),
        field(5, "instance_id", StringType(), True), field(6, "state", StringType(), True),
        field(7, "app_name", StringType()), field(8, "environment", StringType()),
        field(9, "owner_email", StringType()), field(10, "support_group", StringType()),
        field(11, "criticality", StringType()), field(12, "ingest_time", TimestamptzType(), True),
    )
    props = {
        "format-version": "2",
        "write.parquet.compression-codec": "zstd",
        # Streaming writes commit every checkpoint -> many small files. Keep metadata bounded:
        "write.metadata.delete-after-commit.enabled": "true",
        "write.metadata.previous-versions-max": "50",
    }
    for name, schema in (("state_changes_raw", raw), ("state_changes", clean)):
        ident = f"{NAMESPACE}.{name}"
        if cat.table_exists(ident):
            print(f"iceberg {ident}: exists")
            continue
        # Hidden partitioning by day(event_time): Flink SQL DDL cannot declare transforms, so tables are
        # created here and Flink only writes. Queries filter on event_time; Iceberg prunes days for you.
        spec = PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=DayTransform(), name="event_day"))
        cat.create_table(ident, schema=schema, partition_spec=spec, properties=props)
        print(f"iceberg {ident}: created")


def kafka_topics() -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    existing = retry("kafka", lambda: admin.list_topics(timeout=10).topics)
    new = [NewTopic(t.name, num_partitions=t.partitions, replication_factor=1, config=t.config)
           for t in topics.ALL if t.name not in existing]
    for name, fut in admin.create_topics(new).items() if new else []:
        fut.result()
        print(f"topic {name}: created")
    print(f"topics: {len(topics.ALL) - len(new)} already existed")


def schemas() -> None:
    sr = SchemaRegistryClient({"url": REGISTRY})
    subject = topics.STATE_CHANGES_SUBJECT
    retry("schema registry", lambda: sr.get_subjects())
    sr.set_compatibility(subject_name=subject, level="BACKWARD")
    schema_id = sr.register_schema(subject, Schema((ROOT / "schemas/ec2_state_change.avsc").read_text(), "AVRO"))
    print(f"schema {subject}: id {schema_id} (BACKWARD compatible)")


def debezium() -> None:
    cfg = json.loads((ROOT / "stack/connect/cmdb-source.json").read_text())
    code, out = http("PUT", f"{CONNECT}/connectors/{cfg['name']}/config", cfg["config"])
    assert code in (200, 201), out
    print(f"connector {cfg['name']}: {'created' if code == 201 else 'updated'}")


def main() -> None:
    s3 = load_secrets("seaweedfs")
    buckets(s3["access_key"], s3["secret_key"])
    lakekeeper(s3["access_key"], s3["secret_key"])
    iceberg_tables(s3["access_key"], s3["secret_key"])
    kafka_topics()
    schemas()
    debezium()
    print("init complete")


if __name__ == "__main__":
    main()

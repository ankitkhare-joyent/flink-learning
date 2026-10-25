"""Read the newest record of one ec2.state-changes partition and show it three ways: raw bytes, the Confluent wire
format header (magic byte + schema id), and the value decoded with the schema the registry returns for that id.

    mise run -q lab:avro              # partition 0
    mise run -q lab:avro -- 3         # partition 3
"""
import struct
import sys
from io import BytesIO

import fastavro
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient

part = int(sys.argv[1]) if len(sys.argv) > 1 else 0
c = Consumer({"bootstrap.servers": "localhost:19092", "group.id": "lab-avro-peek", "enable.auto.commit": False})
lo, hi = c.get_watermark_offsets(TopicPartition("ec2.state-changes", part), timeout=5)
c.assign([TopicPartition("ec2.state-changes", part, max(lo, hi - 1))])
msg = None
for _ in range(20):
    msg = c.poll(1.0)
    if msg is not None and not msg.error():
        break
c.close()
if msg is None:
    raise SystemExit("no record found; is the stack running?")

value = msg.value()
print(f"partition {msg.partition()} offset {msg.offset()}  key={msg.key().decode()!r}  value={len(value)} bytes")
print("first 16 bytes:", value[:16].hex(" "))
magic, schema_id = struct.unpack(">bI", value[:5])
print(f"magic byte={magic}  schema id={schema_id}  (then {len(value) - 5} bytes of binary Avro, no field names)")
schema = SchemaRegistryClient({"url": "http://localhost:18085"}).get_schema(schema_id)
record = fastavro.schemaless_reader(BytesIO(value[5:]), fastavro.parse_schema(__import__("json").loads(schema.schema_str)))
for k, v in record.items():
    print(f"  {k:<12} {v}")

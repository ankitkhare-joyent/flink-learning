# Lab 03 · Kafka and the Schema Registry: partitions, offsets, lag and contracts

One sitting, about 35 minutes. Background: [docs/02](../docs/02-kafka.md), [docs/03](../docs/03-events-and-contracts.md).
Code: [services/common/topics.py](../services/common/topics.py) (topic contracts),
[schemas/ec2_state_change.avsc](../schemas/ec2_state_change.avsc), [scripts/init_streaming.py](../scripts/init_streaming.py)
(creates topics, registers the schema).

Kafka is the shared log that everything downstream reads. After this lab you can explain:

- what a partition and a key guarantee, and how to check it
- what consumer lag is, and why Flink's lag is never zero
- what an Avro record looks like on the wire, and why it has no field names
- what the registry refuses, and what happens when a schema disappears

Kafka UI at http://localhost:18080 shows all of this graphically. Use it alongside the commands.

## Before you start

```bash
source lab/env.sh
offsets ec2.state-changes
```

## 1. Topics are configured on purpose

```bash
kafka kafka-topics.sh --describe --topic ec2.state-changes | head -2
kafka kafka-topics.sh --describe --topic cmdb.public.ec2_ci | head -1 | grep -oE "Configs: .*"
```

**Predict (P1):** `ec2.state-changes` keeps records for how long? Why is the CMDB topic configured differently?

## 2. Keys decide partitions

```bash
offsets ec2.state-changes
mise run -q lab:avro              # the newest record in partition 0; note its key
K=<paste the key>
kafka kafka-console-consumer.sh --topic ec2.state-changes --from-beginning --max-messages 6000 --timeout-ms 10000 \
  --property print.key=true --property print.partition=true --property print.value=false | grep "$K" | sort | uniq -c
```

**Predict (P2):** are the six partitions about the same size? In how many different partitions do you find that
instance's events?

## 3. The wire format

Look at the `lab:avro` output again.

**Predict (P3):** what are the first five bytes, and where are the field names?

## 4. Lag

```bash
lag flink-ec2-serving
```

**Predict (P4):** Flink processes events within milliseconds. Why is the lag a few dozen per partition, never zero?

## 5. The registry is the contract

```bash
curl -s $SR/subjects; echo
curl -s $SR/subjects/ec2.state-changes-value/versions; echo
curl -s $SR/config/ec2.state-changes-value; echo
```

Test two schema changes without registering them: a new required field, then a new optional one.

```bash
S=$(jq -c '.fields += [{"name":"instance_type","type":"string"}]' schemas/ec2_state_change.avsc | jq -Rs '{schema: .}')
S2=$(jq -c '.fields += [{"name":"instance_type","type":["null","string"],"default":null}]' schemas/ec2_state_change.avsc | jq -Rs '{schema: .}')
H='Content-Type: application/vnd.schemaregistry.v1+json'
curl -s -X POST -H "$H" $SR/compatibility/subjects/ec2.state-changes-value/versions/latest -d "$S"; echo
curl -s -X POST -H "$H" $SR/compatibility/subjects/ec2.state-changes-value/versions/latest -d "$S2"; echo
curl -s -o /dev/null -w "register the first one anyway: HTTP %{http_code}\n" -X POST -H "$H" $SR/subjects/ec2.state-changes-value/versions -d "$S"
```

**Predict (P5):** which change is compatible under BACKWARD, and what does the registry answer when you try to register
the other?

## Break it, then fix it

**Break: someone deletes the subject.** (A soft delete: the schema itself stays, the subject goes.)

**Predict (P6):** the running bridges, the Flink jobs, and a bridge that restarts. Which of them notice?

```bash
curl -s -X DELETE $SR/subjects/ec2.state-changes-value; echo
sleep 5 && mise run -q lab:sqs -- depth | head -1
docker compose restart sqs-bridge && sleep 25
mise run -q lab:sqs -- depth | head -1
docker compose logs --no-log-prefix --since 25s sqs-bridge | grep '^{' | jq -r .event | sort | uniq -c
fjobs | cut -f1-3
curl -s $SR/schemas/ids/1 | jq -c '{schema: (.schema | fromjson | .name)}'
```

**Fix:** registering schemas is CI's job. Run it again:

```bash
docker compose up -d --force-recreate --no-deps init && docker wait ec2-streaming-init-1
docker compose logs --no-log-prefix init | grep schema
sleep 35 && mise run -q lab:sqs -- depth | head -1
curl -s $SR/subjects/ec2.state-changes-value/versions; echo
```

## Check your predictions

**P1.** `retention.ms=604800000`: 7 days, then old segments are deleted. The CMDB topic is
`cleanup.policy=compact, min.compaction.lag.ms=60000`: Kafka keeps at least the latest record per key (per
instance) indefinitely. A new consumer can rebuild the whole CMDB table from the topic alone, which is what each
Flink job does at startup.

**P2.** Roughly even: 969, 993, 903, 933, 847 and 975 records when I ran it. The keys are about 2,000 instance ids,
hashed. All six events for `i-6d7c0f49c9f3f68d3` were in partition 0 (`6 Partition:0`). One key, one partition,
in order. That's what makes per-instance pattern matching (Lab 06) possible.

**P3.** `00 00 00 00 01`: the magic byte `0`, then the schema id `1` as a 4-byte integer. Then 146 bytes of binary Avro
for a 151-byte message. There are no field names in the bytes: a reader needs the schema to decode them, which is why
every consumer asks the registry for schema 1. The equivalent JSON is about three times larger.

**P4.** Flink commits its offsets to the group only when a checkpoint completes (every 30 s), for monitoring. The
lag shown is what arrived since the last checkpoint: about 20-28 per partition, 139 in total at 10 events a second.
Flink's real position is in its checkpoint, not in Kafka. Alert on lag that keeps *growing*, not on lag that isn't zero.

**P5.** The required field is incompatible: `{"is_compatible":false,"messages":["instance_type"]}`. New readers
couldn't fill it from old records. The optional one with a default is `{"is_compatible":true}`. Registering the
incompatible one fails with `HTTP 409`. That rejection is the contract in action: no producer can ship a
breaking schema, however it was written.

**P6.** The running bridges kept going (depth 0): the serializer had already looked up schema id 1 and cached it. The
Flink jobs kept going: they decode by the **id** in each message, and `GET /schemas/ids/1` still works after a soft
delete. The restarted bridges failed every cycle with `SchemaRegistryError: Subject 'ec2.state-changes-value' not found.
(HTTP status code 404)`, because producers here run with `auto.register.schemas=False` and must find their schema
registered. Nothing was lost: messages stayed in SQS (`visible=186 in_flight=91`). After `init` re-registered it
(`id 1`, the same id because the schema text is identical, now as version `[2]`), the backlog drained. A missing
schema stops new producers, not data already in flight. Restrict registry deletes in production.

## What to remember

- Partitions give order per key and parallelism across keys. Choose the key for the ordering you need.
- Avro on Kafka is 5 header bytes plus binary data. The registry is what makes it readable.
- Flink's committed offsets lag by up to a checkpoint. Watch the trend.
- The registry rejects breaking changes before they reach anyone. That's the whole point of it.

Next: [Lab 04 · CDC with Debezium](04-cdc-with-debezium.md).

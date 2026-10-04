# 05. Change data capture with Debezium

Your batch stack pulls ServiceNow data every 2 hours. CDC flips that around: the database *pushes* every insert,
update and delete as it is committed, by reading its transaction log. No polling, no "updated_at > last_run"
queries, and deletes are captured too (polling never sees a deleted row).

Here a Postgres table `cmdb.ec2_ci` plays the ServiceNow CMDB (app, owner, environment, criticality per instance).
The simulator inserts a row when it launches an instance and changes an owner every 20 seconds.

## How it works

```
app writes --> Postgres WAL --(logical decoding, pgoutput)--> replication slot dbz_cmdb --> Debezium (Kafka Connect) --> topic cmdb.public.ec2_ci
```

- **WAL** (write-ahead log): Postgres writes every change here before applying it. `wal_level=logical` (set in
  [`compose.yaml`](../compose.yaml)) makes the WAL decodable into row changes.
- **Publication** `dbz_cmdb`: which tables to stream (created in [`stack/postgres/init.sql`](../stack/postgres/init.sql)).
- **Replication slot** `dbz_cmdb`: Postgres remembers how far Debezium has read and **keeps WAL until it does**.
- **Snapshot**: on first start, Debezium reads the whole table (`snapshot.mode=initial`), then switches to streaming.
- **Kafka Connect** is the runtime: a cluster of workers that run connectors, store their offsets in Kafka topics and
  restart them on failure. Debezium is a set of Connect *source connectors*.

## What a change looks like

```json
{
  "before": {"instance_id": "i-0abc...", "owner_email": "asha@example.com", "...": "..."},
  "after":  {"instance_id": "i-0abc...", "owner_email": "ben@example.com",  "...": "..."},
  "source": {"connector": "postgresql", "db": "cmdb", "table": "ec2_ci", "lsn": 24023128, "ts_ms": 1791098400000},
  "op": "u",
  "ts_ms": 1791098400123
}
```

`op` is `c` (create), `u` (update), `d` (delete) or `r` (read, from the snapshot). Flink's `debezium-json` format
turns these into a changelog: insert, update-before + update-after, delete. That is exactly what a table is, as a
stream. Lesson 07 joins against it.

## Production gotchas (each one has bitten real teams)

1. **Replication slots retain WAL.** If Debezium is down for a day, Postgres keeps a day of WAL and the disk can fill.
   Monitor slot lag (`pg_replication_slots`) and set `max_slot_wal_keep_size`.
2. **`REPLICA IDENTITY FULL`** (set in `init.sql`): without it, updates carry no `before` image and Flink cannot
   retract the old row version.
3. **Least privilege.** Debezium's user has `REPLICATION` and `SELECT` on the table only. The publication is created
   up front so it doesn't need superuser (`publication.autocreate.mode=disabled`).
4. **Heartbeats** (`heartbeat.interval.ms`): on a quiet table the slot still advances, so WAL from *other* tables
   can be released.
5. **Schema changes** in the source table flow through as new fields; consumers must tolerate them (lesson 03).
6. **Secrets**: the connector config says `"database.password": "${file:/run/secrets/debezium.properties:password}"`.
   Kafka Connect resolves that at runtime from a file the OpenBao Agent renders (lesson 09), so the password never
   appears in the connector config REST API or in git.

## Debezium vs Flink CDC

Flink CDC (an Apache Flink sub-project) runs Debezium's engine *inside* Flink and reads the database directly, with no
Kafka in between. Fewer moving parts, but only Flink sees the changes. Debezium-to-Kafka publishes the changelog
once for every consumer (Flink, the lakehouse, other teams). With several consumers, use Kafka.

## Try it

Connector status:

```bash
curl -s localhost:18083/connectors/cmdb-source/status
```

Change an owner yourself and watch the change reach the serving table a few seconds later. (These go through the
admin user for convenience; in a real system you'd never write to a source system like this.)

```bash
docker compose exec postgres psql -U postgres -d cmdb -c "UPDATE ec2_ci SET owner_email='you@example.com', updated_at=now() WHERE instance_id=(SELECT instance_id FROM ec2_ci LIMIT 1) RETURNING instance_id"
```

```bash
docker compose exec postgres psql -U postgres -d serving -c "SELECT instance_id, owner_email, updated_at FROM instance_current_state WHERE owner_email='you@example.com'"
```

(It shows up once that instance has had at least one state change since the stream started.)

Replication slot lag:

```bash
docker compose exec postgres psql -U postgres -d cmdb -c "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag FROM pg_replication_slots"
```

Next: [06. Flink fundamentals](06-flink-fundamentals.md)

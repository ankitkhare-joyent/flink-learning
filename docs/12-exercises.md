# 12. Exercises

Each exercise breaks or changes something on purpose. Predict what will happen, then check. That habit is most of
what makes a good streaming engineer.

## 1. Follow one event end to end

Pick an instance id from Kafka UI (topic `ec2.state-changes`, Messages). Then find it:

- in the serving table: `docker compose exec postgres psql -U postgres -d serving -c "SELECT * FROM instance_current_state WHERE instance_id='<id>'"`
- in Iceberg (lesson 08's DuckDB snippet), with its owner as of each event
- through MCP: ask Claude Code `get_instance_state` and `instance_timeline` for it (lesson 10)

Note the timestamps: event time, Kafka write (`ingest_time`), serving `updated_at`. Where does the latency go?

## 2. Kill the TaskManager mid-stream

```bash
docker compose kill taskmanager
```

Watch the Flink UI: jobs go to RESTARTING. Lag grows in Kafka UI. Then:

```bash
docker compose up -d taskmanager
```

Jobs restore from the last checkpoint and catch up. Check that no alert was published twice (alert ids are
deterministic and the Kafka sink is transactional) and that the serving table is correct. Why did nothing break?

## 3. Stop the bridge and watch SQS buffer

```bash
docker compose stop sqs-bridge
```

Look at queue depth (http://localhost:19325). Wait two minutes, then start it again. The backlog drains in seconds.
Events that waited in SQS keep their original `event_time`; which Flink outputs treat them as late, and which don't?

## 4. Late data on purpose

Raise the late rate and restart the simulator:

```bash
SIM_LATE_RATE=0.2 docker compose up -d simulator
```

Compare `count(*)` per minute in `state_change_counts_1m` with the lakehouse for the same minute. The window
counts are lower: late events were dropped by the windows but kept in Iceberg. How would you repair the serving
counts? (Hint: allowed lateness via a larger watermark bound, or a periodic batch correction from Iceberg.)

## 5. Evolve the schema

Add an optional field to [`schemas/ec2_state_change.avsc`](../schemas/ec2_state_change.avsc):

```json
{"name": "instance_type", "type": ["null", "string"], "default": null}
```

Re-run `docker compose up -d --force-recreate init`. The registry accepts it (BACKWARD compatible). Now try a field
*without* a default and see the 409. Then update the bridge to fill the new field and the Flink source DDL to read
it. In what order must you deploy them, and why?

## 6. Rotate a secret

Follow "Try it" in lesson 09 to rotate Debezium's password. Before restarting the connector, does CDC still work?
(Yes: the open replication connection keeps its session.) What happens when Connect restarts it later, and why does
the bao-agent's 5-minute re-render matter?

## 7. Write a new alert

Add a third pattern to [`jobs/30_alerts.sql`](../jobs/30_alerts.sql): **STUCK_PENDING**, an instance that entered `pending` and has no
`running` within 2 minutes. `MATCH_RECOGNIZE` can't express "nothing happened" directly. Options to research:
a `WITHIN` pattern with a timeout (Flink SQL doesn't emit timed-out partial matches), a DataStream
`KeyedProcessFunction` with an event-time timer, or a query comparing `instance_current_state` with
`now()` from outside. Which would you ship, and why?

Test it with a bounded input before deploying: run the job's SQL in batch mode (`SET 'execution.runtime-mode' = 'batch'`)
over `lake.ec2_stream.state_changes_raw`.

## 8. Make the agent cheaper

The triage agent uses Claude Opus 5.5 at effort `medium`. Change `AGENT_EFFORT` to `low` and compare triage
quality on a few alerts (`alert_triage` table) against tokens used (`agent_tokens_total` on the agent's metrics port).
What is the cheapest setting where the triage is still right? That's how you'd build an eval for it.

## 9. Replay from scratch

Cancel `ec2-serving` in the Flink UI, `TRUNCATE instance_current_state` in Postgres, and re-submit:

```bash
docker compose run --rm submit-jobs
```

It starts from the earliest offsets (no checkpoint to restore) and rebuilds the table from 7 days of Kafka history.
How long did it take, and what does that tell you about retention as a recovery tool?

## 10. Map it to your job

Sketch how your real ingestion (aws cli, DCIM, ansible facts, ServiceNow) would look as streams. Which sources emit
events natively (EventBridge, CloudTrail, ServiceNow outbound REST/webhooks), which need CDC, and which stay batch
(ansible facts might be fine every 2 hours)? Not everything should be a stream.

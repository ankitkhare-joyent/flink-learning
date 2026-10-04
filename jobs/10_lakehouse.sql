-- Job 1: stream every event into the lakehouse (Iceberg), raw and cleaned.
--   state_changes_raw : exactly what Kafka delivered, duplicates included, with partition/offset (audit, replay)
--   state_changes     : de-duplicated + enriched with the CMDB row that was valid *at event time*
-- Iceberg commits once per Flink checkpoint (30 s), so data is queryable ~30 s after it happens, and
-- each commit is atomic: readers never see half a checkpoint.
SET 'pipeline.name' = 'ec2-lakehouse';

EXECUTE STATEMENT SET
BEGIN

INSERT INTO lake.ec2_stream.state_changes_raw
SELECT event_id, event_ts, account_id, region, instance_id, state, ingest_ts, trace_id,
       kafka_partition, kafka_offset
FROM ec2_state_changes;

-- Event-time temporal join: for each event, the CMDB version whose updated_at <= event_time.
-- An owner change at 10:05 does not rewrite who owned the instance when it stopped at 10:00.
INSERT INTO lake.ec2_stream.state_changes
SELECT e.event_id, e.event_time, e.account_id, e.region, e.instance_id, e.state,
       c.app_name, c.environment, c.owner_email, c.support_group, c.criticality, e.ingest_time
FROM ec2_state_changes_dedup AS e
LEFT JOIN cmdb_ci FOR SYSTEM_TIME AS OF e.event_time AS c
  ON e.instance_id = c.instance_id;

END;

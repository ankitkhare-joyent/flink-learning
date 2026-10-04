-- Job 3: detect situations worth a human's (or an agent's) attention and publish them to ec2.alerts.
--   FLAPPING              : stopped -> running -> stopped -> running -> stopped within 10 minutes
--   CRITICAL_TERMINATION  : a tier1 prod instance was terminated
-- The Kafka sink is exactly-once: records are written in a Kafka transaction committed on each checkpoint,
-- so a crash and restart never publishes the same alert twice (consumers use isolation.level=read_committed).
SET 'pipeline.name' = 'ec2-alerts';

CREATE TEMPORARY TABLE ec2_alerts (
  alert_id     STRING,
  alert_type   STRING,
  severity     STRING,
  instance_id  STRING,
  account_id   STRING,
  region       STRING,
  detected_at  TIMESTAMP_LTZ(3),
  window_start TIMESTAMP_LTZ(3),
  window_end   TIMESTAMP_LTZ(3),
  summary      STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'ec2.alerts',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.transaction.timeout.ms' = '900000',   -- must exceed the checkpoint interval + recovery time
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'ec2-alerts',
  'key.format' = 'raw',
  'key.fields' = 'alert_id',
  'value.format' = 'json',
  'value.json.timestamp-format.standard' = 'ISO-8601',
  'value.fields-include' = 'ALL'
);

-- MATCH_RECOGNIZE is complex event processing in SQL: a regular expression over rows, per instance,
-- in event-time order. Only the two states that matter feed the pattern.
CREATE TEMPORARY VIEW stop_start AS
SELECT * FROM ec2_state_changes_dedup WHERE state IN ('running', 'stopped');

-- One INSERT with UNION ALL, not two: each INSERT would create its own exactly-once Kafka sink, and two sinks
-- may not share a transactional-id prefix.
INSERT INTO ec2_alerts
SELECT MD5(CONCAT(instance_id, '|FLAPPING|', CAST(first_stop AS STRING))) AS alert_id,
       -- CAST turns rowtime attributes into plain timestamps: a sink may receive at most one time attribute.
       'FLAPPING', 'warning', instance_id, account_id, region,
       CAST(last_stop AS TIMESTAMP_LTZ(3)), CAST(first_stop AS TIMESTAMP_LTZ(3)), CAST(last_stop AS TIMESTAMP_LTZ(3)),
       CONCAT('Instance stopped and restarted 3 times between ', CAST(first_stop AS STRING), ' and ',
              CAST(last_stop AS STRING), ' UTC')
FROM stop_start
MATCH_RECOGNIZE (
  PARTITION BY instance_id
  ORDER BY event_time
  MEASURES
    S1.event_time AS first_stop,
    S3.event_time AS last_stop,
    S3.account_id AS account_id,
    S3.region     AS region
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (S1 R1 S2 R2 S3) WITHIN INTERVAL '10' MINUTE
  DEFINE
    S1 AS S1.state = 'stopped', R1 AS R1.state = 'running',
    S2 AS S2.state = 'stopped', R2 AS R2.state = 'running',
    S3 AS S3.state = 'stopped'
)

UNION ALL

SELECT MD5(CONCAT(e.event_id, '|CRITICAL_TERMINATION')), 'CRITICAL_TERMINATION', 'critical',
       e.instance_id, e.account_id, e.region, CAST(e.event_time AS TIMESTAMP_LTZ(3)),
       CAST(e.event_time AS TIMESTAMP_LTZ(3)), CAST(e.event_time AS TIMESTAMP_LTZ(3)),
       CONCAT('tier1 prod instance of ', c.app_name, ' (owner ', c.owner_email, ') was terminated')
FROM ec2_state_changes_dedup AS e
JOIN cmdb_ci FOR SYSTEM_TIME AS OF e.event_time AS c
  ON e.instance_id = c.instance_id
WHERE e.state = 'terminated' AND c.criticality = 'tier1' AND c.environment = 'prod';

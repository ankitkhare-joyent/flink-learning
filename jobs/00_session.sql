-- Shared session setup, run before every job file (submit.sh prepends it to each job file).
-- Dollar-brace placeholders are filled from OpenBao-rendered secrets by submit.sh; nothing secret lives in git.

SET 'table.local-time-zone' = 'UTC';
SET 'execution.runtime-mode' = 'streaming';
-- A partition with no traffic must not hold back the watermark of the whole job (see docs/06).
SET 'table.exec.source.idle-timeout' = '10 s';
-- Upper bound on keyed state (dedup ids, join rows). Without it, state grows forever.
SET 'table.exec.state.ttl' = '36 h';
-- Mini-batching (buffer ~500 ms of input before touching state) speeds up aggregations a lot, but it changes
-- the "keep first row" deduplication below from insert-only to an update stream (EXPLAIN CHANGELOG_MODE shows
-- outputInsertOnly=[false]), and then windows, OVER aggregates and MATCH_RECOGNIZE can no longer consume it.
-- So it stays off here. Lesson 07 walks through finding this with EXPLAIN.
SET 'table.exec.mini-batch.enabled' = 'false';

-- ------------------------------------------------------------------ catalogs
-- Iceberg tables live in Lakekeeper (Iceberg REST catalog). In AWS this would be the Glue Iceberg REST
-- endpoint or S3 Tables; only these options change, not the SQL.
CREATE CATALOG lake WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://lakekeeper:8181/catalog',
  'warehouse' = 'lakehouse',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://seaweedfs:8333',
  's3.path-style-access' = 'true',
  's3.access-key-id' = '${S3_ACCESS_KEY}',
  's3.secret-access-key' = '${S3_SECRET_KEY}',
  'client.region' = 'us-east-1'
);

-- ------------------------------------------------------------------ sources
-- EC2 state changes (Avro, schema in the registry). Event time = when AWS says it happened.
-- The watermark says "I believe I have seen everything up to event_time - 30s". Events older than the
-- watermark when they arrive are *late*: windows and pattern matches that already closed ignore them.
CREATE TEMPORARY TABLE ec2_state_changes (
  instance_id      STRING NOT NULL,
  event_id         STRING NOT NULL,
  event_time       TIMESTAMP(3) NOT NULL,       -- Avro timestamp-millis arrives as a zone-less TIMESTAMP (UTC)
  account_id       STRING NOT NULL,
  region           STRING NOT NULL,
  state            STRING NOT NULL,
  ingest_time      TIMESTAMP(3) NOT NULL,
  source           STRING NOT NULL,
  trace_id         STRING,
  kafka_partition  INT    METADATA FROM 'partition' VIRTUAL,
  kafka_offset     BIGINT METADATA FROM 'offset' VIRTUAL,
  -- Turn the UTC wall-clock values into instants (TIMESTAMP_LTZ); the session time zone is UTC, so this is exact.
  event_ts  AS CAST(event_time AS TIMESTAMP_LTZ(3)),
  ingest_ts AS CAST(ingest_time AS TIMESTAMP_LTZ(3)),
  proc_time AS PROCTIME(),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'ec2.state-changes',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-${PIPELINE_NAME}',   -- offsets are committed here on each checkpoint, for lag monitoring only
  'properties.isolation.level' = 'read_committed',
  'scan.startup.mode' = 'earliest-offset',          -- first start only; restarts resume from checkpoint state
  'key.format' = 'raw',
  'key.fields' = 'instance_id',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = 'http://schema-registry:8081',
  'value.fields-include' = 'ALL'
);

-- The CMDB as a *changelog*: Debezium emits create/update/delete, Flink turns it into a versioned table.
-- PRIMARY KEY + WATERMARK make it usable in event-time temporal joins ("the CMDB row as of event time").
CREATE TEMPORARY TABLE cmdb_ci (
  instance_id    STRING NOT NULL,
  account_id     STRING,
  region         STRING,
  app_name       STRING,
  environment    STRING,
  owner_email    STRING,
  support_group  STRING,
  criticality    STRING,
  updated_at     TIMESTAMP_LTZ(3) NOT NULL,
  PRIMARY KEY (instance_id) NOT ENFORCED,
  WATERMARK FOR updated_at AS updated_at - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'cmdb.public.ec2_ci',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-${PIPELINE_NAME}-cmdb',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'debezium-json',
  'value.debezium-json.timestamp-format.standard' = 'ISO-8601'
);

-- ------------------------------------------------------------------ shared views
-- SQS delivers at-least-once, so the same event_id can arrive twice. Keep the first copy we see.
-- ORDER BY proc_time ASC is the "keep first row" pattern: output stays append-only (no retractions).
CREATE TEMPORARY VIEW ec2_state_changes_dedup AS
SELECT instance_id, event_id, event_ts AS event_time, account_id, region, state, ingest_ts AS ingest_time
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY proc_time ASC) AS rn
  FROM ec2_state_changes
)
WHERE rn = 1;

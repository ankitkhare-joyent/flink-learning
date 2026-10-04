-- Job 2: keep low-latency serving tables in Postgres up to date (what apps, APIs and agents query).
--   instance_current_state : one row per instance, upserted on every change (a materialized view)
--   state_change_counts_1m : event counts per region/state in 1-minute tumbling windows
SET 'pipeline.name' = 'ec2-serving';

CREATE TEMPORARY TABLE pg_instance_current_state (
  instance_id STRING, account_id STRING, region STRING, state STRING, previous_state STRING,
  state_since TIMESTAMP(3), app_name STRING, environment STRING, owner_email STRING, criticality STRING,
  transitions_1h BIGINT, updated_at TIMESTAMP(3),
  PRIMARY KEY (instance_id) NOT ENFORCED          -- PK => the JDBC sink upserts (INSERT .. ON CONFLICT)
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/serving',
  'table-name' = 'instance_current_state',
  'username' = '${SERVING_DB_USER}',
  'password' = '${SERVING_DB_PASSWORD}',
  'sink.buffer-flush.interval' = '1 s',
  'sink.buffer-flush.max-rows' = '500',
  'sink.max-retries' = '5'
);

CREATE TEMPORARY TABLE pg_state_change_counts_1m (
  window_start TIMESTAMP(3), window_end TIMESTAMP(3), region STRING, state STRING,
  events BIGINT, instances BIGINT,
  PRIMARY KEY (window_start, region, state) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/serving',
  'table-name' = 'state_change_counts_1m',
  'username' = '${SERVING_DB_USER}',
  'password' = '${SERVING_DB_PASSWORD}',
  'sink.buffer-flush.interval' = '1 s'
);

-- OVER windows: for every event, look back along that instance's own history (ordered by event time).
-- All OVER aggregates in one SELECT must share one window, and LAG takes no frame, so the previous state
-- and the one-hour count are computed separately and put back together with an interval join.
CREATE TEMPORARY VIEW with_previous AS
SELECT instance_id, event_id, account_id, region, state, event_time,
       LAG(state) OVER (PARTITION BY instance_id ORDER BY event_time) AS previous_state
FROM ec2_state_changes_dedup;

CREATE TEMPORARY VIEW with_hourly_count AS
SELECT event_id, event_time,
       COUNT(*) OVER (PARTITION BY instance_id ORDER BY event_time
                      RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW) AS transitions_1h
FROM ec2_state_changes_dedup;

-- Interval join: rows match only within a time bound (here: the same event time), so Flink can drop join
-- state as soon as the watermark passes, instead of keeping it until the TTL.
CREATE TEMPORARY VIEW transitions AS
SELECT p.instance_id, p.account_id, p.region, p.state, p.event_time, p.previous_state, c.transitions_1h
FROM with_previous AS p
JOIN with_hourly_count AS c
  ON p.event_id = c.event_id AND p.event_time BETWEEN c.event_time AND c.event_time;

-- Latest event per instance ("keep last row" on event time). Its output is an *update* stream.
CREATE TEMPORARY VIEW latest AS
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY instance_id ORDER BY event_time DESC) AS rn FROM transitions
) WHERE rn = 1;

EXECUTE STATEMENT SET
BEGIN

-- A *regular* join (not temporal): when the CMDB owner changes, the serving row updates too, because
-- this table answers "who owns it now?". Compare with the as-of-event-time join in 10_lakehouse.sql.
INSERT INTO pg_instance_current_state
SELECT l.instance_id, l.account_id, l.region, l.state, l.previous_state,
       CAST(l.event_time AS TIMESTAMP(3)), c.app_name, c.environment, c.owner_email, c.criticality,
       l.transitions_1h, CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))
FROM latest AS l
LEFT JOIN cmdb_ci AS c ON l.instance_id = c.instance_id;

-- Window TVF: a result per (minute, region, state) is emitted once, when the watermark passes the
-- window end. Late events for a closed window are dropped (they still reach the lakehouse via job 1).
INSERT INTO pg_state_change_counts_1m
SELECT CAST(window_start AS TIMESTAMP(3)), CAST(window_end AS TIMESTAMP(3)), region, state,
       COUNT(*), COUNT(DISTINCT instance_id)
FROM TABLE(TUMBLE(TABLE ec2_state_changes_dedup, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, region, state;

END;

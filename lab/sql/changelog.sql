-- Lab 06: what kind of stream does each query produce? Look for changelogMode=[...] on the top operator.
SET 'sql-client.execution.result-mode' = 'tableau';

-- (a) a plain filter over Kafka
EXPLAIN CHANGELOG_MODE SELECT instance_id, state FROM ec2_state_changes WHERE state = 'stopped';

-- (b) keep-first deduplication (the view every job uses)
EXPLAIN CHANGELOG_MODE SELECT instance_id, event_id FROM ec2_state_changes_dedup;

-- (c) the latest state per instance
EXPLAIN CHANGELOG_MODE
SELECT instance_id, state FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY instance_id ORDER BY event_time DESC) AS rn FROM ec2_state_changes_dedup
) WHERE rn = 1;

-- (d) a running count per state, no window
EXPLAIN CHANGELOG_MODE SELECT state, COUNT(*) FROM ec2_state_changes_dedup GROUP BY state;

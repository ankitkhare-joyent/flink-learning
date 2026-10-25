-- Lab 06, break: turn mini-batch on and look at the dedup view again, then plan the flapping pattern on top of it.
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '500 ms';
SET 'table.exec.mini-batch.size' = '1000';

EXPLAIN CHANGELOG_MODE SELECT instance_id, event_id FROM ec2_state_changes_dedup;

EXPLAIN
SELECT * FROM ec2_state_changes_dedup
MATCH_RECOGNIZE (
  PARTITION BY instance_id ORDER BY event_time
  MEASURES S1.event_time AS first_stop
  ONE ROW PER MATCH AFTER MATCH SKIP PAST LAST ROW
  PATTERN (S1 R1 S2) WITHIN INTERVAL '10' MINUTE
  DEFINE S1 AS S1.state = 'stopped', R1 AS R1.state = 'running', S2 AS S2.state = 'stopped'
);

-- Lab 06: how many duplicates did SQS and the bridge deliver? A bounded (batch) read of the topic up to "now".
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'batch';

SELECT COUNT(*) AS kafka_records,
       COUNT(DISTINCT event_id) AS distinct_events,
       COUNT(*) - COUNT(DISTINCT event_id) AS duplicates,
       SUM(CASE WHEN TIMESTAMPDIFF(SECOND, event_time, ingest_time) > 30 THEN 1 ELSE 0 END) AS arrived_over_30s_late
FROM ec2_state_changes /*+ OPTIONS('scan.bounded.mode' = 'latest-offset') */;

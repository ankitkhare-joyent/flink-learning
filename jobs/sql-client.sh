#!/usr/bin/env bash
# Interactive Flink SQL client with the session setup (catalogs, sources, views) already loaded:
#   docker compose run --rm --no-deps --entrypoint bash submit-jobs /opt/flink/jobs/sql-client.sh
set -euo pipefail
printf '%s\nrest.address: jobmanager\n' "${FLINK_PROPERTIES:-}" >> /opt/flink/conf/config.yaml
set -a; . /run/secrets/flink-jobs.env; set +a
export PIPELINE_NAME="sql-client-$$"   # its own consumer group, so it never disturbs the jobs
session=$(mktemp --suffix .sql)
cp /opt/flink/jobs/00_session.sql "$session"
for var in SERVING_DB_USER SERVING_DB_PASSWORD S3_ACCESS_KEY S3_SECRET_KEY PIPELINE_NAME; do
  sed -i "s|\${$var}|${!var}|g" "$session"
done
exec /opt/flink/bin/sql-client.sh -i "$session"

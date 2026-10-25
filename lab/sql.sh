#!/usr/bin/env bash
# Run a Flink SQL script from lab/sql/ with the session (catalogs, sources, views) preloaded.
#   mise run lab:sql -- changelog.sql
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose run --rm --no-deps -v "$PWD/lab/sql:/lab/sql:ro" --entrypoint bash submit-jobs \
  /opt/flink/jobs/sql-client.sh -f "/lab/sql/${1:?usage: lab/sql.sh FILE.sql}" 2>&1 \
  | grep -vE '^WARNING|jline|dumb terminal|history file|Container ec2-streaming'

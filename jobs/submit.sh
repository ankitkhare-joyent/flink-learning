#!/usr/bin/env bash
# Submits each Flink SQL job (jobs/[1-9]*.sql) to the session cluster, skipping jobs already RUNNING.
# Secrets are rendered by the OpenBao Agent sidecar into /run/secrets/flink-jobs.env and substituted
# into a temporary copy of the SQL, so they never live in git or in the image.
#
# Production equivalent: one FlinkDeployment (Flink Kubernetes Operator) per job in application mode,
# managed by GitOps, with upgrades done from a savepoint (upgradeMode: savepoint).
set -euo pipefail

JM="http://jobmanager:8081"
SECRETS=/run/secrets/flink-jobs.env
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# The sql-client is a client of the cluster: give it the same config the cluster has.
printf '%s\nrest.address: jobmanager\n' "${FLINK_PROPERTIES:-}" >> /opt/flink/conf/config.yaml

for _ in $(seq 90); do
  if [ -s "$SECRETS" ] && curl -fs "$JM/overview" | grep -q '"taskmanagers":[1-9]'; then break; fi
  echo "waiting for jobmanager, a taskmanager and rendered secrets..."; sleep 2
done
set -a; . "$SECRETS"; set +a

render() {  # replace ${VAR} placeholders with values; fail loudly if one is missing
  local out="$WORK/$(basename "$1")"; cp "$1" "$out"
  for var in SERVING_DB_USER SERVING_DB_PASSWORD S3_ACCESS_KEY S3_SECRET_KEY PIPELINE_NAME; do
    sed -i "s|\${$var}|${!var}|g" "$out"
  done
  if grep -q '\${' "$out"; then echo "unrendered placeholder in $1" >&2; exit 1; fi
  echo "$out"
}

running=$(curl -fs "$JM/jobs/overview" | sed 's/},{/}\n{/g' | grep '"state":"RUNNING"' || true)

for job in /opt/flink/jobs/[1-9]*.sql; do
  name=$(sed -n "s/^SET 'pipeline.name' = '\(.*\)';/\1/p" "$job")
  export PIPELINE_NAME="$name"
  if echo "$running" | grep -q "\"name\":\"$name\""; then
    echo "job $name: already running, skipped"
    continue
  fi
  echo "job $name: submitting $(basename "$job")"
  # Session setup + job in one script: sql-client -f stops at, and prints, the first failing statement.
  script="$WORK/run-$(basename "$job")"
  cat "$(render /opt/flink/jobs/00_session.sql)" "$(render "$job")" > "$script"
  /opt/flink/bin/sql-client.sh -f "$script" 2>&1 | tee "$WORK/out.log"
  # sql-client exits 0 even when a statement fails, so check its output.
  if grep -q "\[ERROR\]" "$WORK/out.log"; then echo "job $name: FAILED (see the error above)" >&2; failed=1; fi
done
if [ "${failed:-0}" = 1 ]; then exit 1; fi
echo "all jobs submitted"

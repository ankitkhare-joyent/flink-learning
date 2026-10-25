#!/usr/bin/env bash
# Waits until the one-shot setup containers succeeded and the three Flink jobs are RUNNING.
set -uo pipefail
cd "$(dirname "$0")/.."
for svc in bao-bootstrap init submit-jobs; do
  until [ "$(docker inspect -f '{{.State.Status}}' "ec2-streaming-$svc-1" 2>/dev/null)" = exited ]; do sleep 3; done
  code=$(docker inspect -f '{{.State.ExitCode}}' "ec2-streaming-$svc-1")
  if [ "$code" != 0 ]; then echo "$svc failed (exit $code): docker compose logs $svc" >&2; exit 1; fi
done
for _ in $(seq 60); do
  states=$(curl -s localhost:18081/jobs/overview | jq -r '[.jobs[] | select(.state != "CANCELED" and .state != "FINISHED") | "\(.name)=\(.state)"] | sort | join(" ")')
  case "$states" in *RESTARTING*|*CREATED*|*INITIALIZING*|"") sleep 5 ;; *) break ;; esac
done
echo "flink: $states"

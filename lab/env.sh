# Source this at the start of every lab sitting (bash or zsh), from the repo root:
#
#   source lab/env.sh
#
# It sets variables and defines shell functions. Nothing prints a secret value unless you ask for one, and the
# functions that return one (role_token, bao_field) are meant for $( ... ) only.

export BAO_ADDR="http://localhost:18200"    # OpenBao (UI at $BAO_ADDR/ui)
export FLINK="http://localhost:18081"       # Flink UI + REST API
export SR="http://localhost:18085"          # Schema Registry (Karapace)
export CONNECT="http://localhost:18083"     # Kafka Connect (Debezium)
export MCP_URL="http://localhost:18002/mcp" # MCP server
# Kafka UI: http://localhost:18080   ElasticMQ stats: http://localhost:19325

# OpenBao keeps its init output in the bao-init volume, not on your disk. vol_cat reads a file from the bootstrap
# volumes with a throwaway container:  vol_cat init/init.json | jq 'keys'
vol_cat() {
  docker run --rm -v ec2-streaming_bao-init:/b/init:ro -v ec2-streaming_bao-approle:/b/approle:ro \
    -v ec2-streaming_bao-rendered:/b/rendered:ro --user 0 --entrypoint cat ec2-streaming/ingest:dev "/b/$1"
}
# The root token. A local-only shortcut so you can look at everything; in production nobody keeps one (docs/09).
export BAO_TOKEN="$(vol_cat init/init.json 2>/dev/null | jq -r '.root_token // empty')"
[ -n "$BAO_TOKEN" ] || echo "lab/env.sh: no root token yet; run 'mise run up' first" >&2

# OpenBao ---------------------------------------------------------------------------------------------------
# T=$(role_token mcp)                -> an AppRole login token for init | simulator | bao-agent | mcp | agent
role_token() {
  curl -s "$BAO_ADDR/v1/auth/approle/login" \
    -d "{\"role_id\":\"$(vol_cat "approle/$1/role_id")\",\"secret_id\":\"$(vol_cat "approle/$1/secret_id")\"}" \
    | jq -r '.auth.client_token // empty'
}
# bao_keys ec2-streaming/mcp          -> a KV secret's field names and version, never the values
bao_keys() {
  curl -s -H "X-Vault-Token: ${TOKEN:-$BAO_TOKEN}" "$BAO_ADDR/v1/secret/data/$1" \
    | jq -c 'if .errors then {errors} else {fields: (.data.data | keys), version: .data.metadata.version} end'
}
# K=$(bao_field ec2-streaming/mcp bearer_token)   -> one value, for use inside $( ... )
bao_field() {
  curl -s -H "X-Vault-Token: ${TOKEN:-$BAO_TOKEN}" "$BAO_ADDR/v1/secret/data/$1" | jq -r --arg f "$2" '.data.data[$f] // empty'
}

# Postgres --------------------------------------------------------------------------------------------------
# psql_su serving "select count(*) from instance_current_state"   -> superuser (break-glass; services never use it)
psql_su() { docker compose exec -T postgres psql -U postgres -d "$1" -P pager=off -c "$2"; }
# pg_as USER PASSWORD DB "SQL"        -> over the container network, so the password is really checked
pg_as() { docker compose exec -T -e PGPASSWORD="$2" postgres psql -h postgres -U "$1" -d "$3" -P pager=off -c "$4" 2>&1; }

# Kafka -----------------------------------------------------------------------------------------------------
# kafka kafka-topics.sh --describe --topic ec2.alerts   -> any Kafka CLI tool, already pointed at the broker
kafka() { local tool="$1"; shift; docker compose exec -T kafka "/opt/kafka/bin/$tool" --bootstrap-server localhost:9092 "$@"; }
# offsets TOPIC                       -> end offset per partition, and the total
offsets() {
  kafka kafka-get-offsets.sh --topic "$1" | awk -F: '{print "  p" $2 ": " $3; s += $3} END {print "  total: " s}'
}
# lag GROUP                           -> per-partition lag for a consumer group, and the total
lag() {
  kafka kafka-consumer-groups.sh --describe --group "$1" 2>/dev/null \
    | awk 'NR>1 && $6 ~ /^[0-9]+$/ {print "  " $2 " p" $3 ": committed=" $4 " end=" $5 " lag=" $6; s += $6} END {print "  total lag: " s}'
}

# Flink -----------------------------------------------------------------------------------------------------
# fjobs                               -> each job's name, state, and last-checkpoint summary
fjobs() {
  for jid in $(curl -s "$FLINK/jobs/overview" | jq -r '.jobs[] | select(.state != "CANCELED" and .state != "FINISHED") | .jid'); do
    local name state
    name=$(curl -s "$FLINK/jobs/$jid" | jq -r .name); state=$(curl -s "$FLINK/jobs/$jid" | jq -r .state)
    curl -s "$FLINK/jobs/$jid/checkpoints" | jq -r --arg n "$name" --arg s "$state" --arg j "$jid" \
      '"\($n)\t\($s)\tcheckpoints ok=\(.counts.completed) failed=\(.counts.failed) restored=\(.counts.restored)\tlast=\(((.latest.completed.state_size // 0) / 1024 | floor))KiB\t\($j)"'
  done
}
# fjid ec2-alerts                     -> a running job's id
fjid() { curl -s "$FLINK/jobs/overview" | jq -r --arg n "$1" '.jobs[] | select(.name == $n and .state != "CANCELED") | .jid' | head -1; }

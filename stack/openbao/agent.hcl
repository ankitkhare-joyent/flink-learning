# OpenBao Agent as a sidecar: logs in with AppRole, keeps its token fresh, and renders secrets into files
# that apps which know nothing about OpenBao (Kafka Connect, Flink) simply read. On Kubernetes the same
# thing is an injected init/sidecar container, or the External Secrets Operator syncing into a Secret.
pid_file = "/tmp/bao-agent.pid"

vault {
  address = "http://openbao:8200"
}

auto_auth {
  method "approle" {
    config = {
      role_id_file_path                   = "/run/approle/role_id"
      secret_id_file_path                 = "/run/approle/secret_id"
      remove_secret_id_file_after_reading = false
    }
  }
  sink "file" {
    config = { path = "/tmp/bao-token" }
  }
}

template_config {
  exit_on_retry_failure = true
  static_secret_render_interval = "5m"   # re-read KV and static-role passwords, so rotations propagate
}

# Debezium reads this through Kafka Connect's FileConfigProvider: ${file:/run/secrets/debezium.properties:password}
template {
  destination = "/rendered/debezium.properties"
  perms       = "0644"
  contents    = <<EOT
{{ with secret "database/static-creds/debezium" }}password={{ .Data.password }}{{ end }}
EOT
}

# Flink: S3 keys for checkpoints, appended to the Flink config at container start.
template {
  destination = "/rendered/flink-s3.yaml"
  perms       = "0644"
  contents    = <<EOT
{{ with secret "secret/data/ec2-streaming/seaweedfs" }}s3.access-key: {{ .Data.data.access_key }}
s3.secret-key: {{ .Data.data.secret_key }}{{ end }}
EOT
}

# Flink SQL jobs: placeholders in jobs/*.sql are filled from this file by jobs/submit.sh.
template {
  destination = "/rendered/flink-jobs.env"
  perms       = "0644"
  contents    = <<EOT
{{ with secret "database/static-creds/flink" }}SERVING_DB_USER={{ .Data.username }}
SERVING_DB_PASSWORD={{ .Data.password }}{{ end }}
{{ with secret "secret/data/ec2-streaming/seaweedfs" }}S3_ACCESS_KEY={{ .Data.data.access_key }}
S3_SECRET_KEY={{ .Data.data.secret_key }}{{ end }}
EOT
}

# OpenBao server: single node, integrated storage (Raft). Production runs 3 or 5 Raft nodes across AZs,
# TLS on every listener, and auto-unseal through AWS KMS:  seal "awskms" { kms_key_id = "..." }
storage "raft" {
  path    = "/openbao/file"
  node_id = "bao-1"
}

listener "tcp" {
  address     = "0.0.0.0:8200"
  tls_disable = true      # local only
}

api_addr      = "http://openbao:8200"
cluster_addr  = "http://openbao:8201"
ui            = true
disable_mlock = true      # containers usually cannot mlock; production hosts should allow it

# Audit log: every request and response, with secret values HMAC'd (never in clear). OpenBao only allows
# audit devices to be declared here in config, not enabled over the API, so a compromised admin token
# cannot quietly turn auditing off.
audit "file" "stdout" {
  options {
    file_path = "/dev/stdout"
  }
}

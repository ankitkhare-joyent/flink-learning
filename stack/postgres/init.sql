-- One Postgres, three logical databases (one per owner), like separate RDS instances in production.
--   lakekeeper : Iceberg catalog metadata
--   cmdb       : the "ServiceNow CMDB" source system that Debezium captures (needs wal_level=logical)
--   serving    : low-latency tables Flink keeps up to date for apps, APIs and agents
CREATE DATABASE lakekeeper;
CREATE DATABASE cmdb;
CREATE DATABASE serving;

-- No passwords here: OpenBao owns them (static roles) and rotates them. See scripts/bao_bootstrap.py.
CREATE ROLE debezium WITH LOGIN REPLICATION;
CREATE ROLE flink WITH LOGIN;
CREATE ROLE app WITH LOGIN;

\connect cmdb
CREATE TABLE ec2_ci (
    instance_id   text PRIMARY KEY,
    account_id    text NOT NULL,
    region        text NOT NULL,
    app_name      text NOT NULL,
    environment   text NOT NULL CHECK (environment IN ('prod', 'staging', 'dev')),
    owner_email   text NOT NULL,
    support_group text NOT NULL,
    criticality   text NOT NULL CHECK (criticality IN ('tier1', 'tier2', 'tier3')),
    updated_at    timestamptz NOT NULL DEFAULT now()
);
-- Debezium needs the full old row on UPDATE/DELETE so Flink can retract the previous version.
ALTER TABLE ec2_ci REPLICA IDENTITY FULL;
GRANT SELECT ON ec2_ci TO debezium;
GRANT ALL ON ec2_ci TO app;
-- Created up front so Debezium's user needs no superuser rights (publication.autocreate.mode=disabled).
CREATE PUBLICATION dbz_cmdb FOR TABLE ec2_ci;

\connect serving
CREATE TABLE instance_current_state (
    instance_id       text PRIMARY KEY,
    account_id        text NOT NULL,
    region            text NOT NULL,
    state             text NOT NULL,
    previous_state    text,
    state_since       timestamp(3) NOT NULL,   -- event time (UTC) of the last transition
    app_name          text,
    environment       text,
    owner_email       text,
    criticality       text,
    transitions_1h    bigint NOT NULL,      -- state changes in the hour up to the last one
    updated_at        timestamp(3) NOT NULL    -- when Flink wrote this row (UTC)
);
CREATE INDEX ON instance_current_state (state);
CREATE INDEX ON instance_current_state (app_name);

CREATE TABLE state_change_counts_1m (
    window_start timestamp(3) NOT NULL,
    window_end   timestamp(3) NOT NULL,
    region       text NOT NULL,
    state        text NOT NULL,
    events       bigint NOT NULL,
    instances    bigint NOT NULL,
    PRIMARY KEY (window_start, region, state)
);

CREATE TABLE alert_triage (
    alert_id     text PRIMARY KEY,
    instance_id  text NOT NULL,
    alert_type   text NOT NULL,
    severity     text NOT NULL,
    summary      text NOT NULL,
    likely_cause text NOT NULL,
    next_steps   jsonb NOT NULL,
    notify       text,
    model        text NOT NULL,
    triaged_at   timestamptz NOT NULL DEFAULT now()
);

GRANT INSERT, UPDATE, DELETE, SELECT ON instance_current_state, state_change_counts_1m TO flink;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO app;
GRANT INSERT, UPDATE ON alert_triage TO app;

BEGIN;

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS processed_messages (
    message_id text PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS failed_messages (
    message_id text,
    body_sha256 text NOT NULL,
    body bytea NOT NULL,
    reason text NOT NULL,
    failed_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE NULLS NOT DISTINCT (message_id, body_sha256)
);

CREATE TABLE IF NOT EXISTS companies (
    registration_number text PRIMARY KEY,
    company_name text,
    nature_of_business text,
    state text,
    industry text,
    source_updated_at timestamptz NOT NULL,
    source_message_id text NOT NULL REFERENCES processed_messages (message_id),
    is_active boolean NOT NULL,
    embedding vector(384)
);

COMMIT;

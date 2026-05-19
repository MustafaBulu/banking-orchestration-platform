CREATE TABLE IF NOT EXISTS event_store
(
    id             BIGSERIAL PRIMARY KEY,
    event_id       VARCHAR(64)  NOT NULL UNIQUE,
    stream_id      VARCHAR(64)  NOT NULL,
    stream_type    VARCHAR(64)  NOT NULL,
    stream_version BIGINT       NOT NULL,
    event_type     VARCHAR(128) NOT NULL,
    event_version  INTEGER      NOT NULL,
    payload        JSONB        NOT NULL,
    metadata       JSONB        NOT NULL,
    occurred_at    TIMESTAMPTZ  NOT NULL,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_event_store_stream_version
    ON event_store (stream_id, stream_version);

CREATE INDEX IF NOT EXISTS idx_event_store_stream_id
    ON event_store (stream_id);

CREATE TABLE IF NOT EXISTS outbox
(
    id             UUID PRIMARY KEY,
    aggregate_id   VARCHAR(64)  NOT NULL,
    aggregate_type VARCHAR(64)  NOT NULL,
    event_id       VARCHAR(64)  NOT NULL UNIQUE,
    event_type     VARCHAR(128) NOT NULL,
    event_version  INTEGER      NOT NULL,
    payload        JSONB        NOT NULL,
    status         VARCHAR(32)  NOT NULL,
    retry_count    INTEGER      NOT NULL DEFAULT 0,
    available_at   TIMESTAMPTZ  NOT NULL,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    published_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_status_available_at
    ON outbox (status, available_at);

CREATE TABLE IF NOT EXISTS idempotency_key
(
    id            BIGSERIAL PRIMARY KEY,
    idempotency_key VARCHAR(128) NOT NULL UNIQUE,
    request_hash  VARCHAR(128)   NOT NULL,
    response_body JSONB,
    status_code   INTEGER,
    created_at    TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    expires_at    TIMESTAMPTZ    NOT NULL
);

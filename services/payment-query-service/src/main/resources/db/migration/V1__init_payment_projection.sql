CREATE TABLE IF NOT EXISTS payment_overview
(
    payment_id   VARCHAR(64) PRIMARY KEY,
    customer_id  VARCHAR(64)   NOT NULL,
    amount       NUMERIC(19, 4) NOT NULL,
    currency     VARCHAR(8)    NOT NULL,
    status       VARCHAR(32)   NOT NULL,
    created_at   TIMESTAMPTZ   NOT NULL,
    updated_at   TIMESTAMPTZ   NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_event
(
    event_id      VARCHAR(64) PRIMARY KEY,
    consumer_name VARCHAR(64) NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

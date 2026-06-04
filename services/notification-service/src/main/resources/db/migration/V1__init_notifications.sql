CREATE TABLE IF NOT EXISTS notifications
(
    notification_id UUID PRIMARY KEY,
    payment_id      VARCHAR(64)  NOT NULL,
    customer_id     VARCHAR(64)  NOT NULL,
    source_event_id VARCHAR(64)  NOT NULL,
    channel         VARCHAR(32)  NOT NULL,
    status          VARCHAR(32)  NOT NULL,
    message         TEXT         NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL,
    UNIQUE (source_event_id, channel)
);

CREATE INDEX IF NOT EXISTS idx_notifications_payment_id
    ON notifications (payment_id);

CREATE TABLE IF NOT EXISTS processed_notification_event
(
    event_id      VARCHAR(64) PRIMARY KEY,
    consumer_name VARCHAR(64) NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

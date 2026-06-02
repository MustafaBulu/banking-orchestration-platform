CREATE TABLE IF NOT EXISTS ledger_entries
(
    entry_id        UUID PRIMARY KEY,
    payment_id      VARCHAR(64)    NOT NULL,
    customer_id     VARCHAR(64)    NOT NULL,
    source_event_id VARCHAR(64)    NOT NULL,
    line_number     SMALLINT       NOT NULL,
    account_code    VARCHAR(64)    NOT NULL,
    entry_type      VARCHAR(16)    NOT NULL CHECK (entry_type IN ('DEBIT', 'CREDIT')),
    amount          NUMERIC(19, 4) NOT NULL CHECK (amount > 0),
    currency        VARCHAR(8)     NOT NULL,
    occurred_at     TIMESTAMPTZ    NOT NULL,
    created_at      TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    UNIQUE (source_event_id, line_number)
);

CREATE INDEX IF NOT EXISTS idx_ledger_entries_payment_id
    ON ledger_entries (payment_id);

CREATE TABLE IF NOT EXISTS processed_ledger_event
(
    event_id      VARCHAR(64) PRIMARY KEY,
    consumer_name VARCHAR(64) NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

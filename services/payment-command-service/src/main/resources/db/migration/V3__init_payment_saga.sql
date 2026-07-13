CREATE TABLE IF NOT EXISTS payment_saga
(
    saga_id        VARCHAR(64) PRIMARY KEY,
    payment_id     VARCHAR(64)    NOT NULL UNIQUE,
    customer_id    VARCHAR(64)    NOT NULL,
    amount         NUMERIC(19, 4) NOT NULL CHECK (amount > 0),
    currency       VARCHAR(8)     NOT NULL,
    reservation_id VARCHAR(128),
    current_step   VARCHAR(64)    NOT NULL,
    status         VARCHAR(32)    NOT NULL CHECK (status IN ('STARTED', 'COMPLETED', 'COMPENSATING', 'COMPENSATED', 'FAILED')),
    recovery_owner VARCHAR(128),
    recovery_locked_until TIMESTAMPTZ,
    created_at     TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_payment_saga_status_updated_at
    ON payment_saga (status, updated_at);

CREATE TABLE IF NOT EXISTS payment_saga_step
(
    id                  BIGSERIAL PRIMARY KEY,
    saga_id             VARCHAR(64) NOT NULL REFERENCES payment_saga (saga_id) ON DELETE CASCADE,
    step                VARCHAR(64) NOT NULL,
    status              VARCHAR(32) NOT NULL CHECK (status IN ('PENDING', 'DONE', 'FAILED', 'SKIPPED')),
    compensation_status VARCHAR(32) NOT NULL CHECK (compensation_status IN ('NOT_REQUIRED', 'PENDING', 'DONE', 'FAILED')),
    attempt             INTEGER NOT NULL DEFAULT 0,
    last_error          TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_payment_saga_step UNIQUE (saga_id, step)
);

CREATE INDEX IF NOT EXISTS idx_payment_saga_step_saga_id
    ON payment_saga_step (saga_id);

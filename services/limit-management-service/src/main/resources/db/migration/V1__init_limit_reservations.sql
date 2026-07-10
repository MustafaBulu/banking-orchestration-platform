CREATE TABLE IF NOT EXISTS limit_reservation
(
    reservation_id VARCHAR(80) PRIMARY KEY,
    payment_id     VARCHAR(64)    NOT NULL,
    customer_id    VARCHAR(64)    NOT NULL,
    amount         NUMERIC(19, 4) NOT NULL CHECK (amount > 0),
    currency       VARCHAR(8)     NOT NULL,
    status         VARCHAR(16)    NOT NULL CHECK (status IN ('ACTIVE', 'RELEASED', 'EXPIRED')),
    reserved_at    TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    expires_at     TIMESTAMPTZ    NOT NULL,
    released_at    TIMESTAMPTZ,
    updated_at     TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_limit_reservation_active_payment
    ON limit_reservation (payment_id)
    WHERE status = 'ACTIVE';

CREATE INDEX IF NOT EXISTS idx_limit_reservation_status_expires_at
    ON limit_reservation (status, expires_at);

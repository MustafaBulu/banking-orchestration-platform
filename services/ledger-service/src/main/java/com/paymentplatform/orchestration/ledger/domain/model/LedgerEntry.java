package com.paymentplatform.orchestration.ledger.domain.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record LedgerEntry(
        UUID entryId,
        String paymentId,
        String customerId,
        String sourceEventId,
        int lineNumber,
        String accountCode,
        LedgerEntryType entryType,
        BigDecimal amount,
        String currency,
        Instant occurredAt
) {

    public LedgerEntry {
        Objects.requireNonNull(entryId, "entryId is required");
        Objects.requireNonNull(paymentId, "paymentId is required");
        Objects.requireNonNull(customerId, "customerId is required");
        Objects.requireNonNull(sourceEventId, "sourceEventId is required");
        Objects.requireNonNull(accountCode, "accountCode is required");
        Objects.requireNonNull(entryType, "entryType is required");
        Objects.requireNonNull(amount, "amount is required");
        Objects.requireNonNull(currency, "currency is required");
        Objects.requireNonNull(occurredAt, "occurredAt is required");
        if (lineNumber <= 0) {
            throw new IllegalArgumentException("lineNumber must be positive");
        }
        if (amount.signum() <= 0) {
            throw new IllegalArgumentException("amount must be positive");
        }
    }
}

package com.paymentplatform.orchestration.ledger.application.service;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentCreatedLedgerEvent(
        String eventId,
        String paymentId,
        String customerId,
        BigDecimal amount,
        String currency,
        Instant occurredAt
) {
}

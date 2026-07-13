package com.paymentplatform.orchestration.ledger.application.service;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentLedgerEvent(
        String eventId,
        String paymentId,
        String eventType,
        String customerId,
        BigDecimal amount,
        String currency,
        Instant occurredAt
) {
}

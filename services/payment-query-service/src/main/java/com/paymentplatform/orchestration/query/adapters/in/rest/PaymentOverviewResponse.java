package com.paymentplatform.orchestration.query.adapters.in.rest;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentOverviewResponse(
        String paymentId,
        String customerId,
        BigDecimal amount,
        String currency,
        String status,
        Instant createdAt,
        Instant updatedAt
) {
}

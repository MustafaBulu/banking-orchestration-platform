package com.paymentplatform.orchestration.query.adapters.out.postgres;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentOverview(
        String paymentId,
        String customerId,
        BigDecimal amount,
        String currency,
        String status,
        Instant createdAt,
        Instant updatedAt
) {
}

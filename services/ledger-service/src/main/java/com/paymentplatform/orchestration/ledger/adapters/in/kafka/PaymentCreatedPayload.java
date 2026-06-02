package com.paymentplatform.orchestration.ledger.adapters.in.kafka;

import java.math.BigDecimal;

public record PaymentCreatedPayload(
        String customerId,
        BigDecimal amount,
        String currency
) {
}

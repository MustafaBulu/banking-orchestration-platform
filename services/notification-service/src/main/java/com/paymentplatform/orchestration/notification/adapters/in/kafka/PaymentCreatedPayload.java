package com.paymentplatform.orchestration.notification.adapters.in.kafka;

import java.math.BigDecimal;

public record PaymentCreatedPayload(
        String customerId,
        BigDecimal amount,
        String currency
) {
}

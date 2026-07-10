package com.paymentplatform.orchestration.events.payment.v1;

import java.math.BigDecimal;

public record PaymentCreatedPayload(
        String customerId,
        BigDecimal amount,
        String currency
) {
}

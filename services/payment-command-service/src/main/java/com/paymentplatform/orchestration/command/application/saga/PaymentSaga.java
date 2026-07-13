package com.paymentplatform.orchestration.command.application.saga;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentSaga(
        String sagaId,
        String paymentId,
        String customerId,
        BigDecimal amount,
        String currency,
        String reservationId,
        PaymentSagaStepName currentStep,
        PaymentSagaStatus status,
        Instant createdAt,
        Instant updatedAt
) {
}

package com.paymentplatform.orchestration.command.application.saga;

import java.time.Instant;

public record PaymentSagaStep(
        String sagaId,
        PaymentSagaStepName step,
        PaymentSagaStepStatus status,
        PaymentSagaCompensationStatus compensationStatus,
        int attempt,
        String lastError,
        Instant createdAt,
        Instant updatedAt
) {
}

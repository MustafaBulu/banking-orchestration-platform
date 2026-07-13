package com.paymentplatform.orchestration.command.application.saga;

public enum PaymentSagaStatus {
    STARTED,
    COMPLETED,
    COMPENSATING,
    COMPENSATED,
    FAILED
}

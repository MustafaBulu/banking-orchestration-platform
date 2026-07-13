package com.paymentplatform.orchestration.command.application.saga;

public enum PaymentSagaStepStatus {
    PENDING,
    DONE,
    FAILED,
    SKIPPED
}

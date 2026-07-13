package com.paymentplatform.orchestration.command.application.saga;

public enum PaymentSagaCompensationStatus {
    NOT_REQUIRED,
    PENDING,
    DONE,
    FAILED
}

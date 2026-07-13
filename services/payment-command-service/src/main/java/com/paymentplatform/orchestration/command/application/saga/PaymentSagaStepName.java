package com.paymentplatform.orchestration.command.application.saga;

public enum PaymentSagaStepName {
    FRAUD_CHECK,
    RESERVE_LIMIT,
    AUTHORIZE,
    CAPTURE
}

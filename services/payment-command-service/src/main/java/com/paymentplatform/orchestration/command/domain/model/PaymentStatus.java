package com.paymentplatform.orchestration.command.domain.model;

public enum PaymentStatus {
    CREATED,
    AUTHORIZED,
    CAPTURED,
    VOIDED,
    REFUNDED,
    CANCELLED,
    FAILED
}

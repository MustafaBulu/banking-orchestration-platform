package com.paymentplatform.orchestration.command.infrastructure.idempotency;

public class IdempotencyInProgressException extends RuntimeException {

    public IdempotencyInProgressException(String message) {
        super(message);
    }
}

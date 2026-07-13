package com.paymentplatform.orchestration.command.domain.exception;

public class InvalidPaymentStateTransitionException extends RuntimeException {

    public InvalidPaymentStateTransitionException(String message) {
        super(message);
    }
}

package com.paymentplatform.orchestration.command.application.saga;

public class AcquirerDeclinedException extends RuntimeException {

    public AcquirerDeclinedException(String message) {
        super(message);
    }
}

package com.paymentplatform.orchestration.command.adapters.in.rest;

import com.paymentplatform.orchestration.command.domain.exception.PaymentRejectedException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class PaymentCommandExceptionHandler {

    @ExceptionHandler(PaymentRejectedException.class)
    @ResponseStatus(HttpStatus.UNPROCESSABLE_CONTENT)
    public ErrorResponse handleRejected(PaymentRejectedException ex) {
        return new ErrorResponse("PAYMENT_REJECTED", ex.getMessage());
    }

    public record ErrorResponse(String code, String message) {
    }
}

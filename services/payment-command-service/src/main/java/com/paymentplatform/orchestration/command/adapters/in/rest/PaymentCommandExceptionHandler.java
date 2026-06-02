package com.paymentplatform.orchestration.command.adapters.in.rest;

import com.paymentplatform.orchestration.command.domain.exception.PaymentRejectedException;
import com.paymentplatform.orchestration.command.infrastructure.idempotency.IdempotencyConflictException;
import com.paymentplatform.orchestration.command.infrastructure.idempotency.InvalidIdempotencyKeyException;
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

    @ExceptionHandler(IdempotencyConflictException.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    public ErrorResponse handleIdempotencyConflict(IdempotencyConflictException ex) {
        return new ErrorResponse("IDEMPOTENCY_CONFLICT", ex.getMessage());
    }

    @ExceptionHandler({InvalidIdempotencyKeyException.class, IllegalArgumentException.class})
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleBadRequest(RuntimeException ex) {
        return new ErrorResponse("BAD_REQUEST", ex.getMessage());
    }

    public record ErrorResponse(String code, String message) {
    }
}

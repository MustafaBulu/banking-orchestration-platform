package com.paymentplatform.orchestration.command.adapters.in.rest;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;

public record CreatePaymentRequest(
        @NotBlank String customerId,
        @NotNull @DecimalMin(value = "0.01") BigDecimal amount,
        @NotBlank String currency
) {
}

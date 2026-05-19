package com.paymentplatform.orchestration.command.application.command;

import java.math.BigDecimal;

public record CreatePaymentCommand(
        String paymentId,
        String customerId,
        BigDecimal amount,
        String currency
) {
}

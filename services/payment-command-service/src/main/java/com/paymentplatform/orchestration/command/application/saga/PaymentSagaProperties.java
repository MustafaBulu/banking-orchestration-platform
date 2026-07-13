package com.paymentplatform.orchestration.command.application.saga;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.payment-saga")
public record PaymentSagaProperties(
        int maxAttempts,
        int recoveryBatchSize,
        long recoveryStuckAfterMs
) {
}

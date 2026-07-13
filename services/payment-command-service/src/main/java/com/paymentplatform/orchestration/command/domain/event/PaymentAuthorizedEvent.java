package com.paymentplatform.orchestration.command.domain.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record PaymentAuthorizedEvent(
        String eventId,
        String aggregateId,
        Instant occurredAt,
        String customerId,
        BigDecimal amount,
        String currency
) implements PaymentEvent {

    public static PaymentAuthorizedEvent of(
            String paymentId,
            String customerId,
            BigDecimal amount,
            String currency
    ) {
        return new PaymentAuthorizedEvent(
                UUID.randomUUID().toString(),
                paymentId,
                Instant.now(),
                customerId,
                amount,
                currency
        );
    }

    @Override
    public String eventType() {
        return "PaymentAuthorized";
    }
}

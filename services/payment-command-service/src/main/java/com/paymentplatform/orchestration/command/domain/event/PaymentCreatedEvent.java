package com.paymentplatform.orchestration.command.domain.event;

import com.paymentplatform.orchestration.common.domain.DomainEvent;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record PaymentCreatedEvent(
        String eventId,
        String aggregateId,
        Instant occurredAt,
        String customerId,
        BigDecimal amount,
        String currency
) implements DomainEvent {

    public static PaymentCreatedEvent of(
            String paymentId,
            String customerId,
            BigDecimal amount,
            String currency
    ) {
        return new PaymentCreatedEvent(
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
        return "PaymentCreated";
    }
}

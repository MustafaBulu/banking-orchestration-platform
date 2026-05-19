package com.paymentplatform.orchestration.query.adapters.in.kafka;

import java.time.Instant;

public record PaymentEventEnvelope(
        String eventId,
        String aggregateId,
        String eventType,
        Instant occurredAt,
        PaymentCreatedPayload data
) {
}

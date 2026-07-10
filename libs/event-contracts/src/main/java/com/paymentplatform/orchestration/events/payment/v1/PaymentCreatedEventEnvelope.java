package com.paymentplatform.orchestration.events.payment.v1;

import java.time.Instant;

public record PaymentCreatedEventEnvelope(
        String eventId,
        String aggregateId,
        String eventType,
        int eventVersion,
        Instant occurredAt,
        PaymentCreatedPayload data
) {

    public static final String EVENT_TYPE = "PaymentCreated";
    public static final int EVENT_VERSION = 1;

    public PaymentCreatedEventEnvelope(
            String eventId,
            String aggregateId,
            Instant occurredAt,
            PaymentCreatedPayload data
    ) {
        this(eventId, aggregateId, EVENT_TYPE, EVENT_VERSION, occurredAt, data);
    }
}

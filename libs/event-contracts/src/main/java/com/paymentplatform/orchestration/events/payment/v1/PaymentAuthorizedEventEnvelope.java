package com.paymentplatform.orchestration.events.payment.v1;

import java.time.Instant;

public record PaymentAuthorizedEventEnvelope(
        String eventId,
        String aggregateId,
        String eventType,
        int eventVersion,
        Instant occurredAt,
        PaymentLifecyclePayload data
) {

    public static final String EVENT_TYPE = "PaymentAuthorized";
    public static final int EVENT_VERSION = 1;

    public PaymentAuthorizedEventEnvelope(
            String eventId,
            String aggregateId,
            Instant occurredAt,
            PaymentLifecyclePayload data
    ) {
        this(eventId, aggregateId, EVENT_TYPE, EVENT_VERSION, occurredAt, data);
    }
}

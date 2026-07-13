package com.paymentplatform.orchestration.events.payment.v1;

import java.time.Instant;

public record PaymentRefundedEventEnvelope(
        String eventId,
        String aggregateId,
        String eventType,
        int eventVersion,
        Instant occurredAt,
        PaymentLifecyclePayload data
) {

    public static final String EVENT_TYPE = "PaymentRefunded";
    public static final int EVENT_VERSION = 1;

    public PaymentRefundedEventEnvelope(
            String eventId,
            String aggregateId,
            Instant occurredAt,
            PaymentLifecyclePayload data
    ) {
        this(eventId, aggregateId, EVENT_TYPE, EVENT_VERSION, occurredAt, data);
    }
}

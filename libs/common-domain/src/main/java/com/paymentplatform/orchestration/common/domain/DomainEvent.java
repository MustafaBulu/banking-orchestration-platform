package com.paymentplatform.orchestration.common.domain;

import java.time.Instant;

public interface DomainEvent {

    String eventId();

    String aggregateId();

    Instant occurredAt();

    String eventType();
}

package com.paymentplatform.orchestration.command.adapters.out.postgres;

import java.time.Instant;
import java.util.UUID;

public record OutboxMessage(
        UUID id,
        String eventId,
        String eventType,
        String payload,
        int retryCount,
        Instant availableAt
) {
}

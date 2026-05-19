package com.paymentplatform.orchestration.command.infrastructure.outbox;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.outbox.relay")
public record OutboxRelayProperties(
        boolean enabled,
        int batchSize,
        int maxRetries,
        long pollDelayMs,
        long baseBackoffMs,
        String topic
) {
}

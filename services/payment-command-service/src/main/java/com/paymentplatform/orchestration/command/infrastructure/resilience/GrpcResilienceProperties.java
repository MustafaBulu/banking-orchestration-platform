package com.paymentplatform.orchestration.command.infrastructure.resilience;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.resilience.grpc")
public record GrpcResilienceProperties(
        int timeoutMs,
        int maxAttempts,
        long waitDurationMs,
        int failureRateThreshold,
        int slidingWindowSize,
        int minimumNumberOfCalls,
        int permittedCallsInHalfOpenState,
        int maxConcurrentCalls
) {
}

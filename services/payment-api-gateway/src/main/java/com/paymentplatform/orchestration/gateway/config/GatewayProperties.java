package com.paymentplatform.orchestration.gateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.gateway")
public record GatewayProperties(
        String commandBaseUrl,
        String queryBaseUrl,
        int timeoutMs,
        int rateLimitPerMinute
) {
}

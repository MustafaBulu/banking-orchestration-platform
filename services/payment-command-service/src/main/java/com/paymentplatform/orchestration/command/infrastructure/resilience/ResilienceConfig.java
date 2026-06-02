package com.paymentplatform.orchestration.command.infrastructure.resilience;

import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.RetryRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
public class ResilienceConfig {

    @Bean
    public CircuitBreakerRegistry circuitBreakerRegistry(GrpcResilienceProperties props) {
        return CircuitBreakerRegistry.of(io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.custom()
                .failureRateThreshold(Math.max(1, props.failureRateThreshold()))
                .slidingWindowSize(Math.max(5, props.slidingWindowSize()))
                .minimumNumberOfCalls(Math.max(1, props.minimumNumberOfCalls()))
                .permittedNumberOfCallsInHalfOpenState(Math.max(1, props.permittedCallsInHalfOpenState()))
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .build());
    }

    @Bean
    public RetryRegistry retryRegistry(GrpcResilienceProperties props) {
        return RetryRegistry.of(io.github.resilience4j.retry.RetryConfig.custom()
                .maxAttempts(Math.max(1, props.maxAttempts()))
                .waitDuration(Duration.ofMillis(Math.max(50, props.waitDurationMs())))
                .build());
    }

    @Bean
    public BulkheadRegistry bulkheadRegistry(GrpcResilienceProperties props) {
        return BulkheadRegistry.of(io.github.resilience4j.bulkhead.BulkheadConfig.custom()
                .maxConcurrentCalls(Math.max(1, props.maxConcurrentCalls()))
                .maxWaitDuration(Duration.ofMillis(0))
                .build());
    }
}

package com.paymentplatform.orchestration.command.adapters.out.grpc;

import com.paymentplatform.orchestration.command.application.port.out.LimitCheckPort;
import com.paymentplatform.orchestration.command.domain.model.Money;
import com.paymentplatform.orchestration.command.infrastructure.resilience.GrpcResilienceProperties;
import com.paymentplatform.orchestration.contracts.limit.v1.LimitControlServiceGrpc;
import com.paymentplatform.orchestration.contracts.limit.v1.LimitReleaseRequest;
import com.paymentplatform.orchestration.contracts.limit.v1.LimitReserveRequest;
import com.paymentplatform.orchestration.contracts.limit.v1.LimitReserveResponse;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Component
public class LimitGrpcAdapter implements LimitCheckPort {

    private static final String RESILIENCE_NAME = "limitGrpc";

    private final LimitControlServiceGrpc.LimitControlServiceBlockingStub blockingStub;
    private final GrpcResilienceProperties properties;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final Bulkhead bulkhead;
    private final Counter releaseFailedCounter;

    public LimitGrpcAdapter(
            LimitControlServiceGrpc.LimitControlServiceBlockingStub blockingStub,
            GrpcResilienceProperties properties,
            CircuitBreakerRegistry circuitBreakerRegistry,
            RetryRegistry retryRegistry,
            BulkheadRegistry bulkheadRegistry,
            MeterRegistry meterRegistry
    ) {
        this.blockingStub = blockingStub;
        this.properties = properties;
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(RESILIENCE_NAME);
        this.retry = retryRegistry.retry(RESILIENCE_NAME);
        this.bulkhead = bulkheadRegistry.bulkhead(RESILIENCE_NAME);
        this.releaseFailedCounter = Counter.builder("limit.reservation.release.failed")
                .description("Limit reservation compensations that could not be delivered")
                .register(meterRegistry);
    }

    @Override
    public LimitCheckResult reserve(String paymentId, String customerId, Money money) {
        Supplier<LimitCheckResult> call = () -> doGrpcCall(paymentId, customerId, money);
        Supplier<LimitCheckResult> withCb = CircuitBreaker.decorateSupplier(circuitBreaker, call);
        Supplier<LimitCheckResult> withRetry = Retry.decorateSupplier(retry, withCb);
        Supplier<LimitCheckResult> withBulkhead = Bulkhead.decorateSupplier(bulkhead, withRetry);
        try {
            return withBulkhead.get();
        } catch (Exception ex) {
            return new LimitCheckResult(false, "LIMIT_SERVICE_UNAVAILABLE", null);
        }
    }

    @Override
    public void release(String reservationId) {
        if (reservationId == null || reservationId.isBlank()) {
            return;
        }
        Runnable call = () -> doRelease(reservationId);
        Runnable withCb = CircuitBreaker.decorateRunnable(circuitBreaker, call);
        Runnable withRetry = Retry.decorateRunnable(retry, withCb);
        Runnable withBulkhead = Bulkhead.decorateRunnable(bulkhead, withRetry);
        try {
            withBulkhead.run();
        } catch (Exception ex) {
            releaseFailedCounter.increment();
        }
    }

    private void doRelease(String reservationId) {
        LimitReleaseRequest request = LimitReleaseRequest.newBuilder()
                .setReservationId(reservationId)
                .build();
        blockingStub
                .withDeadlineAfter(properties.timeoutMs(), TimeUnit.MILLISECONDS)
                .release(request);
    }

    private LimitCheckResult doGrpcCall(String paymentId, String customerId, Money money) {
        LimitReserveRequest request = LimitReserveRequest.newBuilder()
                .setPaymentId(paymentId)
                .setCustomerId(customerId)
                .setAmount(money.amount().toPlainString())
                .setCurrency(money.currency().getCurrencyCode())
                .build();
        LimitReserveResponse response = blockingStub
                .withDeadlineAfter(properties.timeoutMs(), TimeUnit.MILLISECONDS)
                .reserve(request);
        return new LimitCheckResult(response.getApproved(), response.getReasonCode(), response.getReservationId());
    }
}

package com.paymentplatform.orchestration.command.adapters.out.grpc;

import com.paymentplatform.orchestration.command.application.port.out.FraudCheckPort;
import com.paymentplatform.orchestration.command.domain.model.Money;
import com.paymentplatform.orchestration.command.infrastructure.resilience.GrpcResilienceProperties;
import com.paymentplatform.orchestration.contracts.fraud.v1.FraudCheckRequest;
import com.paymentplatform.orchestration.contracts.fraud.v1.FraudCheckResponse;
import com.paymentplatform.orchestration.contracts.fraud.v1.FraudControlServiceGrpc;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Component
public class FraudGrpcAdapter implements FraudCheckPort {

    private static final String RESILIENCE_NAME = "fraudGrpc";

    private final FraudControlServiceGrpc.FraudControlServiceBlockingStub blockingStub;
    private final GrpcResilienceProperties properties;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final Bulkhead bulkhead;

    public FraudGrpcAdapter(
            FraudControlServiceGrpc.FraudControlServiceBlockingStub blockingStub,
            GrpcResilienceProperties properties,
            CircuitBreakerRegistry circuitBreakerRegistry,
            RetryRegistry retryRegistry,
            BulkheadRegistry bulkheadRegistry
    ) {
        this.blockingStub = blockingStub;
        this.properties = properties;
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(RESILIENCE_NAME);
        this.retry = retryRegistry.retry(RESILIENCE_NAME);
        this.bulkhead = bulkheadRegistry.bulkhead(RESILIENCE_NAME);
    }

    @Override
    public FraudCheckResult evaluate(String paymentId, String customerId, Money money) {
        Supplier<FraudCheckResult> call = () -> doGrpcCall(paymentId, customerId, money);
        Supplier<FraudCheckResult> withCb = CircuitBreaker.decorateSupplier(circuitBreaker, call);
        Supplier<FraudCheckResult> withRetry = Retry.decorateSupplier(retry, withCb);
        Supplier<FraudCheckResult> withBulkhead = Bulkhead.decorateSupplier(bulkhead, withRetry);
        try {
            return withBulkhead.get();
        } catch (Exception ex) {
            return new FraudCheckResult(false, "FRAUD_SERVICE_UNAVAILABLE", 100);
        }
    }

    private FraudCheckResult doGrpcCall(String paymentId, String customerId, Money money) {
        FraudCheckRequest request = FraudCheckRequest.newBuilder()
                .setPaymentId(paymentId)
                .setCustomerId(customerId)
                .setAmount(money.amount().toPlainString())
                .setCurrency(money.currency().getCurrencyCode())
                .build();
        FraudCheckResponse response = blockingStub
                .withDeadlineAfter(properties.timeoutMs(), TimeUnit.MILLISECONDS)
                .evaluate(request);
        return new FraudCheckResult(response.getApproved(), response.getReasonCode(), response.getRiskScore());
    }
}

package com.paymentplatform.orchestration.command.adapters.out.grpc;

import com.paymentplatform.orchestration.command.application.port.out.AcquirerPort;
import com.paymentplatform.orchestration.command.domain.model.Money;
import com.paymentplatform.orchestration.command.infrastructure.resilience.GrpcResilienceProperties;
import com.paymentplatform.orchestration.contracts.acquirer.v1.AcquirerAuthorizeRequest;
import com.paymentplatform.orchestration.contracts.acquirer.v1.AcquirerCaptureRequest;
import com.paymentplatform.orchestration.contracts.acquirer.v1.AcquirerControlServiceGrpc;
import com.paymentplatform.orchestration.contracts.acquirer.v1.AcquirerOperation;
import com.paymentplatform.orchestration.contracts.acquirer.v1.AcquirerOperationResponse;
import com.paymentplatform.orchestration.contracts.acquirer.v1.AcquirerRefundRequest;
import com.paymentplatform.orchestration.contracts.acquirer.v1.AcquirerStatusRequest;
import com.paymentplatform.orchestration.contracts.acquirer.v1.AcquirerVoidRequest;
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
public class AcquirerGrpcAdapter implements AcquirerPort {

    private static final String RESILIENCE_NAME = "acquirerGrpc";

    private final AcquirerControlServiceGrpc.AcquirerControlServiceBlockingStub blockingStub;
    private final GrpcResilienceProperties properties;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final Bulkhead bulkhead;
    private final Counter ambiguousResolvedCounter;

    public AcquirerGrpcAdapter(
            AcquirerControlServiceGrpc.AcquirerControlServiceBlockingStub blockingStub,
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
        this.ambiguousResolvedCounter = Counter.builder("acquirer.outcome.ambiguous.resolved")
                .description("Ambiguous acquirer results resolved by an idempotent status re-query")
                .register(meterRegistry);
    }

    @Override
    public AcquirerResult authorize(String paymentId, Money money) {
        AcquirerAuthorizeRequest request = AcquirerAuthorizeRequest.newBuilder()
                .setPaymentId(paymentId)
                .setAmount(money.amount().toPlainString())
                .setCurrency(money.currency().getCurrencyCode())
                .build();
        return callOrReQuery(paymentId, AcquirerOperation.AUTHORIZE, () -> deadlineStub().authorize(request));
    }

    @Override
    public AcquirerResult capture(String paymentId, Money money) {
        AcquirerCaptureRequest request = AcquirerCaptureRequest.newBuilder()
                .setPaymentId(paymentId)
                .setAmount(money.amount().toPlainString())
                .setCurrency(money.currency().getCurrencyCode())
                .build();
        return callOrReQuery(paymentId, AcquirerOperation.CAPTURE, () -> deadlineStub().capture(request));
    }

    @Override
    public AcquirerResult voidAuthorization(String paymentId) {
        AcquirerVoidRequest request = AcquirerVoidRequest.newBuilder()
                .setPaymentId(paymentId)
                .build();
        return callOrReQuery(paymentId, AcquirerOperation.VOID, () -> deadlineStub().voidAuthorization(request));
    }

    @Override
    public AcquirerResult refund(String paymentId, Money money) {
        AcquirerRefundRequest request = AcquirerRefundRequest.newBuilder()
                .setPaymentId(paymentId)
                .setAmount(money.amount().toPlainString())
                .setCurrency(money.currency().getCurrencyCode())
                .build();
        return callOrReQuery(paymentId, AcquirerOperation.REFUND, () -> deadlineStub().refund(request));
    }

    private AcquirerResult callOrReQuery(
            String paymentId,
            AcquirerOperation operation,
            Supplier<AcquirerOperationResponse> call
    ) {
        Supplier<AcquirerResult> mapped = () -> map(call.get());
        Supplier<AcquirerResult> withCb = CircuitBreaker.decorateSupplier(circuitBreaker, mapped);
        Supplier<AcquirerResult> withRetry = Retry.decorateSupplier(retry, withCb);
        Supplier<AcquirerResult> withBulkhead = Bulkhead.decorateSupplier(bulkhead, withRetry);
        try {
            AcquirerResult result = withBulkhead.get();
            if (result.outcome() == Outcome.UNKNOWN) {
                return reQuery(paymentId, operation);
            }
            return result;
        } catch (Exception ex) {
            return reQuery(paymentId, operation);
        }
    }

    private AcquirerResult reQuery(String paymentId, AcquirerOperation operation) {
        AcquirerStatusRequest request = AcquirerStatusRequest.newBuilder()
                .setPaymentId(paymentId)
                .setOperation(operation)
                .build();
        try {
            AcquirerResult result = map(deadlineStub().getStatus(request));
            if (result.outcome() != Outcome.UNKNOWN) {
                ambiguousResolvedCounter.increment();
            }
            return result;
        } catch (Exception ex) {
            return new AcquirerResult(Outcome.UNKNOWN, "ACQUIRER_UNAVAILABLE", null);
        }
    }

    private AcquirerControlServiceGrpc.AcquirerControlServiceBlockingStub deadlineStub() {
        return blockingStub.withDeadlineAfter(properties.timeoutMs(), TimeUnit.MILLISECONDS);
    }

    private AcquirerResult map(AcquirerOperationResponse response) {
        Outcome outcome = switch (response.getOutcome()) {
            case APPROVED -> Outcome.APPROVED;
            case DECLINED -> Outcome.DECLINED;
            default -> Outcome.UNKNOWN;
        };
        return new AcquirerResult(outcome, response.getReasonCode(), response.getAcquirerRef());
    }
}

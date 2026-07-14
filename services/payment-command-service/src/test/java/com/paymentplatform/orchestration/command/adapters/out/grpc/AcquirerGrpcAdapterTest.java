package com.paymentplatform.orchestration.command.adapters.out.grpc;

import com.paymentplatform.orchestration.command.application.port.out.AcquirerPort;
import com.paymentplatform.orchestration.command.domain.model.Money;
import com.paymentplatform.orchestration.command.infrastructure.resilience.GrpcResilienceProperties;
import com.paymentplatform.orchestration.contracts.acquirer.v1.AcquirerAuthorizeRequest;
import com.paymentplatform.orchestration.contracts.acquirer.v1.AcquirerControlServiceGrpc;
import com.paymentplatform.orchestration.contracts.acquirer.v1.AcquirerOperationResponse;
import com.paymentplatform.orchestration.contracts.acquirer.v1.AcquirerOutcome;
import com.paymentplatform.orchestration.contracts.acquirer.v1.AcquirerStatusRequest;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.RetryRegistry;
import io.github.resilience4j.retry.RetryConfig;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Currency;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class AcquirerGrpcAdapterTest {

    private static final Money MONEY = new Money(new BigDecimal("120.50"), Currency.getInstance("EUR"));

    private ManagedChannel channel;
    private Server server;

    @AfterEach
    void tearDown() throws Exception {
        if (channel != null) {
            channel.shutdownNow().awaitTermination(1, TimeUnit.SECONDS);
        }
        if (server != null) {
            server.shutdownNow().awaitTermination(1, TimeUnit.SECONDS);
        }
    }

    @Test
    void returnsApprovedWhenAcquirerApproves() throws Exception {
        ControllableAcquirerService service = new ControllableAcquirerService(AcquirerOutcome.APPROVED);
        AcquirerPort adapter = startAdapter(service);

        AcquirerPort.AcquirerResult result = adapter.authorize("payment-1", MONEY);

        assertThat(result.outcome()).isEqualTo(AcquirerPort.Outcome.APPROVED);
        assertThat(service.getStatusInvocations.get()).isZero();
    }

    @Test
    void returnsDeclinedWhenAcquirerDeclines() throws Exception {
        ControllableAcquirerService service = new ControllableAcquirerService(AcquirerOutcome.DECLINED);
        AcquirerPort adapter = startAdapter(service);

        AcquirerPort.AcquirerResult result = adapter.authorize("payment-1", MONEY);

        assertThat(result.outcome()).isEqualTo(AcquirerPort.Outcome.DECLINED);
    }

    @Test
    void resolvesAmbiguousAuthorizeThroughStatusReQuery() throws Exception {
        ControllableAcquirerService service = new ControllableAcquirerService(AcquirerOutcome.APPROVED);
        service.failuresBeforeResponse.set(5);
        AcquirerPort adapter = startAdapter(service);

        AcquirerPort.AcquirerResult result = adapter.authorize("payment-1", MONEY);

        assertThat(result.outcome()).isEqualTo(AcquirerPort.Outcome.APPROVED);
        assertThat(service.authorizeInvocations.get()).isGreaterThan(1);
        assertThat(service.getStatusInvocations.get()).isEqualTo(1);
    }

    @Test
    void reQueriesWhenAcquirerReturnsUnknownResponseAndResolves() throws Exception {
        ControllableAcquirerService service = new ControllableAcquirerService(AcquirerOutcome.UNKNOWN);
        service.statusOutcome = AcquirerOutcome.APPROVED;
        AcquirerPort adapter = startAdapter(service);

        AcquirerPort.AcquirerResult result = adapter.authorize("payment-1", MONEY);

        assertThat(result.outcome()).isEqualTo(AcquirerPort.Outcome.APPROVED);
        assertThat(service.authorizeInvocations.get()).isEqualTo(1);
        assertThat(service.getStatusInvocations.get()).isEqualTo(1);
    }

    @Test
    void returnsUnknownWhenNeitherCallNorReQueryResolves() throws Exception {
        ControllableAcquirerService service = new ControllableAcquirerService(AcquirerOutcome.UNKNOWN);
        service.failuresBeforeResponse.set(5);
        AcquirerPort adapter = startAdapter(service);

        AcquirerPort.AcquirerResult result = adapter.authorize("payment-1", MONEY);

        assertThat(result.outcome()).isEqualTo(AcquirerPort.Outcome.UNKNOWN);
    }

    @Test
    void authorizeIsIdempotentPerPaymentId() throws Exception {
        ControllableAcquirerService service = new ControllableAcquirerService(AcquirerOutcome.APPROVED);
        AcquirerPort adapter = startAdapter(service);

        AcquirerPort.AcquirerResult first = adapter.authorize("payment-1", MONEY);
        AcquirerPort.AcquirerResult second = adapter.authorize("payment-1", MONEY);

        assertThat(first.outcome()).isEqualTo(AcquirerPort.Outcome.APPROVED);
        assertThat(second.outcome()).isEqualTo(AcquirerPort.Outcome.APPROVED);
        assertThat(service.authorizeInvocations.get()).isEqualTo(2);
        assertThat(service.committedPayments()).isEqualTo(1);
    }

    private AcquirerPort startAdapter(ControllableAcquirerService service) throws Exception {
        String serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
                .addService(service)
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(serverName).build();

        AcquirerControlServiceGrpc.AcquirerControlServiceBlockingStub stub =
                AcquirerControlServiceGrpc.newBlockingStub(channel);
        GrpcResilienceProperties properties = new GrpcResilienceProperties(500, 2, 10, 50, 100, 100, 1, 10);
        return new AcquirerGrpcAdapter(
                stub,
                properties,
                CircuitBreakerRegistry.ofDefaults(),
                RetryRegistry.of(RetryConfig.custom()
                        .maxAttempts(2)
                        .waitDuration(Duration.ofMillis(10))
                        .build()),
                BulkheadRegistry.ofDefaults(),
                new SimpleMeterRegistry()
        );
    }

    private static final class ControllableAcquirerService
            extends AcquirerControlServiceGrpc.AcquirerControlServiceImplBase {

        private final AcquirerOutcome outcome;
        private AcquirerOutcome statusOutcome;
        private final AtomicInteger failuresBeforeResponse = new AtomicInteger();
        private final AtomicInteger authorizeInvocations = new AtomicInteger();
        private final AtomicInteger getStatusInvocations = new AtomicInteger();
        private final Set<String> authorizedPayments = ConcurrentHashMap.newKeySet();

        private ControllableAcquirerService(AcquirerOutcome outcome) {
            this.outcome = outcome;
            this.statusOutcome = outcome;
        }

        @Override
        public void authorize(AcquirerAuthorizeRequest request, StreamObserver<AcquirerOperationResponse> observer) {
            authorizeInvocations.incrementAndGet();
            if (failuresBeforeResponse.getAndUpdate(remaining -> remaining > 0 ? remaining - 1 : 0) > 0) {
                observer.onError(Status.UNAVAILABLE.withDescription("transient").asRuntimeException());
                return;
            }
            if (outcome == AcquirerOutcome.APPROVED) {
                authorizedPayments.add(request.getPaymentId());
            }
            observer.onNext(response(outcome));
            observer.onCompleted();
        }

        @Override
        public void getStatus(AcquirerStatusRequest request, StreamObserver<AcquirerOperationResponse> observer) {
            getStatusInvocations.incrementAndGet();
            observer.onNext(response(statusOutcome));
            observer.onCompleted();
        }

        private int committedPayments() {
            return authorizedPayments.size();
        }

        private AcquirerOperationResponse response(AcquirerOutcome value) {
            return AcquirerOperationResponse.newBuilder()
                    .setOutcome(value)
                    .setReasonCode(value.name())
                    .setAcquirerRef("acq-1")
                    .build();
        }
    }
}

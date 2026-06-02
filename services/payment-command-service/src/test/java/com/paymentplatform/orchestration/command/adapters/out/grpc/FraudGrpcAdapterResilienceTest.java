package com.paymentplatform.orchestration.command.adapters.out.grpc;

import com.paymentplatform.orchestration.command.application.port.out.FraudCheckPort;
import com.paymentplatform.orchestration.command.domain.model.Money;
import com.paymentplatform.orchestration.command.infrastructure.resilience.GrpcResilienceProperties;
import com.paymentplatform.orchestration.command.infrastructure.resilience.ResilienceConfig;
import com.paymentplatform.orchestration.contracts.fraud.v1.FraudCheckRequest;
import com.paymentplatform.orchestration.contracts.fraud.v1.FraudCheckResponse;
import com.paymentplatform.orchestration.contracts.fraud.v1.FraudControlServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Currency;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FraudGrpcAdapterResilienceTest {

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
    void shouldFallbackWhenGrpcCallTimesOut() throws Exception {
        startServer(new FraudControlServiceGrpc.FraudControlServiceImplBase() {
            @Override
            public void evaluate(FraudCheckRequest request, StreamObserver<FraudCheckResponse> responseObserver) {
                awaitClientDeadline();
            }
        });

        FraudGrpcAdapter adapter = createAdapter(120, 1, 10, 10, 5, 2);
        FraudCheckPort.FraudCheckResult result = adapter.evaluate("p-1", "c-1", money("120.50"));

        assertFalse(result.approved());
        assertEquals("FRAUD_SERVICE_UNAVAILABLE", result.reasonCode());
    }

    @Test
    void shouldRetryAndRecoverAfterTransientFailure() throws Exception {
        AtomicInteger callCount = new AtomicInteger();
        startServer(new FraudControlServiceGrpc.FraudControlServiceImplBase() {
            @Override
            public void evaluate(FraudCheckRequest request, StreamObserver<FraudCheckResponse> responseObserver) {
                int current = callCount.incrementAndGet();
                if (current == 1) {
                    responseObserver.onError(new StatusRuntimeException(Status.UNAVAILABLE));
                    return;
                }
                responseObserver.onNext(FraudCheckResponse.newBuilder()
                        .setApproved(true)
                        .setReasonCode("APPROVED")
                        .setRiskScore(11)
                        .build());
                responseObserver.onCompleted();
            }
        });

        FraudGrpcAdapter adapter = createAdapter(500, 2, 20, 10, 5, 2);
        FraudCheckPort.FraudCheckResult result = adapter.evaluate("p-2", "c-2", money("90.00"));

        assertTrue(result.approved());
        assertEquals(2, callCount.get());
    }

    @Test
    void shouldOpenCircuitAfterRepeatedFailures() throws Exception {
        AtomicInteger callCount = new AtomicInteger();
        startServer(new FraudControlServiceGrpc.FraudControlServiceImplBase() {
            @Override
            public void evaluate(FraudCheckRequest request, StreamObserver<FraudCheckResponse> responseObserver) {
                callCount.incrementAndGet();
                responseObserver.onError(new StatusRuntimeException(Status.UNAVAILABLE));
            }
        });

        FraudGrpcAdapter adapter = createAdapter(500, 1, 10, 4, 4, 1);

        for (int i = 0; i < 8; i++) {
            FraudCheckPort.FraudCheckResult result = adapter.evaluate("p-3", "c-3", money("80.00"));
            assertFalse(result.approved());
        }

        assertTrue(callCount.get() < 8, "Circuit breaker should prevent some downstream calls.");
    }

    private void startServer(FraudControlServiceGrpc.FraudControlServiceImplBase service) throws Exception {
        String serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
                .addService(service)
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(serverName).build();
    }

    private void awaitClientDeadline() {
        try {
            boolean deadlinePassed = new CountDownLatch(1).await(2, TimeUnit.SECONDS);
            assertFalse(deadlinePassed);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private FraudGrpcAdapter createAdapter(
            int timeoutMs,
            int maxAttempts,
            long waitDurationMs,
            int slidingWindowSize,
            int minimumNumberOfCalls,
            int permittedHalfOpenCalls
    ) {
        GrpcResilienceProperties props = new GrpcResilienceProperties(
                timeoutMs,
                maxAttempts,
                waitDurationMs,
                50,
                slidingWindowSize,
                minimumNumberOfCalls,
                permittedHalfOpenCalls,
                10
        );
        ResilienceConfig config = new ResilienceConfig();
        return new FraudGrpcAdapter(
                FraudControlServiceGrpc.newBlockingStub(channel),
                props,
                config.circuitBreakerRegistry(props),
                config.retryRegistry(props),
                config.bulkheadRegistry(props)
        );
    }

    private Money money(String amount) {
        return new Money(new BigDecimal(amount), Currency.getInstance("EUR"));
    }
}

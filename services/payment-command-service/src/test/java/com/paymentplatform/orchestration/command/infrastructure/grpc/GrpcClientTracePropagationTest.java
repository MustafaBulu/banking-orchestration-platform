package com.paymentplatform.orchestration.command.infrastructure.grpc;

import com.paymentplatform.orchestration.contracts.fraud.v1.FraudCheckRequest;
import com.paymentplatform.orchestration.contracts.fraud.v1.FraudCheckResponse;
import com.paymentplatform.orchestration.contracts.fraud.v1.FraudControlServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        classes = GrpcClientTracePropagationTest.TestApplication.class,
        properties = {
                "management.tracing.enabled=true",
                "management.tracing.sampling.probability=1.0",
                "spring.autoconfigure.exclude="
                        + "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration,"
                        + "org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration"
        }
)
class GrpcClientTracePropagationTest {

    private static final Metadata.Key<String> TRACEPARENT =
            Metadata.Key.of("traceparent", Metadata.ASCII_STRING_MARSHALLER);

    private ManagedChannel channel;
    private Server server;

    @Autowired
    private GrpcClientTraceInterceptor traceInterceptor;

    @Autowired
    private Tracer tracer;

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
    void injectsCurrentTraceparentIntoGrpcMetadata() throws Exception {
        AtomicReference<String> observedTraceparent = new AtomicReference<>();
        startServer(new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call,
                    Metadata headers,
                    ServerCallHandler<ReqT, RespT> next
            ) {
                observedTraceparent.set(headers.get(TRACEPARENT));
                return next.startCall(call, headers);
            }
        });

        Span span = tracer.nextSpan().name("command-grpc-client").start();
        String expectedTraceId;
        try (Tracer.SpanInScope scope = tracer.withSpan(span)) {
            expectedTraceId = span.context().traceId();
            FraudControlServiceGrpc.newBlockingStub(channel)
                    .evaluate(FraudCheckRequest.newBuilder()
                            .setPaymentId("payment-1")
                            .setCustomerId("customer-1")
                            .setAmount("120.50")
                            .setCurrency("EUR")
                            .build());
        } finally {
            span.end();
        }

        assertThat(observedTraceparent.get())
                .matches("00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}")
                .contains(expectedTraceId);
    }

    private void startServer(ServerInterceptor captureInterceptor) throws Exception {
        String serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
                .intercept(captureInterceptor)
                .addService(new FraudControlServiceGrpc.FraudControlServiceImplBase() {
                    @Override
                    public void evaluate(
                            FraudCheckRequest request,
                            StreamObserver<FraudCheckResponse> responseObserver
                    ) {
                        responseObserver.onNext(FraudCheckResponse.newBuilder()
                                .setApproved(true)
                                .setReasonCode("APPROVED")
                                .setRiskScore(0)
                                .build());
                        responseObserver.onCompleted();
                    }
                })
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(serverName)
                .intercept(traceInterceptor)
                .build();
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @Import(GrpcClientTraceInterceptor.class)
    static class TestApplication {
    }
}

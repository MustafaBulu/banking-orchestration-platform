package com.paymentplatform.orchestration.limit.grpc;

import com.paymentplatform.orchestration.contracts.limit.v1.LimitControlServiceGrpc;
import com.paymentplatform.orchestration.contracts.limit.v1.LimitReserveRequest;
import com.paymentplatform.orchestration.contracts.limit.v1.LimitReserveResponse;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
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
        classes = LimitGrpcTracePropagationTest.TestApplication.class,
        properties = {
                "management.tracing.enabled=true",
                "management.tracing.sampling.probability=1.0",
                "spring.autoconfigure.exclude="
                        + "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration,"
                        + "org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration"
        }
)
class LimitGrpcTracePropagationTest {

    private static final Metadata.Key<String> TRACEPARENT =
            Metadata.Key.of("traceparent", Metadata.ASCII_STRING_MARSHALLER);

    private ManagedChannel channel;
    private Server server;

    @Autowired
    private GrpcServerTraceInterceptor traceInterceptor;

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
    void continuesIncomingTraceparentOnLimitServerSpan() throws Exception {
        AtomicReference<String> observedTraceId = new AtomicReference<>();
        startServer(observedTraceId);

        String traceId = "4bf92f3577b34da6a3ce929d0e0e4736";
        Channel tracedChannel = attachTraceparent("00-" + traceId + "-00f067aa0ba902b7-01");

        LimitControlServiceGrpc.newBlockingStub(tracedChannel)
                .reserve(LimitReserveRequest.newBuilder()
                        .setPaymentId("payment-1")
                        .setCustomerId("customer-1")
                        .setAmount("120.50")
                        .setCurrency("EUR")
                        .build());

        assertThat(observedTraceId).hasValue(traceId);
    }

    private void startServer(AtomicReference<String> observedTraceId) throws Exception {
        String serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
                .intercept(traceInterceptor)
                .addService(new LimitControlServiceGrpc.LimitControlServiceImplBase() {
                    @Override
                    public void reserve(
                            LimitReserveRequest request,
                            StreamObserver<LimitReserveResponse> responseObserver
                    ) {
                        Span currentSpan = tracer.currentSpan();
                        if (currentSpan != null) {
                            observedTraceId.set(currentSpan.context().traceId());
                        }
                        responseObserver.onNext(LimitReserveResponse.newBuilder()
                                .setApproved(true)
                                .setReasonCode("APPROVED")
                                .setReservationId("reservation-1")
                                .build());
                        responseObserver.onCompleted();
                    }
                })
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(serverName).build();
    }

    private Channel attachTraceparent(String traceparent) {
        Metadata headers = new Metadata();
        headers.put(TRACEPARENT, traceparent);
        return ClientInterceptors.intercept(
                channel,
                MetadataUtils.newAttachHeadersInterceptor(headers)
        );
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @Import(GrpcServerTraceInterceptor.class)
    static class TestApplication {
    }
}

package com.paymentplatform.orchestration.command.infrastructure.grpc;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.Locale;

@Component
public class GrpcClientTraceInterceptor implements ClientInterceptor {

    private final ObjectProvider<Tracer> tracerProvider;
    private final ObjectProvider<Propagator> propagatorProvider;

    public GrpcClientTraceInterceptor(
            ObjectProvider<Tracer> tracerProvider,
            ObjectProvider<Propagator> propagatorProvider
    ) {
        this.tracerProvider = tracerProvider;
        this.propagatorProvider = propagatorProvider;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method,
            CallOptions callOptions,
            Channel next
    ) {
        ClientCall<ReqT, RespT> call = next.newCall(method, callOptions);
        return new ForwardingClientCall.SimpleForwardingClientCall<>(call) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                injectTraceContext(headers);
                super.start(responseListener, headers);
            }
        };
    }

    private void injectTraceContext(Metadata headers) {
        Tracer tracer = tracerProvider.getIfAvailable();
        Propagator propagator = propagatorProvider.getIfAvailable();
        if (tracer == null || propagator == null) {
            return;
        }
        Span currentSpan = tracer.currentSpan();
        if (currentSpan == null) {
            return;
        }
        propagator.inject(currentSpan.context(), headers, GrpcClientTraceInterceptor::setHeader);
    }

    private static void setHeader(Metadata metadata, String name, String value) {
        if (name == null || name.isBlank() || value == null || value.isBlank()) {
            return;
        }
        Metadata.Key<String> key = Metadata.Key.of(
                name.toLowerCase(Locale.ROOT),
                Metadata.ASCII_STRING_MARSHALLER
        );
        metadata.removeAll(key);
        metadata.put(key, value);
    }
}

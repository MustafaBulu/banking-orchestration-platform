package com.paymentplatform.orchestration.fraud.grpc;

import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class GrpcServerTraceInterceptor implements ServerInterceptor {

    private final ObjectProvider<Tracer> tracerProvider;
    private final ObjectProvider<Propagator> propagatorProvider;

    public GrpcServerTraceInterceptor(
            ObjectProvider<Tracer> tracerProvider,
            ObjectProvider<Propagator> propagatorProvider
    ) {
        this.tracerProvider = tracerProvider;
        this.propagatorProvider = propagatorProvider;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next
    ) {
        Tracer tracer = tracerProvider.getIfAvailable();
        Propagator propagator = propagatorProvider.getIfAvailable();
        if (tracer == null || propagator == null) {
            return next.startCall(call, headers);
        }

        Span span = propagator.extract(headers, GrpcServerTraceInterceptor::getHeader)
                .name(call.getMethodDescriptor().getFullMethodName())
                .kind(Span.Kind.SERVER)
                .start();

        ServerCall.Listener<ReqT> listener;
        try (Tracer.SpanInScope scope = tracer.withSpan(span)) {
            listener = next.startCall(call, headers);
        } catch (RuntimeException | Error ex) {
            span.error(ex);
            span.end();
            throw ex;
        }
        return new ScopedServerCallListener<>(listener, tracer, span);
    }

    private static String getHeader(Metadata metadata, String name) {
        if (name == null || name.isBlank()) {
            return null;
        }
        return metadata.get(Metadata.Key.of(
                name.toLowerCase(Locale.ROOT),
                Metadata.ASCII_STRING_MARSHALLER
        ));
    }

    private static final class ScopedServerCallListener<ReqT>
            extends ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT> {

        private final Tracer tracer;
        private final Span span;
        private final AtomicBoolean ended = new AtomicBoolean();

        private ScopedServerCallListener(ServerCall.Listener<ReqT> delegate, Tracer tracer, Span span) {
            super(delegate);
            this.tracer = tracer;
            this.span = span;
        }

        @Override
        public void onMessage(ReqT message) {
            withSpan(() -> super.onMessage(message));
        }

        @Override
        public void onHalfClose() {
            withSpan(() -> super.onHalfClose());
        }

        @Override
        public void onCancel() {
            withSpan(() -> {
                super.onCancel();
                endSpan();
            });
        }

        @Override
        public void onComplete() {
            withSpan(() -> {
                super.onComplete();
                endSpan();
            });
        }

        @Override
        public void onReady() {
            withSpan(() -> super.onReady());
        }

        private void withSpan(Runnable action) {
            try (Tracer.SpanInScope scope = tracer.withSpan(span)) {
                action.run();
            } catch (RuntimeException | Error ex) {
                span.error(ex);
                endSpan();
                throw ex;
            }
        }

        private void endSpan() {
            if (ended.compareAndSet(false, true)) {
                span.end();
            }
        }
    }
}

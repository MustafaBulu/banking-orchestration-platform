package com.paymentplatform.orchestration.command.infrastructure.tracing;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;

import java.util.regex.Pattern;

public final class Traceparent {

    public static final String HEADER_NAME = "traceparent";

    private static final Pattern W3C_TRACEPARENT =
            Pattern.compile("00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}");

    public static String fromCurrentSpan(Tracer tracer) {
        if (tracer == null) {
            return null;
        }
        Span span = tracer.currentSpan();
        if (span == null || span.isNoop()) {
            return null;
        }
        TraceContext context = span.context();
        String flags = Boolean.TRUE.equals(context.sampled()) ? "01" : "00";
        return "00-" + context.traceId() + "-" + context.spanId() + "-" + flags;
    }

    public static boolean isValid(String traceparent) {
        return traceparent != null && W3C_TRACEPARENT.matcher(traceparent).matches();
    }

    private Traceparent() {
    }
}

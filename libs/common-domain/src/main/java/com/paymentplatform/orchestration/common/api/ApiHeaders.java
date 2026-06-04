package com.paymentplatform.orchestration.common.api;

public final class ApiHeaders {

    private static final String IDEMPOTENCY = "Idempotency";

    public static final String IDEMPOTENCY_KEY = IDEMPOTENCY + "-Key";

    public static String idempotencyKey() {
        return IDEMPOTENCY_KEY;
    }

    private ApiHeaders() {
    }
}

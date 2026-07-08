package com.paymentplatform.orchestration.command.infrastructure.idempotency;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.function.Supplier;

@Service
public class IdempotencyService {

    private static final int MAX_KEY_LENGTH = 128;
    private static final int ACCEPTED_STATUS_CODE = 202;
    private static final Duration RETENTION = Duration.ofHours(24);

    private final IdempotencyStore store;
    private final ObjectMapper objectMapper;
    private final Counter idempotencyHitCounter;
    private final Counter idempotencyConflictCounter;

    public IdempotencyService(IdempotencyStore store, ObjectMapper objectMapper, MeterRegistry meterRegistry) {
        this.store = store;
        this.objectMapper = objectMapper;
        this.idempotencyHitCounter = Counter.builder("idempotency.hit")
                .description("Idempotency requests served from a stored response")
                .register(meterRegistry);
        this.idempotencyConflictCounter = Counter.builder("idempotency.conflict")
                .description("Idempotency key reuse with a different request body")
                .register(meterRegistry);
    }

    public <T> T execute(String idempotencyKey, Object request, Class<T> responseType, Supplier<T> action) {
        String normalizedKey = normalize(idempotencyKey);
        String requestHash = requestHash(request);

        IdempotencyStore.Claim claim = store.claim(normalizedKey, requestHash, Instant.now().plus(RETENTION));
        switch (claim.state()) {
            case REPLAY -> {
                idempotencyHitCounter.increment();
                return readResponse(claim.responseBody(), responseType);
            }
            case CONFLICT -> {
                idempotencyConflictCounter.increment();
                throw new IdempotencyConflictException("Idempotency-Key was already used with a different request body");
            }
            case IN_PROGRESS -> throw new IdempotencyInProgressException(
                    "A request with this Idempotency-Key is still being processed");
            case OWNED -> {
            }
        }

        T response;
        try {
            response = action.get();
        } catch (RuntimeException ex) {
            store.discard(normalizedKey);
            throw ex;
        }
        store.recordResponse(normalizedKey, writeResponse(response), ACCEPTED_STATUS_CODE);
        return response;
    }

    private String normalize(String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new InvalidIdempotencyKeyException("Idempotency-Key must not be blank");
        }
        String normalized = idempotencyKey.trim();
        if (normalized.length() > MAX_KEY_LENGTH) {
            throw new InvalidIdempotencyKeyException("Idempotency-Key must be at most 128 characters");
        }
        return normalized;
    }

    private String requestHash(Object request) {
        try {
            byte[] json = objectMapper.writeValueAsString(request).getBytes(StandardCharsets.UTF_8);
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(json);
            return HexFormat.of().formatHex(digest);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize idempotent request", ex);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 is not available", ex);
        }
    }

    private String writeResponse(Object response) {
        try {
            return objectMapper.writeValueAsString(response);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize idempotent response", ex);
        }
    }

    private <T> T readResponse(String responseBody, Class<T> responseType) {
        try {
            return objectMapper.readValue(responseBody, responseType);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to deserialize idempotent response", ex);
        }
    }
}

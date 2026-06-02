package com.paymentplatform.orchestration.command.infrastructure.idempotency;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HexFormat;
import java.util.Optional;
import java.util.function.Supplier;

@Service
public class IdempotencyService {

    private static final int MAX_KEY_LENGTH = 128;
    private static final int ACCEPTED_STATUS_CODE = 202;

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public IdempotencyService(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public <T> T execute(String idempotencyKey, Object request, Class<T> responseType, Supplier<T> action) {
        String normalizedKey = normalize(idempotencyKey);
        String requestHash = requestHash(request);

        jdbcTemplate.update("""
                INSERT INTO idempotency_key (idempotency_key, request_hash, expires_at)
                VALUES (?, ?, ?)
                ON CONFLICT (idempotency_key) DO NOTHING
                """,
                normalizedKey,
                requestHash,
                Timestamp.from(Instant.now().plus(24, ChronoUnit.HOURS))
        );

        IdempotencyRecord idempotencyRecord = findForUpdate(normalizedKey)
                .orElseThrow(() -> new IllegalStateException("Idempotency key was not persisted"));

        if (!idempotencyRecord.requestHash().equals(requestHash)) {
            throw new IdempotencyConflictException("Idempotency-Key was already used with a different request body");
        }
        if (idempotencyRecord.responseBody() != null) {
            return readResponse(idempotencyRecord.responseBody(), responseType);
        }

        T response = action.get();
        jdbcTemplate.update("""
                UPDATE idempotency_key
                SET response_body = CAST(? AS jsonb),
                    status_code = ?
                WHERE idempotency_key = ?
                """,
                writeResponse(response),
                ACCEPTED_STATUS_CODE,
                normalizedKey
        );
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

    private Optional<IdempotencyRecord> findForUpdate(String idempotencyKey) {
        return jdbcTemplate.query("""
                SELECT request_hash, response_body::text
                FROM idempotency_key
                WHERE idempotency_key = ?
                FOR UPDATE
                """, rs -> {
            if (!rs.next()) {
                return Optional.empty();
            }
            return Optional.of(new IdempotencyRecord(
                    rs.getString("request_hash"),
                    rs.getString("response_body")
            ));
        }, idempotencyKey);
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

    private record IdempotencyRecord(String requestHash, String responseBody) {
    }
}

package com.paymentplatform.orchestration.command.infrastructure.idempotency;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;

@Component
public class IdempotencyStore {

    private final JdbcTemplate jdbcTemplate;

    public IdempotencyStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional
    public Claim claim(String idempotencyKey, String requestHash, Instant expiresAt) {
        int inserted = jdbcTemplate.update("""
                INSERT INTO idempotency_key (idempotency_key, request_hash, expires_at)
                VALUES (?, ?, ?)
                ON CONFLICT (idempotency_key) DO NOTHING
                """,
                idempotencyKey,
                requestHash,
                Timestamp.from(expiresAt)
        );
        if (inserted == 1) {
            return new Claim(State.OWNED, null);
        }
        return jdbcTemplate.query("""
                SELECT request_hash, response_body::text
                FROM idempotency_key
                WHERE idempotency_key = ?
                """, rs -> {
            if (!rs.next()) {
                return new Claim(State.IN_PROGRESS, null);
            }
            String existingHash = rs.getString("request_hash");
            String responseBody = rs.getString("response_body");
            if (!existingHash.equals(requestHash)) {
                return new Claim(State.CONFLICT, null);
            }
            if (responseBody != null) {
                return new Claim(State.REPLAY, responseBody);
            }
            return new Claim(State.IN_PROGRESS, null);
        }, idempotencyKey);
    }

    @Transactional
    public void recordResponse(String idempotencyKey, String responseBody, int statusCode) {
        jdbcTemplate.update("""
                UPDATE idempotency_key
                SET response_body = CAST(? AS jsonb),
                    status_code = ?
                WHERE idempotency_key = ?
                """,
                responseBody,
                statusCode,
                idempotencyKey
        );
    }

    @Transactional
    public void discard(String idempotencyKey) {
        jdbcTemplate.update("""
                DELETE FROM idempotency_key
                WHERE idempotency_key = ? AND response_body IS NULL
                """, idempotencyKey);
    }

    public enum State {
        OWNED,
        REPLAY,
        CONFLICT,
        IN_PROGRESS
    }

    public record Claim(State state, String responseBody) {
    }
}

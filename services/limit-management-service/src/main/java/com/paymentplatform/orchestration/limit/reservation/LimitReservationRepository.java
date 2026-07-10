package com.paymentplatform.orchestration.limit.reservation;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Repository
public class LimitReservationRepository {

    private static final String ACTIVE = "ACTIVE";
    private static final String RELEASED = "RELEASED";
    private static final String EXPIRED = "EXPIRED";

    private final JdbcTemplate jdbcTemplate;

    public LimitReservationRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional
    public String reserve(
            String paymentId,
            String customerId,
            BigDecimal amount,
            String currency,
            Instant expiresAt
    ) {
        expireExpiredReservations();
        Optional<String> existing = findActiveReservationIdByPaymentId(paymentId);
        if (existing.isPresent()) {
            return existing.get();
        }

        String reservationId = "resv-" + UUID.randomUUID();
        try {
            jdbcTemplate.update("""
                    INSERT INTO limit_reservation (
                        reservation_id, payment_id, customer_id, amount, currency, status, expires_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    reservationId,
                    paymentId,
                    customerId,
                    amount,
                    currency,
                    ACTIVE,
                    Timestamp.from(expiresAt)
            );
            return reservationId;
        } catch (DuplicateKeyException ex) {
            return findActiveReservationIdByPaymentId(paymentId)
                    .orElseThrow(() -> ex);
        }
    }

    @Transactional
    public boolean release(String reservationId) {
        int updated = jdbcTemplate.update("""
                UPDATE limit_reservation
                SET status = ?,
                    released_at = NOW(),
                    updated_at = NOW()
                WHERE reservation_id = ?
                  AND status = ?
                """, RELEASED, reservationId, ACTIVE);
        return updated == 1;
    }

    @Transactional
    public int expireExpiredReservations() {
        return jdbcTemplate.update("""
                UPDATE limit_reservation
                SET status = ?,
                    updated_at = NOW()
                WHERE status = ?
                  AND expires_at <= NOW()
                """, EXPIRED, ACTIVE);
    }

    public boolean hasActiveReservation(String reservationId) {
        String sql = """
                SELECT COUNT(*)
                FROM limit_reservation
                WHERE reservation_id = ?
                  AND status = ?
                  AND expires_at > NOW()
                """;
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, reservationId, ACTIVE);
        return count != null && count > 0;
    }

    public Optional<String> findActiveReservationIdByPaymentId(String paymentId) {
        String sql = """
                SELECT reservation_id
                FROM limit_reservation
                WHERE payment_id = ?
                  AND status = ?
                  AND expires_at > NOW()
                """;
        return jdbcTemplate.query(sql, rs -> {
            if (!rs.next()) {
                return Optional.empty();
            }
            return Optional.of(rs.getString("reservation_id"));
        }, paymentId, ACTIVE);
    }
}

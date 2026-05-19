package com.paymentplatform.orchestration.query.adapters.out.postgres;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;

@Repository
public class PaymentProjectionRepository {

    private final JdbcTemplate jdbcTemplate;

    public PaymentProjectionRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public boolean isProcessed(String eventId) {
        String sql = "SELECT COUNT(*) FROM processed_event WHERE event_id = ?";
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, eventId);
        return count != null && count > 0;
    }

    public void markProcessed(String eventId, String consumerName) {
        String sql = """
                INSERT INTO processed_event (event_id, consumer_name)
                VALUES (?, ?)
                ON CONFLICT (event_id) DO NOTHING
                """;
        jdbcTemplate.update(sql, eventId, consumerName);
    }

    public void upsertPaymentOverview(
            String paymentId,
            String customerId,
            BigDecimal amount,
            String currency,
            String status,
            Instant timestamp
    ) {
        String sql = """
                INSERT INTO payment_overview (
                    payment_id, customer_id, amount, currency, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (payment_id) DO UPDATE SET
                    customer_id = EXCLUDED.customer_id,
                    amount = EXCLUDED.amount,
                    currency = EXCLUDED.currency,
                    status = EXCLUDED.status,
                    updated_at = EXCLUDED.updated_at
                """;
        Timestamp ts = Timestamp.from(timestamp);
        jdbcTemplate.update(sql, paymentId, customerId, amount, currency, status, ts, ts);
    }

    public Optional<PaymentOverview> findByPaymentId(String paymentId) {
        String sql = """
                SELECT payment_id, customer_id, amount, currency, status, created_at, updated_at
                FROM payment_overview
                WHERE payment_id = ?
                """;
        return jdbcTemplate.query(sql, rs -> {
            if (!rs.next()) {
                return Optional.empty();
            }
            return Optional.of(new PaymentOverview(
                    rs.getString("payment_id"),
                    rs.getString("customer_id"),
                    rs.getBigDecimal("amount"),
                    rs.getString("currency"),
                    rs.getString("status"),
                    rs.getTimestamp("created_at").toInstant(),
                    rs.getTimestamp("updated_at").toInstant()
            ));
        }, paymentId);
    }
}

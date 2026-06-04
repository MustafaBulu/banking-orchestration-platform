package com.paymentplatform.orchestration.command.adapters.out.postgres;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public class OutboxJdbcRepository {

    private final JdbcTemplate jdbcTemplate;

    public OutboxJdbcRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<OutboxMessage> fetchBatchForUpdate(int batchSize) {
        String sql = """
                SELECT id, event_id, event_type, payload::text, retry_count, available_at
                FROM outbox
                WHERE status IN ('NEW', 'RETRY')
                  AND available_at <= NOW()
                ORDER BY created_at
                LIMIT ?
                FOR UPDATE SKIP LOCKED
                """;
        return jdbcTemplate.query(
                sql,
                (rs, rowNum) -> new OutboxMessage(
                        rs.getObject("id", UUID.class),
                        rs.getString("event_id"),
                        rs.getString("event_type"),
                        rs.getString("payload"),
                        rs.getInt("retry_count"),
                        rs.getTimestamp("available_at").toInstant()
                ),
                batchSize
        );
    }

    public void markPublished(UUID id) {
        String sql = """
                UPDATE outbox
                SET status = 'PUBLISHED',
                    published_at = NOW()
                WHERE id = ?
                """;
        jdbcTemplate.update(sql, id);
    }

    public void markRetry(UUID id, int retryCount, Duration backoff) {
        String sql = """
                UPDATE outbox
                SET status = 'RETRY',
                    retry_count = ?,
                    available_at = ?
                WHERE id = ?
                """;
        Instant nextTry = Instant.now().plus(backoff);
        jdbcTemplate.update(sql, retryCount, Timestamp.from(nextTry), id);
    }

    public void markFailed(UUID id, int retryCount) {
        String sql = """
                UPDATE outbox
                SET status = 'FAILED',
                    retry_count = ?
                WHERE id = ?
                """;
        jdbcTemplate.update(sql, retryCount, id);
    }

    public int countPending() {
        String sql = """
                SELECT COUNT(*)
                FROM outbox
                WHERE status IN ('NEW', 'RETRY')
                """;
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class);
        return count == null ? 0 : count;
    }
}

package com.paymentplatform.orchestration.notification.adapters.out.postgres;

import com.paymentplatform.orchestration.notification.application.port.out.NotificationRepository;
import com.paymentplatform.orchestration.notification.domain.model.NotificationRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;

@Repository
public class JdbcNotificationRepository implements NotificationRepository {

    private static final String INSERT_NOTIFICATION_SQL = """
            INSERT INTO notifications (
                notification_id, payment_id, customer_id, source_event_id,
                channel, status, message, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (source_event_id, channel) DO NOTHING
            """;

    private final JdbcTemplate jdbcTemplate;

    public JdbcNotificationRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public boolean isProcessed(String eventId) {
        String sql = "SELECT COUNT(*) FROM processed_notification_event WHERE event_id = ?";
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, eventId);
        return count != null && count > 0;
    }

    @Override
    public void save(NotificationRecord notificationRecord) {
        jdbcTemplate.update(
                INSERT_NOTIFICATION_SQL,
                notificationRecord.notificationId(),
                notificationRecord.paymentId(),
                notificationRecord.customerId(),
                notificationRecord.sourceEventId(),
                notificationRecord.channel().name(),
                notificationRecord.status().name(),
                notificationRecord.message(),
                Timestamp.from(notificationRecord.createdAt())
        );
    }

    @Override
    public void markProcessed(String eventId, String consumerName) {
        String sql = """
                INSERT INTO processed_notification_event (event_id, consumer_name)
                VALUES (?, ?)
                ON CONFLICT (event_id) DO NOTHING
                """;
        jdbcTemplate.update(sql, eventId, consumerName);
    }
}

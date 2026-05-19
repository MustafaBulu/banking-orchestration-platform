package com.paymentplatform.orchestration.command.adapters.out.postgres;

import com.paymentplatform.orchestration.command.application.port.out.OutboxPort;
import com.paymentplatform.orchestration.command.domain.event.PaymentCreatedEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

@Component
public class JdbcOutboxAdapter implements OutboxPort {

    private static final String INSERT_SQL = """
            INSERT INTO outbox (
                id, aggregate_id, aggregate_type, event_id, event_type, event_version, payload, status, retry_count, available_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, CAST(? AS jsonb), ?, ?, ?, ?)
            """;

    private final JdbcTemplate jdbcTemplate;

    public JdbcOutboxAdapter(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void enqueue(PaymentCreatedEvent event) {
        Instant now = Instant.now();
        String payloadJson = """
                {
                  "eventId":"%s",
                  "aggregateId":"%s",
                  "eventType":"%s",
                  "occurredAt":"%s",
                  "data":{
                    "customerId":"%s",
                    "amount":%s,
                    "currency":"%s"
                  }
                }
                """.formatted(
                event.eventId(),
                event.aggregateId(),
                event.eventType(),
                event.occurredAt(),
                event.customerId(),
                event.amount().toPlainString(),
                event.currency()
        );

        jdbcTemplate.update(
                INSERT_SQL,
                UUID.randomUUID(),
                event.aggregateId(),
                "Payment",
                event.eventId(),
                event.eventType(),
                1,
                payloadJson,
                "NEW",
                0,
                Timestamp.from(now),
                Timestamp.from(now)
        );
    }
}

package com.paymentplatform.orchestration.command.adapters.out.postgres;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentplatform.orchestration.command.application.port.out.OutboxPort;
import com.paymentplatform.orchestration.command.domain.event.PaymentCreatedEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
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
    private final ObjectMapper objectMapper;

    public JdbcOutboxAdapter(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    public void enqueue(PaymentCreatedEvent event) {
        Instant now = Instant.now();
        String payloadJson = toJson(new PaymentEventEnvelope(
                event.eventId(),
                event.aggregateId(),
                event.eventType(),
                event.occurredAt(),
                new PaymentCreatedPayload(event.customerId(), event.amount(), event.currency())
        ));

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

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize outbox payload", ex);
        }
    }

    private record PaymentEventEnvelope(
            String eventId,
            String aggregateId,
            String eventType,
            Instant occurredAt,
            PaymentCreatedPayload data
    ) {
    }

    private record PaymentCreatedPayload(String customerId, BigDecimal amount, String currency) {
    }
}

package com.paymentplatform.orchestration.command.adapters.out.postgres;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentplatform.orchestration.command.application.port.out.PaymentEventStorePort;
import com.paymentplatform.orchestration.command.domain.event.PaymentCreatedEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.sql.Timestamp;

@Component
public class JdbcPaymentEventStoreAdapter implements PaymentEventStorePort {

    private static final String INSERT_SQL = """
            INSERT INTO event_store (
                event_id, stream_id, stream_type, stream_version, event_type, event_version, payload, metadata, occurred_at
            ) VALUES (?, ?, ?, ?, ?, ?, CAST(? AS jsonb), CAST(? AS jsonb), ?)
            """;

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public JdbcPaymentEventStoreAdapter(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    public void append(PaymentCreatedEvent event) {
        jdbcTemplate.update(
                INSERT_SQL,
                event.eventId(),
                event.aggregateId(),
                "Payment",
                1L,
                event.eventType(),
                1,
                toJson(new EventPayload(event.customerId(), event.amount(), event.currency())),
                toJson(new EventMetadata(event.aggregateId())),
                Timestamp.from(event.occurredAt())
        );
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize payment event payload", ex);
        }
    }

    private record EventPayload(String customerId, BigDecimal amount, String currency) {
    }

    private record EventMetadata(String aggregateId) {
    }
}

package com.paymentplatform.orchestration.command.adapters.out.postgres;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentplatform.orchestration.command.application.port.out.PaymentEventStorePort;
import com.paymentplatform.orchestration.command.domain.event.PaymentAuthorizedEvent;
import com.paymentplatform.orchestration.command.domain.event.PaymentCapturedEvent;
import com.paymentplatform.orchestration.command.domain.event.PaymentCreatedEvent;
import com.paymentplatform.orchestration.command.domain.event.PaymentEvent;
import com.paymentplatform.orchestration.command.domain.event.PaymentRefundedEvent;
import com.paymentplatform.orchestration.command.domain.event.PaymentVoidedEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

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
    public void append(PaymentEvent event) {
        jdbcTemplate.update(
                INSERT_SQL,
                event.eventId(),
                event.aggregateId(),
                "Payment",
                nextStreamVersion(event.aggregateId()),
                event.eventType(),
                event.eventVersion(),
                toJson(new EventPayload(event.customerId(), event.amount(), event.currency())),
                toJson(new EventMetadata(event.aggregateId())),
                Timestamp.from(event.occurredAt())
        );
    }

    @Override
    public List<PaymentEvent> load(String paymentId) {
        String sql = """
                SELECT event_id, stream_id, event_type, payload::text, occurred_at
                FROM event_store
                WHERE stream_id = ?
                ORDER BY stream_version
                """;
        return jdbcTemplate.query(
                sql,
                (rs, rowNum) -> toEvent(
                        rs.getString("event_type"),
                        rs.getString("event_id"),
                        rs.getString("stream_id"),
                        rs.getTimestamp("occurred_at").toInstant(),
                        fromJson(rs.getString("payload"))
                ),
                paymentId
        );
    }

    private long nextStreamVersion(String paymentId) {
        Long currentVersion = jdbcTemplate.queryForObject(
                "SELECT COALESCE(MAX(stream_version), 0) FROM event_store WHERE stream_id = ?",
                Long.class,
                paymentId
        );
        return currentVersion == null ? 1L : currentVersion + 1L;
    }

    private PaymentEvent toEvent(
            String eventType,
            String eventId,
            String aggregateId,
            Instant occurredAt,
            EventPayload payload
    ) {
        return switch (eventType) {
            case "PaymentCreated" -> new PaymentCreatedEvent(
                    eventId, aggregateId, occurredAt, payload.customerId(), payload.amount(), payload.currency());
            case "PaymentAuthorized" -> new PaymentAuthorizedEvent(
                    eventId, aggregateId, occurredAt, payload.customerId(), payload.amount(), payload.currency());
            case "PaymentCaptured" -> new PaymentCapturedEvent(
                    eventId, aggregateId, occurredAt, payload.customerId(), payload.amount(), payload.currency());
            case "PaymentVoided" -> new PaymentVoidedEvent(
                    eventId, aggregateId, occurredAt, payload.customerId(), payload.amount(), payload.currency());
            case "PaymentRefunded" -> new PaymentRefundedEvent(
                    eventId, aggregateId, occurredAt, payload.customerId(), payload.amount(), payload.currency());
            default -> throw new IllegalStateException("Unsupported payment event type: " + eventType);
        };
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize payment event payload", ex);
        }
    }

    private EventPayload fromJson(String rawJson) {
        try {
            return objectMapper.readValue(rawJson, EventPayload.class);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to deserialize payment event payload", ex);
        }
    }

    private record EventPayload(String customerId, BigDecimal amount, String currency) {
    }

    private record EventMetadata(String aggregateId) {
    }
}

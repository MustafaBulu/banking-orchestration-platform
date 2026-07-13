package com.paymentplatform.orchestration.command.adapters.out.postgres;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentplatform.orchestration.command.application.port.out.OutboxPort;
import com.paymentplatform.orchestration.command.domain.event.PaymentEvent;
import com.paymentplatform.orchestration.events.payment.v1.PaymentAuthorizedEventEnvelope;
import com.paymentplatform.orchestration.events.payment.v1.PaymentCapturedEventEnvelope;
import com.paymentplatform.orchestration.command.infrastructure.tracing.Traceparent;
import com.paymentplatform.orchestration.events.payment.v1.PaymentCreatedEventEnvelope;
import com.paymentplatform.orchestration.events.payment.v1.PaymentCreatedPayload;
import com.paymentplatform.orchestration.events.payment.v1.PaymentLifecyclePayload;
import com.paymentplatform.orchestration.events.payment.v1.PaymentRefundedEventEnvelope;
import com.paymentplatform.orchestration.events.payment.v1.PaymentVoidedEventEnvelope;
import com.paymentplatform.orchestration.events.schema.EventSchemaRegistry;
import io.micrometer.tracing.Tracer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

@Component
public class JdbcOutboxAdapter implements OutboxPort {

    private static final String INSERT_SQL = """
            INSERT INTO outbox (
                id, aggregate_id, aggregate_type, event_id, event_type, event_version, payload, traceparent, status, retry_count, available_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, CAST(? AS jsonb), ?, ?, ?, ?, ?)
            """;

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final EventSchemaRegistry eventSchemaRegistry;
    private final ObjectProvider<Tracer> tracerProvider;

    public JdbcOutboxAdapter(
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            EventSchemaRegistry eventSchemaRegistry,
            ObjectProvider<Tracer> tracerProvider
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.eventSchemaRegistry = eventSchemaRegistry;
        this.tracerProvider = tracerProvider;
    }

    @Override
    public void enqueue(PaymentEvent event) {
        Instant now = Instant.now();
        String payloadJson = toJson(envelope(event));
        eventSchemaRegistry.validate(
                event.eventType(),
                event.eventVersion(),
                payloadJson
        );

        jdbcTemplate.update(
                INSERT_SQL,
                UUID.randomUUID(),
                event.aggregateId(),
                "Payment",
                event.eventId(),
                event.eventType(),
                event.eventVersion(),
                payloadJson,
                Traceparent.fromCurrentSpan(tracerProvider.getIfAvailable()),
                "NEW",
                0,
                Timestamp.from(now),
                Timestamp.from(now)
        );
    }

    private Object envelope(PaymentEvent event) {
        PaymentLifecyclePayload payload =
                new PaymentLifecyclePayload(event.customerId(), event.amount(), event.currency());
        return switch (event.eventType()) {
            case "PaymentCreated" -> new PaymentCreatedEventEnvelope(
                    event.eventId(),
                    event.aggregateId(),
                    event.occurredAt(),
                    new PaymentCreatedPayload(event.customerId(), event.amount(), event.currency())
            );
            case "PaymentAuthorized" -> new PaymentAuthorizedEventEnvelope(
                    event.eventId(), event.aggregateId(), event.occurredAt(), payload);
            case "PaymentCaptured" -> new PaymentCapturedEventEnvelope(
                    event.eventId(), event.aggregateId(), event.occurredAt(), payload);
            case "PaymentVoided" -> new PaymentVoidedEventEnvelope(
                    event.eventId(), event.aggregateId(), event.occurredAt(), payload);
            case "PaymentRefunded" -> new PaymentRefundedEventEnvelope(
                    event.eventId(), event.aggregateId(), event.occurredAt(), payload);
            default -> throw new IllegalStateException("Unsupported payment event type: " + event.eventType());
        };
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize outbox payload", ex);
        }
    }
}

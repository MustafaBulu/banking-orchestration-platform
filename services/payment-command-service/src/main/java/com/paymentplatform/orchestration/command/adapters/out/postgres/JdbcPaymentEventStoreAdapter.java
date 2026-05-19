package com.paymentplatform.orchestration.command.adapters.out.postgres;

import com.paymentplatform.orchestration.command.application.port.out.PaymentEventStorePort;
import com.paymentplatform.orchestration.command.domain.event.PaymentCreatedEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;

@Component
public class JdbcPaymentEventStoreAdapter implements PaymentEventStorePort {

    private static final String INSERT_SQL = """
            INSERT INTO event_store (
                event_id, stream_id, stream_type, stream_version, event_type, event_version, payload, metadata, occurred_at
            ) VALUES (?, ?, ?, ?, ?, ?, CAST(? AS jsonb), CAST(? AS jsonb), ?)
            """;

    private final JdbcTemplate jdbcTemplate;

    public JdbcPaymentEventStoreAdapter(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void append(PaymentCreatedEvent event) {
        String payloadJson = """
                {"customerId":"%s","amount":%s,"currency":"%s"}
                """.formatted(event.customerId(), event.amount().toPlainString(), event.currency());
        String metadataJson = """
                {"aggregateId":"%s"}
                """.formatted(event.aggregateId());

        jdbcTemplate.update(
                INSERT_SQL,
                event.eventId(),
                event.aggregateId(),
                "Payment",
                1L,
                event.eventType(),
                1,
                payloadJson,
                metadataJson,
                Timestamp.from(event.occurredAt())
        );
    }
}

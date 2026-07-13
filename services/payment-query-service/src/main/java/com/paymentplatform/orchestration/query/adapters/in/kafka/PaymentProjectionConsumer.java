package com.paymentplatform.orchestration.query.adapters.in.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentplatform.orchestration.events.schema.EventSchemaRegistry;
import com.paymentplatform.orchestration.query.adapters.out.postgres.PaymentProjectionRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Map;

@Component
public class PaymentProjectionConsumer {

    private static final String CONSUMER_NAME = "payment-query-service";
    private static final Map<String, String> STATUSES = Map.of(
            "PaymentCreated", "CREATED",
            "PaymentAuthorized", "AUTHORIZED",
            "PaymentCaptured", "CAPTURED",
            "PaymentVoided", "VOIDED",
            "PaymentRefunded", "REFUNDED"
    );

    private final ObjectMapper objectMapper;
    private final EventSchemaRegistry eventSchemaRegistry;
    private final PaymentProjectionRepository paymentProjectionRepository;

    public PaymentProjectionConsumer(
            ObjectMapper objectMapper,
            EventSchemaRegistry eventSchemaRegistry,
            PaymentProjectionRepository paymentProjectionRepository
    ) {
        this.objectMapper = objectMapper;
        this.eventSchemaRegistry = eventSchemaRegistry;
        this.paymentProjectionRepository = paymentProjectionRepository;
    }

    @KafkaListener(
            topics = "${app.payment-events.topic:payment.domain.events}",
            groupId = "${spring.kafka.consumer.group-id:payment-query-service}"
    )
    @Transactional
    public void consume(String rawMessage) throws Exception {
        eventSchemaRegistry.validate(rawMessage);
        JsonNode envelope = objectMapper.readTree(rawMessage);
        String eventId = envelope.path("eventId").asText(null);
        if (eventId == null || paymentProjectionRepository.isProcessed(eventId)) {
            return;
        }

        String eventType = envelope.path("eventType").asText(null);
        JsonNode data = envelope.path("data");
        String status = STATUSES.get(eventType);
        if (status != null && !data.isMissingNode()) {
            paymentProjectionRepository.upsertPaymentOverview(
                    envelope.path("aggregateId").asText(),
                    data.path("customerId").asText(),
                    data.path("amount").decimalValue(),
                    data.path("currency").asText(),
                    status,
                    occurredAt(envelope)
            );
        }

        paymentProjectionRepository.markProcessed(eventId, CONSUMER_NAME);
    }

    private Instant occurredAt(JsonNode envelope) {
        String occurredAt = envelope.path("occurredAt").asText(null);
        return occurredAt == null ? Instant.now() : Instant.parse(occurredAt);
    }
}

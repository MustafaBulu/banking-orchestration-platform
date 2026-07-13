package com.paymentplatform.orchestration.ledger.adapters.in.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentplatform.orchestration.events.schema.EventSchemaRegistry;
import com.paymentplatform.orchestration.ledger.application.service.PaymentLedgerEvent;
import com.paymentplatform.orchestration.ledger.application.service.PostLedgerEntriesService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Set;

@Component
public class PaymentLedgerConsumer {

    private static final Set<String> SUPPORTED_EVENT_TYPES = Set.of(
            "PaymentCreated",
            "PaymentAuthorized",
            "PaymentCaptured",
            "PaymentVoided",
            "PaymentRefunded"
    );

    private final ObjectMapper objectMapper;
    private final EventSchemaRegistry eventSchemaRegistry;
    private final PostLedgerEntriesService postLedgerEntriesService;

    public PaymentLedgerConsumer(
            ObjectMapper objectMapper,
            EventSchemaRegistry eventSchemaRegistry,
            PostLedgerEntriesService postLedgerEntriesService
    ) {
        this.objectMapper = objectMapper;
        this.eventSchemaRegistry = eventSchemaRegistry;
        this.postLedgerEntriesService = postLedgerEntriesService;
    }

    @KafkaListener(
            topics = "${app.payment-events.topic:payment.domain.events}",
            groupId = "${spring.kafka.consumer.group-id:ledger-service}"
    )
    public void consume(String rawMessage) throws JsonProcessingException {
        eventSchemaRegistry.validate(rawMessage);
        JsonNode envelope = objectMapper.readTree(rawMessage);
        String eventType = envelope.path("eventType").asText(null);
        JsonNode data = envelope.path("data");
        if (!SUPPORTED_EVENT_TYPES.contains(eventType)
                || envelope.path("eventId").isMissingNode()
                || data.isMissingNode()) {
            return;
        }

        postLedgerEntriesService.postPaymentEvent(new PaymentLedgerEvent(
                envelope.path("eventId").asText(),
                envelope.path("aggregateId").asText(),
                eventType,
                data.path("customerId").asText(),
                data.path("amount").decimalValue(),
                data.path("currency").asText(),
                occurredAt(envelope)
        ));
    }

    private Instant occurredAt(JsonNode envelope) {
        String occurredAt = envelope.path("occurredAt").asText(null);
        return occurredAt == null ? Instant.now() : Instant.parse(occurredAt);
    }
}

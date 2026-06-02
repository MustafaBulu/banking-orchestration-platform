package com.paymentplatform.orchestration.ledger.adapters.in.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentplatform.orchestration.ledger.application.service.PaymentCreatedLedgerEvent;
import com.paymentplatform.orchestration.ledger.application.service.PostLedgerEntriesService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class PaymentLedgerConsumer {

    private static final String PAYMENT_CREATED = "PaymentCreated";

    private final ObjectMapper objectMapper;
    private final PostLedgerEntriesService postLedgerEntriesService;

    public PaymentLedgerConsumer(
            ObjectMapper objectMapper,
            PostLedgerEntriesService postLedgerEntriesService
    ) {
        this.objectMapper = objectMapper;
        this.postLedgerEntriesService = postLedgerEntriesService;
    }

    @KafkaListener(
            topics = "${app.payment-events.topic:payment.domain.events}",
            groupId = "${spring.kafka.consumer.group-id:ledger-service}"
    )
    public void consume(String rawMessage) throws JsonProcessingException {
        PaymentEventEnvelope envelope = objectMapper.readValue(rawMessage, PaymentEventEnvelope.class);
        if (!PAYMENT_CREATED.equals(envelope.eventType()) || envelope.eventId() == null || envelope.data() == null) {
            return;
        }

        postLedgerEntriesService.postPaymentCreated(new PaymentCreatedLedgerEvent(
                envelope.eventId(),
                envelope.aggregateId(),
                envelope.data().customerId(),
                envelope.data().amount(),
                envelope.data().currency(),
                envelope.occurredAt() == null ? Instant.now() : envelope.occurredAt()
        ));
    }
}

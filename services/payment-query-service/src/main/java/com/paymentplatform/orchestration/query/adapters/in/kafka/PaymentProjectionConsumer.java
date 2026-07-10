package com.paymentplatform.orchestration.query.adapters.in.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentplatform.orchestration.events.payment.v1.PaymentCreatedEventEnvelope;
import com.paymentplatform.orchestration.events.schema.EventSchemaRegistry;
import com.paymentplatform.orchestration.query.adapters.out.postgres.PaymentProjectionRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Component
public class PaymentProjectionConsumer {

    private static final String CONSUMER_NAME = "payment-query-service";

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
        PaymentCreatedEventEnvelope envelope = objectMapper.readValue(rawMessage, PaymentCreatedEventEnvelope.class);
        if (envelope.eventId() == null || paymentProjectionRepository.isProcessed(envelope.eventId())) {
            return;
        }

        if (PaymentCreatedEventEnvelope.EVENT_TYPE.equals(envelope.eventType()) && envelope.data() != null) {
            paymentProjectionRepository.upsertPaymentOverview(
                    envelope.aggregateId(),
                    envelope.data().customerId(),
                    envelope.data().amount(),
                    envelope.data().currency(),
                    "CREATED",
                    envelope.occurredAt() == null ? Instant.now() : envelope.occurredAt()
            );
        }

        paymentProjectionRepository.markProcessed(envelope.eventId(), CONSUMER_NAME);
    }
}

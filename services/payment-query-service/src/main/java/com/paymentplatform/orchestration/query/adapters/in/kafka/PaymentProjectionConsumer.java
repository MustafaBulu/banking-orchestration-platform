package com.paymentplatform.orchestration.query.adapters.in.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentplatform.orchestration.query.adapters.out.postgres.PaymentProjectionRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Component
public class PaymentProjectionConsumer {

    private static final String CONSUMER_NAME = "payment-query-service";

    private final ObjectMapper objectMapper;
    private final PaymentProjectionRepository paymentProjectionRepository;

    public PaymentProjectionConsumer(
            ObjectMapper objectMapper,
            PaymentProjectionRepository paymentProjectionRepository
    ) {
        this.objectMapper = objectMapper;
        this.paymentProjectionRepository = paymentProjectionRepository;
    }

    @KafkaListener(
            topics = "${app.payment-events.topic:payment.domain.events}",
            groupId = "${spring.kafka.consumer.group-id:payment-query-service}"
    )
    @Transactional
    public void consume(String rawMessage) throws Exception {
        PaymentEventEnvelope envelope = objectMapper.readValue(rawMessage, PaymentEventEnvelope.class);
        if (envelope.eventId() == null || paymentProjectionRepository.isProcessed(envelope.eventId())) {
            return;
        }

        if ("PaymentCreated".equals(envelope.eventType()) && envelope.data() != null) {
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

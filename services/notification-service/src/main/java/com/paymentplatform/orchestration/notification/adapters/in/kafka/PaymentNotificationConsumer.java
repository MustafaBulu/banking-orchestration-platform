package com.paymentplatform.orchestration.notification.adapters.in.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentplatform.orchestration.events.payment.v1.PaymentCreatedEventEnvelope;
import com.paymentplatform.orchestration.events.schema.EventSchemaRegistry;
import com.paymentplatform.orchestration.notification.application.service.PaymentCreatedNotificationEvent;
import com.paymentplatform.orchestration.notification.application.service.RecordNotificationService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class PaymentNotificationConsumer {

    private final ObjectMapper objectMapper;
    private final EventSchemaRegistry eventSchemaRegistry;
    private final RecordNotificationService recordNotificationService;

    public PaymentNotificationConsumer(
            ObjectMapper objectMapper,
            EventSchemaRegistry eventSchemaRegistry,
            RecordNotificationService recordNotificationService
    ) {
        this.objectMapper = objectMapper;
        this.eventSchemaRegistry = eventSchemaRegistry;
        this.recordNotificationService = recordNotificationService;
    }

    @KafkaListener(
            topics = "${app.payment-events.topic:payment.domain.events}",
            groupId = "${spring.kafka.consumer.group-id:notification-service}"
    )
    public void consume(String rawMessage) throws JsonProcessingException {
        eventSchemaRegistry.validate(rawMessage);
        PaymentCreatedEventEnvelope envelope = objectMapper.readValue(rawMessage, PaymentCreatedEventEnvelope.class);
        if (!PaymentCreatedEventEnvelope.EVENT_TYPE.equals(envelope.eventType())
                || envelope.eventId() == null
                || envelope.data() == null) {
            return;
        }

        recordNotificationService.recordPaymentCreated(new PaymentCreatedNotificationEvent(
                envelope.eventId(),
                envelope.aggregateId(),
                envelope.data().customerId(),
                envelope.occurredAt() == null ? Instant.now() : envelope.occurredAt()
        ));
    }
}

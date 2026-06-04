package com.paymentplatform.orchestration.notification.application.service;

import com.paymentplatform.orchestration.notification.application.port.out.NotificationRepository;
import com.paymentplatform.orchestration.notification.domain.model.NotificationChannel;
import com.paymentplatform.orchestration.notification.domain.model.NotificationRecord;
import com.paymentplatform.orchestration.notification.domain.model.NotificationStatus;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecordNotificationServiceTest {

    private static final PaymentCreatedNotificationEvent PAYMENT_CREATED_EVENT = new PaymentCreatedNotificationEvent(
            "event-1",
            "payment-1",
            "customer-1",
            Instant.parse("2026-06-04T10:15:30Z")
    );

    @Test
    void shouldRecordNotificationForPaymentCreatedEvent() {
        FakeNotificationRepository repository = new FakeNotificationRepository();
        RecordNotificationService service = new RecordNotificationService(repository, new SimpleMeterRegistry());

        service.recordPaymentCreated(PAYMENT_CREATED_EVENT);

        assertEquals(1, repository.notifications.size());
        NotificationRecord notificationRecord = repository.notifications.getFirst();
        assertEquals(PAYMENT_CREATED_EVENT.paymentId(), notificationRecord.paymentId());
        assertEquals(PAYMENT_CREATED_EVENT.customerId(), notificationRecord.customerId());
        assertEquals(PAYMENT_CREATED_EVENT.eventId(), notificationRecord.sourceEventId());
        assertEquals(NotificationChannel.EMAIL, notificationRecord.channel());
        assertEquals(NotificationStatus.RECORDED, notificationRecord.status());
        assertTrue(repository.processedEventIds.contains(PAYMENT_CREATED_EVENT.eventId()));
    }

    @Test
    void shouldNotRecordDuplicateNotificationForAlreadyProcessedEvent() {
        FakeNotificationRepository repository = new FakeNotificationRepository();
        RecordNotificationService service = new RecordNotificationService(repository, new SimpleMeterRegistry());

        service.recordPaymentCreated(PAYMENT_CREATED_EVENT);
        service.recordPaymentCreated(PAYMENT_CREATED_EVENT);

        assertEquals(1, repository.notifications.size());
    }

    private static class FakeNotificationRepository implements NotificationRepository {

        private final List<NotificationRecord> notifications = new ArrayList<>();
        private final Set<String> processedEventIds = new HashSet<>();

        @Override
        public boolean isProcessed(String eventId) {
            return processedEventIds.contains(eventId);
        }

        @Override
        public void save(NotificationRecord notificationRecord) {
            notifications.add(notificationRecord);
        }

        @Override
        public void markProcessed(String eventId, String consumerName) {
            processedEventIds.add(eventId);
        }
    }
}

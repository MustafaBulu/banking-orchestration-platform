package com.paymentplatform.orchestration.notification.application.service;

import com.paymentplatform.orchestration.notification.application.port.out.NotificationRepository;
import com.paymentplatform.orchestration.notification.domain.model.NotificationRecord;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class RecordNotificationService {

    private static final String CONSUMER_NAME = "notification-service";

    private final NotificationRepository notificationRepository;
    private final Counter notificationsRecordedCounter;

    public RecordNotificationService(NotificationRepository notificationRepository, MeterRegistry meterRegistry) {
        this.notificationRepository = notificationRepository;
        this.notificationsRecordedCounter = Counter.builder("notifications.recorded")
                .description("Notification records created from payment events")
                .register(meterRegistry);
    }

    @Transactional
    public void recordPaymentCreated(PaymentCreatedNotificationEvent event) {
        if (notificationRepository.isProcessed(event.eventId())) {
            return;
        }

        NotificationRecord notificationRecord = NotificationRecord.paymentCreated(
                event.eventId(),
                event.paymentId(),
                event.customerId(),
                event.occurredAt()
        );

        notificationRepository.save(notificationRecord);
        notificationRepository.markProcessed(event.eventId(), CONSUMER_NAME);
        notificationsRecordedCounter.increment();
    }
}

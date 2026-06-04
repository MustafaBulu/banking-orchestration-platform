package com.paymentplatform.orchestration.notification.application.port.out;

import com.paymentplatform.orchestration.notification.domain.model.NotificationRecord;

public interface NotificationRepository {

    boolean isProcessed(String eventId);

    void save(NotificationRecord notificationRecord);

    void markProcessed(String eventId, String consumerName);
}

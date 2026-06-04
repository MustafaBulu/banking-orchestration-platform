package com.paymentplatform.orchestration.notification.application.service;

import java.time.Instant;

public record PaymentCreatedNotificationEvent(
        String eventId,
        String paymentId,
        String customerId,
        Instant occurredAt
) {
}

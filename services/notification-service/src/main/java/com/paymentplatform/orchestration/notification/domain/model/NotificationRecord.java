package com.paymentplatform.orchestration.notification.domain.model;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record NotificationRecord(
        UUID notificationId,
        String paymentId,
        String customerId,
        String sourceEventId,
        NotificationChannel channel,
        NotificationStatus status,
        String message,
        Instant createdAt
) {

    public NotificationRecord {
        Objects.requireNonNull(notificationId, "notificationId is required");
        Objects.requireNonNull(paymentId, "paymentId is required");
        Objects.requireNonNull(customerId, "customerId is required");
        Objects.requireNonNull(sourceEventId, "sourceEventId is required");
        Objects.requireNonNull(channel, "channel is required");
        Objects.requireNonNull(status, "status is required");
        Objects.requireNonNull(message, "message is required");
        Objects.requireNonNull(createdAt, "createdAt is required");
    }

    public static NotificationRecord paymentCreated(
            String eventId,
            String paymentId,
            String customerId,
            Instant occurredAt
    ) {
        return new NotificationRecord(
                UUID.randomUUID(),
                paymentId,
                customerId,
                eventId,
                NotificationChannel.EMAIL,
                NotificationStatus.RECORDED,
                "Payment %s was created.".formatted(paymentId),
                occurredAt
        );
    }
}

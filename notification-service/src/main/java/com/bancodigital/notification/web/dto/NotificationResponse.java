package com.bancodigital.notification.web.dto;

import com.bancodigital.notification.domain.Notification;
import com.bancodigital.notification.domain.NotificationChannel;
import com.bancodigital.notification.domain.NotificationStatus;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record NotificationResponse(
        String id,
        UUID transferId,
        NotificationChannel channel,
        NotificationStatus status,
        String title,
        String body,
        Map<String, Object> metadata,
        Instant createdAt
) {

    public static NotificationResponse from(Notification notification) {
        return new NotificationResponse(
                notification.getId(),
                notification.getTransferId(),
                notification.getChannel(),
                notification.getStatus(),
                notification.getTitle(),
                notification.getBody(),
                notification.getMetadata(),
                notification.getCreatedAt()
        );
    }
}

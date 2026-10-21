package com.bancodigital.notification.web.dto;

import com.bancodigital.notification.domain.Notification;
import com.bancodigital.notification.domain.NotificationChannel;
import com.bancodigital.notification.domain.NotificationStatus;
import io.swagger.v3.oas.annotations.media.Schema;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Schema(description = "Notificação de uma transferência.")
public record NotificationResponse(
        @Schema(description = "Identificador da notificação.") String id,
        @Schema(description = "Transferência que originou a notificação.") UUID transferId,
        @Schema(description = "Canal de entrega.") NotificationChannel channel,
        @Schema(description = "Situação da notificação.") NotificationStatus status,
        @Schema(description = "Título.", example = "Transferência recebida") String title,
        @Schema(description = "Texto da notificação.", example = "Você recebeu um valor.") String body,
        @Schema(description = "Dados adicionais (valor, contas, motivo da falha...).") Map<String, Object> metadata,
        @Schema(description = "Momento da notificação (UTC).", example = "2026-10-21T14:05:00Z") Instant createdAt
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

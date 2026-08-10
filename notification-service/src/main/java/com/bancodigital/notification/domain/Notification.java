package com.bancodigital.notification.domain;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;

/*
 * Cada canal (EMAIL, PUSH, SMS) tem campos específicos completamente diferentes — é por isso que
 * este é um documento MongoDB e não uma tabela relacional (ver README, seção sobre a escolha do
 * MongoDB no notification-service). metadata carrega o que é específico do canal (ex.: PUSH:
 * icon/deepLink; EMAIL: contentType); title/body são comuns a todos.
 *
 * A consulta principal do serviço é sempre por accountId ordenado por data — o índice composto
 * abaixo cobre exatamente esse acesso.
 */
@Document(collection = "notifications")
@CompoundIndex(name = "accountId_createdAt", def = "{'accountId': 1, 'createdAt': -1}")
public class Notification {

    @Id
    private String id;

    private UUID accountId;
    private UUID transferId;
    private NotificationChannel channel;
    private NotificationStatus status;
    private String title;
    private String body;
    private Map<String, Object> metadata;
    private Instant createdAt;

    protected Notification() {
    }

    private Notification(UUID accountId, UUID transferId, NotificationChannel channel, String title, String body,
                          Map<String, Object> metadata) {
        this.accountId = accountId;
        this.transferId = transferId;
        this.channel = channel;
        this.status = NotificationStatus.SENT;
        this.title = title;
        this.body = body;
        this.metadata = metadata;
        this.createdAt = Instant.now();
    }

    public static Notification email(UUID accountId, UUID transferId, String title, String body) {
        return new Notification(accountId, transferId, NotificationChannel.EMAIL, title, body,
                Map.of("contentType", "text/html"));
    }

    public static Notification push(UUID accountId, UUID transferId, String title, String body, String icon,
                                     String deepLink) {
        return new Notification(accountId, transferId, NotificationChannel.PUSH, title, body,
                Map.of("icon", icon, "deepLink", deepLink));
    }

    public static Notification sms(UUID accountId, UUID transferId, String body) {
        return new Notification(accountId, transferId, NotificationChannel.SMS, null, body, Map.of());
    }

    public String getId() {
        return id;
    }

    public UUID getAccountId() {
        return accountId;
    }

    public UUID getTransferId() {
        return transferId;
    }

    public NotificationChannel getChannel() {
        return channel;
    }

    public NotificationStatus getStatus() {
        return status;
    }

    public String getTitle() {
        return title;
    }

    public String getBody() {
        return body;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}

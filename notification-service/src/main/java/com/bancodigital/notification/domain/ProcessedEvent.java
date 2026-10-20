package com.bancodigital.notification.domain;

import java.time.Instant;
import java.util.UUID;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Deduplicação (fonte de verdade da idempotência): um evento com este eventId já foi processado. Complementa o
 * cache do Redis; é o equivalente em MongoDB do ProcessedEvent JPA dos outros serviços.
 */
@Document(collection = "processed_events")
public class ProcessedEvent {

    @Id
    private UUID eventId;

    /** TTL de 14 dias, acima da retenção de 7 dias do Kafka (mesma janela do RetentionCleanupScheduler dos outros serviços). */
    @Indexed(expireAfter = "14d")
    private Instant processedAt;

    protected ProcessedEvent() {
    }

    private ProcessedEvent(UUID eventId, Instant processedAt) {
        this.eventId = eventId;
        this.processedAt = processedAt;
    }

    public static ProcessedEvent agora(UUID eventId) {
        return new ProcessedEvent(eventId, Instant.now());
    }

    public UUID getEventId() {
        return eventId;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }
}

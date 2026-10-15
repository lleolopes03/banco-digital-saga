package com.bancodigital.notification.domain;

import java.time.Instant;
import java.util.UUID;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Registro de deduplicação (fonte de verdade da idempotência): um evento com este eventId já foi
 * processado por este serviço. Complementa o cache rápido no Redis — se o Redis for limpo, esta
 * collection ainda impede reprocessamento. Mesmo papel do ProcessedEvent JPA/Postgres usado nos
 * outros serviços, adaptado para MongoDB aqui porque este serviço não tem banco relacional.
 */
@Document(collection = "processed_events")
public class ProcessedEvent {

    @Id
    private UUID eventId;

    /*
     * Indice TTL: o MongoDB apaga o registro sozinho 14 dias depois. Mesma janela de retencao
     * dos processed_events dos servicos Postgres (RetentionCleanupScheduler): precisa cobrir o
     * periodo em que o Kafka ainda pode reentregar a mensagem (retencao padrao do topico: 7 dias).
     * O monitor de TTL do MongoDB roda a cada ~60s.
     */
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

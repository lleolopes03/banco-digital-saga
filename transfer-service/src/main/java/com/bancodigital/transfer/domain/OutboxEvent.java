package com.bancodigital.transfer.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Entity
@Table(name = "outbox_events")
public class OutboxEvent {

    @Id
    private UUID id;

    @Column(name = "aggregate_type", nullable = false)
    private String aggregateType;

    @Column(name = "aggregate_id", nullable = false)
    private UUID aggregateId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", nullable = false, columnDefinition = "jsonb")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private OutboxEventStatus status;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    /*
     * Traceparent (W3C) capturado do contexto de trace ATIVO no momento em que este evento foi
     * criado (dentro do método de negócio, nunca no OutboxPublisherScheduler) — é isso que
     * permite ao publisher, rodando minutos depois numa thread de scheduling sem nenhuma relação
     * com a transação original, colocar o header correto na mensagem Kafka e manter a saga
     * inteira como um único trace mesmo atravessando a fronteira assíncrona do Outbox. Nullable
     * porque eventos publicados antes desta coluna existir (ou em cenários de teste que não
     * configuram tracing) simplesmente não propagam contexto — o consumer trata isso como início
     * de um trace novo, nunca como erro.
     */
    @Column(name = "trace_context")
    private String traceContext;

    protected OutboxEvent() {
    }

    private OutboxEvent(UUID id, String aggregateType, UUID aggregateId, String eventType, String payload,
                         OutboxEventStatus status, Instant createdAt, Instant publishedAt, String traceContext) {
        this.id = id;
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.payload = payload;
        this.status = status;
        this.createdAt = createdAt;
        this.publishedAt = publishedAt;
        this.traceContext = traceContext;
    }

    public static OutboxEvent pending(UUID id, String aggregateType, UUID aggregateId, String eventType,
                                       String payload, String traceContext) {
        return new OutboxEvent(id, aggregateType, aggregateId, eventType, payload, OutboxEventStatus.PENDING,
                Instant.now(), null, traceContext);
    }

    public void markPublished() {
        this.status = OutboxEventStatus.PUBLISHED;
        this.publishedAt = Instant.now();
    }

    public UUID getId() {
        return id;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public UUID getAggregateId() {
        return aggregateId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getPayload() {
        return payload;
    }

    public OutboxEventStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }

    public String getTraceContext() {
        return traceContext;
    }
}

package com.bancodigital.antifraud.application;

import com.bancodigital.antifraud.domain.OutboxEvent;
import com.bancodigital.antifraud.infrastructure.observability.SagaObservability;
import com.bancodigital.antifraud.infrastructure.persistence.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * Grava um evento de saída no Outbox, na transação corrente do chamador (nenhum código de negócio
 * publica direto no Kafka; o OutboxPublisherScheduler publica depois).
 *
 * O tipo do evento é o nome simples da classe do payload (record em {@code domain.event}), então não
 * há string de tipo escrita à mão para divergir da classe. O traceparent do trace ATIVO neste momento
 * (o span do consumer que disparou o método de negócio) é gravado junto, permitindo ao scheduler —
 * numa thread sem relação com esta chamada — propagar o mesmo trace ao transfer-service via Kafka.
 */
@Component
public class OutboxEventWriter {

    private static final String AGGREGATE_TYPE_TRANSFER = "Transfer";
    private static final ObjectMapper PAYLOAD_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    private final OutboxEventRepository outboxEventRepository;
    private final SagaObservability sagaObservability;

    public OutboxEventWriter(OutboxEventRepository outboxEventRepository, SagaObservability sagaObservability) {
        this.outboxEventRepository = outboxEventRepository;
        this.sagaObservability = sagaObservability;
    }

    public void gravarParaTransferencia(UUID transferId, Object payload) {
        outboxEventRepository.save(OutboxEvent.pending(UUID.randomUUID(), AGGREGATE_TYPE_TRANSFER, transferId,
                payload.getClass().getSimpleName(), serializar(payload), sagaObservability.traceparentAtual()));
    }

    private static String serializar(Object payload) {
        try {
            return PAYLOAD_MAPPER.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Falha ao serializar payload do outbox event", e);
        }
    }
}

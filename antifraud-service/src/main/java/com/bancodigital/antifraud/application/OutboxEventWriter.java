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
 * Grava um evento de saída no Outbox, na transação do chamador (o OutboxPublisherScheduler publica depois).
 * O eventType é o nome simples da classe do payload; o traceparent ativo é gravado junto para o scheduler
 * propagar o mesmo trace ao próximo hop.
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

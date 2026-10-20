package com.bancodigital.account.application;

import com.bancodigital.account.domain.OutboxEvent;
import com.bancodigital.account.infrastructure.observability.SagaObservability;
import com.bancodigital.account.infrastructure.persistence.OutboxEventRepository;
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

    private static final String AGGREGATE_ACCOUNT = "Account";
    private static final String AGGREGATE_TRANSFER = "Transfer";
    private static final ObjectMapper PAYLOAD_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    private final OutboxEventRepository outboxEventRepository;
    private final SagaObservability sagaObservability;

    public OutboxEventWriter(OutboxEventRepository outboxEventRepository, SagaObservability sagaObservability) {
        this.outboxEventRepository = outboxEventRepository;
        this.sagaObservability = sagaObservability;
    }

    public void gravarParaConta(UUID accountId, Object payload) {
        gravar(AGGREGATE_ACCOUNT, accountId, payload);
    }

    public void gravarParaTransferencia(UUID transferId, Object payload) {
        gravar(AGGREGATE_TRANSFER, transferId, payload);
    }

    private void gravar(String aggregateType, UUID aggregateId, Object payload) {
        OutboxEvent event = OutboxEvent.pending(UUID.randomUUID(), aggregateType, aggregateId,
                payload.getClass().getSimpleName(), serializar(payload), sagaObservability.traceparentAtual());
        outboxEventRepository.save(event);
    }

    private static String serializar(Object payload) {
        try {
            return PAYLOAD_MAPPER.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Falha ao serializar payload do outbox event", e);
        }
    }
}

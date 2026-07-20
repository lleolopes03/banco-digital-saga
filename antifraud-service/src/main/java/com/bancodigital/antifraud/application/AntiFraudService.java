package com.bancodigital.antifraud.application;

import com.bancodigital.antifraud.domain.AntiFraudCheck;
import com.bancodigital.antifraud.domain.OutboxEvent;
import com.bancodigital.antifraud.domain.event.AntiFraudApproved;
import com.bancodigital.antifraud.domain.event.AntiFraudRejected;
import com.bancodigital.antifraud.domain.event.TransferReserved;
import com.bancodigital.antifraud.infrastructure.observability.SagaObservability;
import com.bancodigital.antifraud.infrastructure.persistence.AntiFraudCheckRepository;
import com.bancodigital.antifraud.infrastructure.persistence.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class AntiFraudService {

    private static final String AGGREGATE_TYPE_TRANSFER = "Transfer";
    private static final ObjectMapper OUTBOX_PAYLOAD_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    private final AntiFraudRuleEngine ruleEngine;
    private final AntiFraudCheckRepository antiFraudCheckRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final SagaObservability sagaObservability;

    public AntiFraudService(AntiFraudRuleEngine ruleEngine, AntiFraudCheckRepository antiFraudCheckRepository,
                             OutboxEventRepository outboxEventRepository, SagaObservability sagaObservability) {
        this.ruleEngine = ruleEngine;
        this.antiFraudCheckRepository = antiFraudCheckRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.sagaObservability = sagaObservability;
    }

    @Transactional
    public void avaliar(TransferReserved event) {
        Optional<String> motivoRejeicao = ruleEngine.avaliar(event);

        AntiFraudCheck check;
        OutboxEvent outboxEvent;
        if (motivoRejeicao.isPresent()) {
            String motivo = motivoRejeicao.get();
            check = AntiFraudCheck.rejeitado(event.transferId(), motivo);
            AntiFraudRejected payload = new AntiFraudRejected(event.transferId(), motivo, Instant.now());
            outboxEvent = criarOutboxEvent(event.transferId(), "AntiFraudRejected", payload);
        } else {
            check = AntiFraudCheck.aprovado(event.transferId());
            AntiFraudApproved payload = new AntiFraudApproved(event.transferId(), Instant.now());
            outboxEvent = criarOutboxEvent(event.transferId(), "AntiFraudApproved", payload);
        }

        antiFraudCheckRepository.save(check);
        outboxEventRepository.save(outboxEvent);
    }

    private String serializePayload(Object event) {
        try {
            return OUTBOX_PAYLOAD_MAPPER.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Falha ao serializar payload do outbox event", e);
        }
    }

    /*
     * Captura o traceparent do trace ATIVO neste exato momento (o span aberto pelo consumer que
     * disparou avaliar()) e grava junto do OutboxEvent — é isso que permite ao
     * OutboxPublisherScheduler, rodando depois numa thread sem relação com esta chamada,
     * propagar o mesmo trace para o transfer-service via Kafka.
     */
    private OutboxEvent criarOutboxEvent(UUID aggregateId, String eventType, Object payload) {
        return OutboxEvent.pending(UUID.randomUUID(), AGGREGATE_TYPE_TRANSFER, aggregateId, eventType,
                serializePayload(payload), sagaObservability.traceparentAtual());
    }
}

package com.bancodigital.transfer.infrastructure.messaging;

import com.bancodigital.transfer.application.SagaOrchestratorService;
import com.bancodigital.transfer.domain.event.CreditConfirmed;
import com.bancodigital.transfer.domain.event.CreditFailed;
import com.bancodigital.transfer.infrastructure.idempotency.IdempotentEventHandler;
import com.bancodigital.transfer.infrastructure.observability.SagaObservability;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.Map;
import java.util.UUID;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class CreditConfirmedConsumer {

    private static final ObjectMapper PAYLOAD_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    private final SagaOrchestratorService sagaOrchestratorService;
    private final IdempotentEventHandler idempotentEventHandler;
    private final SagaObservability sagaObservability;

    public CreditConfirmedConsumer(SagaOrchestratorService sagaOrchestratorService, IdempotentEventHandler idempotentEventHandler,
                                    SagaObservability sagaObservability) {
        this.sagaOrchestratorService = sagaOrchestratorService;
        this.idempotentEventHandler = idempotentEventHandler;
        this.sagaObservability = sagaObservability;
    }

    // transfer.credited carrega CreditConfirmed e CreditFailed (conta destino bloqueada/fechada),
    // distinguidos pelo header eventType adicionado pelo OutboxPublisherScheduler do account-service.
    @KafkaListener(topics = "transfer.credited", groupId = "transfer-service")
    public void consume(String payload, @Header("eventType") String eventType, @Header("eventId") String eventId,
                         @Header(value = "traceparent", required = false) String traceparent) {
        idempotentEventHandler.processarSeNovo(UUID.fromString(eventId), eventType, () -> {
            try {
                switch (eventType) {
                    case "CreditConfirmed" -> {
                        CreditConfirmed event = PAYLOAD_MAPPER.readValue(payload, CreditConfirmed.class);
                        sagaObservability.executarEtapa("orchestrate.credit_result", traceparent,
                                Map.of("transferId", event.transferId().toString(), "valor", event.valor().toString()),
                                () -> sagaOrchestratorService.processarCreditoConfirmado(event));
                    }
                    case "CreditFailed" -> {
                        CreditFailed event = PAYLOAD_MAPPER.readValue(payload, CreditFailed.class);
                        sagaObservability.executarEtapa("orchestrate.credit_result", traceparent,
                                Map.of("transferId", event.transferId().toString()),
                                () -> sagaOrchestratorService.processarCreditoFalhou(event));
                    }
                    default -> {
                        // eventType desconhecido: ignora.
                    }
                }
            } catch (Exception e) {
                throw new IllegalStateException("Falha ao processar evento de crédito (" + eventType + "): " + payload, e);
            }
        });
    }
}

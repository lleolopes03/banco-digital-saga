package com.bancodigital.transfer.infrastructure.messaging;

import com.bancodigital.transfer.application.SagaOrchestratorService;
import com.bancodigital.transfer.domain.event.CreditConfirmed;
import com.bancodigital.transfer.domain.event.CreditFailed;
import com.bancodigital.transfer.infrastructure.idempotency.IdempotentEventHandler;
import com.bancodigital.transfer.infrastructure.observability.SagaObservability;
import java.util.Map;
import java.util.UUID;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class CreditConfirmedConsumer {

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
            switch (eventType) {
                case "CreditConfirmed" -> {
                    CreditConfirmed event = EventPayloadReader.ler(payload, CreditConfirmed.class);
                    sagaObservability.executarEtapa("orchestrate.credit_result", traceparent,
                            Map.of("transferId", event.transferId().toString(), "valor", event.valor().toString()),
                            () -> sagaOrchestratorService.processarCreditoConfirmado(event));
                }
                case "CreditFailed" -> {
                    CreditFailed event = EventPayloadReader.ler(payload, CreditFailed.class);
                    sagaObservability.executarEtapa("orchestrate.credit_result", traceparent,
                            Map.of("transferId", event.transferId().toString()),
                            () -> sagaOrchestratorService.processarCreditoFalhou(event));
                }
                default -> {
                    // eventType desconhecido: ignora.
                }
            }
        });
    }
}

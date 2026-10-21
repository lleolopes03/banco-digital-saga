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

    private final IdempotentEventHandler idempotentEventHandler;
    private final EventRouter router;

    public CreditConfirmedConsumer(SagaOrchestratorService sagaOrchestratorService, IdempotentEventHandler idempotentEventHandler,
            SagaObservability sagaObservability) {
        this.idempotentEventHandler = idempotentEventHandler;
        this.router = EventRouter.ignorandoDesconhecidos()
                .quando(CreditConfirmed.class, (event, traceparent) -> sagaObservability.executarEtapa("orchestrate.credit_result", traceparent,
                        Map.of("transferId", event.transferId().toString(), "valor", event.valor().toString()),
                        () -> sagaOrchestratorService.processarCreditoConfirmado(event)))
                .quando(CreditFailed.class, (event, traceparent) -> sagaObservability.executarEtapa("orchestrate.credit_result", traceparent,
                        Map.of("transferId", event.transferId().toString()),
                        () -> sagaOrchestratorService.processarCreditoFalhou(event)));
    }

    // transfer.credited carrega CreditConfirmed e CreditFailed (conta destino bloqueada/fechada).
    @KafkaListener(topics = "transfer.credited", groupId = "transfer-service")
    public void consume(String payload, @Header("eventType") String eventType, @Header("eventId") String eventId,
            @Header(value = "traceparent", required = false) String traceparent) {
        idempotentEventHandler.processarSeNovo(UUID.fromString(eventId), eventType,
                () -> router.encaminhar(eventType, payload, traceparent));
    }
}

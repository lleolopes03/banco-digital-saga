package com.bancodigital.transfer.infrastructure.messaging;

import com.bancodigital.transfer.application.SagaOrchestratorService;
import com.bancodigital.transfer.domain.event.DebitReversed;
import com.bancodigital.transfer.domain.event.ReservationReleased;
import com.bancodigital.transfer.infrastructure.idempotency.IdempotentEventHandler;
import com.bancodigital.transfer.infrastructure.observability.SagaObservability;
import java.util.Map;
import java.util.UUID;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class TransferCompensatedConsumer {

    private final IdempotentEventHandler idempotentEventHandler;
    private final EventRouter router;

    public TransferCompensatedConsumer(SagaOrchestratorService sagaOrchestratorService, IdempotentEventHandler idempotentEventHandler,
            SagaObservability sagaObservability) {
        this.idempotentEventHandler = idempotentEventHandler;
        this.router = EventRouter.ignorandoDesconhecidos()
                .quando(ReservationReleased.class, (event, traceparent) -> sagaObservability.executarEtapa("orchestrate.compensation_ack", traceparent,
                        Map.of("transferId", event.transferId().toString()),
                        () -> sagaOrchestratorService.processarReservaLiberada(event)))
                .quando(DebitReversed.class, (event, traceparent) -> sagaObservability.executarEtapa("orchestrate.compensation_ack", traceparent,
                        Map.of("transferId", event.transferId().toString()),
                        () -> sagaOrchestratorService.processarDebitoRevertido(event)));
    }

    // transfer.compensated carrega os resultados das compensações: ReservationReleased e DebitReversed.
    @KafkaListener(topics = "transfer.compensated", groupId = "transfer-service")
    public void consume(String payload, @Header("eventType") String eventType, @Header("eventId") String eventId,
            @Header(value = "traceparent", required = false) String traceparent) {
        idempotentEventHandler.processarSeNovo(UUID.fromString(eventId), eventType,
                () -> router.encaminhar(eventType, payload, traceparent));
    }
}

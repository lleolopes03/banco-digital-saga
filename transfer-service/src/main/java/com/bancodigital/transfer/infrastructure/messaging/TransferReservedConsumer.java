package com.bancodigital.transfer.infrastructure.messaging;

import com.bancodigital.transfer.application.SagaOrchestratorService;
import com.bancodigital.transfer.domain.event.TransferReservationFailed;
import com.bancodigital.transfer.domain.event.TransferReserved;
import com.bancodigital.transfer.infrastructure.idempotency.IdempotentEventHandler;
import com.bancodigital.transfer.infrastructure.observability.SagaObservability;
import java.util.Map;
import java.util.UUID;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class TransferReservedConsumer {

    private final IdempotentEventHandler idempotentEventHandler;
    private final EventRouter router;

    public TransferReservedConsumer(SagaOrchestratorService sagaOrchestratorService, IdempotentEventHandler idempotentEventHandler,
            SagaObservability sagaObservability) {
        this.idempotentEventHandler = idempotentEventHandler;
        this.router = EventRouter.ignorandoDesconhecidos()
                .quando(TransferReserved.class, (event, traceparent) -> sagaObservability.executarEtapa("orchestrate.reserve_ack", traceparent,
                        Map.of("transferId", event.transferId().toString(), "valor", event.valor().toString()),
                        () -> sagaOrchestratorService.processarReservaConfirmada(event)))
                .quando(TransferReservationFailed.class, (event, traceparent) -> sagaObservability.executarEtapa("orchestrate.reservation_failed", traceparent,
                        Map.of("transferId", event.transferId().toString(), "motivo", String.valueOf(event.motivo())),
                        () -> sagaOrchestratorService.processarReservaFalhou(event)));
    }

    // transfer.reserved carrega TransferReserved e TransferReservationFailed (resultado da mesma etapa de reserva no account-service).
    @KafkaListener(topics = "transfer.reserved", groupId = "transfer-service")
    public void consume(String payload, @Header("eventType") String eventType, @Header("eventId") String eventId,
            @Header(value = "traceparent", required = false) String traceparent) {
        idempotentEventHandler.processarSeNovo(UUID.fromString(eventId), eventType,
                () -> router.encaminhar(eventType, payload, traceparent));
    }
}

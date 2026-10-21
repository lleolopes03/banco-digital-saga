package com.bancodigital.notification.infrastructure.messaging;

import com.bancodigital.notification.application.NotificationService;
import com.bancodigital.notification.domain.event.TransferCompleted;
import com.bancodigital.notification.domain.event.TransferFailed;
import com.bancodigital.notification.infrastructure.idempotency.IdempotentEventHandler;
import com.bancodigital.notification.infrastructure.observability.SagaObservability;
import java.util.Map;
import java.util.UUID;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class TransferFinishedConsumer {

    private final IdempotentEventHandler idempotentEventHandler;
    private final EventRouter router;

    public TransferFinishedConsumer(NotificationService notificationService, IdempotentEventHandler idempotentEventHandler,
            SagaObservability sagaObservability) {
        this.idempotentEventHandler = idempotentEventHandler;
        this.router = EventRouter.rejeitandoDesconhecidos("transfer.finished")
                .quando(TransferCompleted.class, (event, traceparent) -> sagaObservability.executarEtapa("notify", traceparent,
                        Map.of("transferId", event.transferId().toString(), "valor", event.valor().toString()),
                        () -> notificationService.registrarTransferConcluida(event)))
                .quando(TransferFailed.class, (event, traceparent) -> sagaObservability.executarEtapa("notify", traceparent,
                        Map.of("transferId", event.transferId().toString(), "valor", event.valor().toString()),
                        () -> notificationService.registrarTransferFalhou(event)));
    }

    // transfer.finished carrega os dois eventos terminais da saga: TransferCompleted e TransferFailed.
    @KafkaListener(topics = "transfer.finished", groupId = "notification-service")
    public void consume(String payload, @Header("eventType") String eventType, @Header("eventId") String eventId,
            @Header(value = "traceparent", required = false) String traceparent) {
        idempotentEventHandler.processarSeNovo(UUID.fromString(eventId), eventType,
                () -> router.encaminhar(eventType, payload, traceparent));
    }
}

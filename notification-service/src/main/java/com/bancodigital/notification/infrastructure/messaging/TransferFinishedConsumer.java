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

    private final NotificationService notificationService;
    private final IdempotentEventHandler idempotentEventHandler;
    private final SagaObservability sagaObservability;

    public TransferFinishedConsumer(NotificationService notificationService, IdempotentEventHandler idempotentEventHandler,
                                     SagaObservability sagaObservability) {
        this.notificationService = notificationService;
        this.idempotentEventHandler = idempotentEventHandler;
        this.sagaObservability = sagaObservability;
    }

    /*
     * O tópico transfer.finished carrega os dois eventos terminais da saga (TransferCompleted e
     * TransferFailed, publicados pelo transfer-service via Outbox) — mesmo padrão de tópico
     * compartilhado + header eventType já usado em transfer.compensate.
     */
    @KafkaListener(topics = "transfer.finished", groupId = "notification-service")
    public void consume(String payload, @Header("eventType") String eventType, @Header("eventId") String eventId,
                         @Header(value = "traceparent", required = false) String traceparent) {
        idempotentEventHandler.processarSeNovo(UUID.fromString(eventId), eventType, () -> {
            switch (eventType) {
                case "TransferCompleted" -> {
                    TransferCompleted event = EventPayloadReader.ler(payload, TransferCompleted.class);
                    sagaObservability.executarEtapa("notify", traceparent,
                            Map.of("transferId", event.transferId().toString(), "valor", event.valor().toString()),
                            () -> notificationService.registrarTransferConcluida(event));
                }
                case "TransferFailed" -> {
                    TransferFailed event = EventPayloadReader.ler(payload, TransferFailed.class);
                    sagaObservability.executarEtapa("notify", traceparent,
                            Map.of("transferId", event.transferId().toString(), "valor", event.valor().toString()),
                            () -> notificationService.registrarTransferFalhou(event));
                }
                default -> throw new IllegalStateException("eventType desconhecido em transfer.finished: " + eventType);
            }
        });
    }
}

package com.bancodigital.notification.infrastructure.messaging;

import com.bancodigital.notification.application.NotificationService;
import com.bancodigital.notification.domain.event.TransferCompleted;
import com.bancodigital.notification.domain.event.TransferFailed;
import com.bancodigital.notification.infrastructure.idempotency.IdempotentEventHandler;
import com.bancodigital.notification.infrastructure.observability.SagaObservability;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.Map;
import java.util.UUID;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class TransferFinishedConsumer {

    private static final ObjectMapper PAYLOAD_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

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
            try {
                switch (eventType) {
                    case "TransferCompleted" -> {
                        TransferCompleted event = PAYLOAD_MAPPER.readValue(payload, TransferCompleted.class);
                        sagaObservability.executarEtapa("notify", traceparent,
                                Map.of("transferId", event.transferId().toString(), "valor", event.valor().toString()),
                                () -> notificationService.registrarTransferConcluida(event));
                    }
                    case "TransferFailed" -> {
                        TransferFailed event = PAYLOAD_MAPPER.readValue(payload, TransferFailed.class);
                        sagaObservability.executarEtapa("notify", traceparent,
                                Map.of("transferId", event.transferId().toString(), "valor", event.valor().toString()),
                                () -> notificationService.registrarTransferFalhou(event));
                    }
                    default -> throw new IllegalStateException("eventType desconhecido em transfer.finished: " + eventType);
                }
            } catch (Exception e) {
                throw new IllegalStateException("Falha ao processar evento do tópico transfer.finished: " + payload, e);
            }
        });
    }
}

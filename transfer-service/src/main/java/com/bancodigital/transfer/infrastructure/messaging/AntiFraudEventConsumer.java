package com.bancodigital.transfer.infrastructure.messaging;

import com.bancodigital.transfer.application.SagaOrchestratorService;
import com.bancodigital.transfer.domain.event.AntiFraudApproved;
import com.bancodigital.transfer.domain.event.AntiFraudRejected;
import com.bancodigital.transfer.infrastructure.idempotency.IdempotentEventHandler;
import com.bancodigital.transfer.infrastructure.observability.SagaObservability;
import java.util.Map;
import java.util.UUID;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class AntiFraudEventConsumer {

    private final SagaOrchestratorService sagaOrchestratorService;
    private final IdempotentEventHandler idempotentEventHandler;
    private final SagaObservability sagaObservability;

    public AntiFraudEventConsumer(SagaOrchestratorService sagaOrchestratorService, IdempotentEventHandler idempotentEventHandler,
                                   SagaObservability sagaObservability) {
        this.sagaOrchestratorService = sagaOrchestratorService;
        this.idempotentEventHandler = idempotentEventHandler;
        this.sagaObservability = sagaObservability;
    }

    // antifraud.events carrega AntiFraudApproved e AntiFraudRejected, distinguidos pelo header
    // eventType (adicionado pelo OutboxPublisherScheduler do antifraud-service).
    @KafkaListener(topics = "antifraud.events", groupId = "transfer-service")
    public void consume(String payload, @Header("eventType") String eventType, @Header("eventId") String eventId,
                         @Header(value = "traceparent", required = false) String traceparent) {
        idempotentEventHandler.processarSeNovo(UUID.fromString(eventId), eventType, () -> {
            switch (eventType) {
                case "AntiFraudApproved" -> {
                    AntiFraudApproved event = EventPayloadReader.ler(payload, AntiFraudApproved.class);
                    sagaObservability.executarEtapa("orchestrate.antifraud_result", traceparent,
                            Map.of("transferId", event.transferId().toString()),
                            () -> sagaOrchestratorService.processarAntifraudeAprovada(event));
                }
                case "AntiFraudRejected" -> {
                    AntiFraudRejected event = EventPayloadReader.ler(payload, AntiFraudRejected.class);
                    sagaObservability.executarEtapa("orchestrate.antifraud_result", traceparent,
                            Map.of("transferId", event.transferId().toString()),
                            () -> sagaOrchestratorService.processarAntifraudeRejeitada(event));
                }
                default -> {
                    // eventType desconhecido: ignora.
                }
            }
        });
    }
}

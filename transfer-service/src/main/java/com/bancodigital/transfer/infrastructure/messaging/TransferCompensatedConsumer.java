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

    private final SagaOrchestratorService sagaOrchestratorService;
    private final IdempotentEventHandler idempotentEventHandler;
    private final SagaObservability sagaObservability;

    public TransferCompensatedConsumer(SagaOrchestratorService sagaOrchestratorService, IdempotentEventHandler idempotentEventHandler,
                                        SagaObservability sagaObservability) {
        this.sagaOrchestratorService = sagaOrchestratorService;
        this.idempotentEventHandler = idempotentEventHandler;
        this.sagaObservability = sagaObservability;
    }

    // transfer.compensated carrega os resultados de uma compensação executada pelo account-service:
    // ReservationReleased (cenário 1/timeout pré-débito) e DebitReversed (cenário 2/timeout pós-débito),
    // distinguidos pelo header eventType.
    @KafkaListener(topics = "transfer.compensated", groupId = "transfer-service")
    public void consume(String payload, @Header("eventType") String eventType, @Header("eventId") String eventId,
                         @Header(value = "traceparent", required = false) String traceparent) {
        idempotentEventHandler.processarSeNovo(UUID.fromString(eventId), eventType, () -> {
            switch (eventType) {
                case "ReservationReleased" -> {
                    ReservationReleased event = EventPayloadReader.ler(payload, ReservationReleased.class);
                    sagaObservability.executarEtapa("orchestrate.compensation_ack", traceparent,
                            Map.of("transferId", event.transferId().toString()),
                            () -> sagaOrchestratorService.processarReservaLiberada(event));
                }
                case "DebitReversed" -> {
                    DebitReversed event = EventPayloadReader.ler(payload, DebitReversed.class);
                    sagaObservability.executarEtapa("orchestrate.compensation_ack", traceparent,
                            Map.of("transferId", event.transferId().toString()),
                            () -> sagaOrchestratorService.processarDebitoRevertido(event));
                }
                default -> {
                    // eventType desconhecido: ignora.
                }
            }
        });
    }
}

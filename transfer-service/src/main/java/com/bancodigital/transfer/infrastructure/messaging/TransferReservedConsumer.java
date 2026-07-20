package com.bancodigital.transfer.infrastructure.messaging;

import com.bancodigital.transfer.application.SagaOrchestratorService;
import com.bancodigital.transfer.domain.event.TransferReserved;
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
public class TransferReservedConsumer {

    private static final ObjectMapper PAYLOAD_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    private final SagaOrchestratorService sagaOrchestratorService;
    private final IdempotentEventHandler idempotentEventHandler;
    private final SagaObservability sagaObservability;

    public TransferReservedConsumer(SagaOrchestratorService sagaOrchestratorService, IdempotentEventHandler idempotentEventHandler,
                                     SagaObservability sagaObservability) {
        this.sagaOrchestratorService = sagaOrchestratorService;
        this.idempotentEventHandler = idempotentEventHandler;
        this.sagaObservability = sagaObservability;
    }

    /*
     * transfer.reserved carrega TransferReserved e TransferReservationFailed (publicados pelo
     * account-service como resultado da mesma etapa de reserva). O caminho de falha entra em
     * uma fatia futura de compensação — aqui só reagimos ao sucesso.
     */
    @KafkaListener(topics = "transfer.reserved", groupId = "transfer-service")
    public void consume(String payload, @Header("eventType") String eventType, @Header("eventId") String eventId,
                         @Header(value = "traceparent", required = false) String traceparent) {
        if (!"TransferReserved".equals(eventType)) {
            return;
        }
        idempotentEventHandler.processarSeNovo(UUID.fromString(eventId), eventType, () -> {
            try {
                TransferReserved event = PAYLOAD_MAPPER.readValue(payload, TransferReserved.class);
                sagaObservability.executarEtapa("orchestrate.reserve_ack", traceparent,
                        Map.of("transferId", event.transferId().toString(), "valor", event.valor().toString()),
                        () -> sagaOrchestratorService.processarReservaConfirmada(event));
            } catch (Exception e) {
                throw new IllegalStateException("Falha ao processar TransferReserved: " + payload, e);
            }
        });
    }
}

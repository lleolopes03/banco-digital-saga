package com.bancodigital.antifraud.infrastructure.messaging;

import com.bancodigital.antifraud.application.AntiFraudService;
import com.bancodigital.antifraud.domain.event.TransferReserved;
import com.bancodigital.antifraud.infrastructure.idempotency.IdempotentEventHandler;
import com.bancodigital.antifraud.infrastructure.observability.SagaObservability;
import java.util.Map;
import java.util.UUID;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class TransferReservedConsumer {

    private final AntiFraudService antiFraudService;
    private final IdempotentEventHandler idempotentEventHandler;
    private final SagaObservability sagaObservability;

    public TransferReservedConsumer(AntiFraudService antiFraudService, IdempotentEventHandler idempotentEventHandler,
                                     SagaObservability sagaObservability) {
        this.antiFraudService = antiFraudService;
        this.idempotentEventHandler = idempotentEventHandler;
        this.sagaObservability = sagaObservability;
    }

    /*
     * O tópico transfer.reserved carrega dois tipos de evento (TransferReserved e
     * TransferReservationFailed, publicados pelo account-service como resultado da mesma etapa
     * de reserva). Só nos interessa o primeiro aqui — reserva que falhou não tem o que ser
     * avaliado por antifraude. O header eventType (adicionado pelo OutboxPublisherScheduler do
     * account-service) é quem permite diferenciar sem depender do formato do payload.
     */
    @KafkaListener(topics = "transfer.reserved", groupId = "antifraud-service")
    public void consume(String payload, @Header("eventType") String eventType, @Header("eventId") String eventId,
                         @Header(value = "traceparent", required = false) String traceparent) {
        if (!"TransferReserved".equals(eventType)) {
            return;
        }
        idempotentEventHandler.processarSeNovo(UUID.fromString(eventId), eventType, () -> {
            TransferReserved event = EventPayloadReader.ler(payload, TransferReserved.class);
            sagaObservability.executarEtapa("antifraud_check", traceparent,
                    Map.of("transferId", event.transferId().toString(), "valor", event.valor().toString()),
                    () -> antiFraudService.avaliar(event));
        });
    }
}

package com.bancodigital.account.infrastructure.messaging;

import com.bancodigital.account.application.AccountSagaService;
import com.bancodigital.account.domain.event.TransferRequested;
import com.bancodigital.account.infrastructure.idempotency.IdempotentEventHandler;
import com.bancodigital.account.infrastructure.observability.SagaObservability;
import java.util.Map;
import java.util.UUID;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class TransferRequestedConsumer {

    private static final String EVENT_TYPE = "TransferRequested";

    private final AccountSagaService accountSagaService;
    private final IdempotentEventHandler idempotentEventHandler;
    private final SagaObservability sagaObservability;

    public TransferRequestedConsumer(AccountSagaService accountSagaService, IdempotentEventHandler idempotentEventHandler,
                                      SagaObservability sagaObservability) {
        this.accountSagaService = accountSagaService;
        this.idempotentEventHandler = idempotentEventHandler;
        this.sagaObservability = sagaObservability;
    }

    @KafkaListener(topics = "transfer.requested", groupId = "account-service")
    public void consume(String payload, @Header("eventId") String eventId,
                         @Header(value = "traceparent", required = false) String traceparent) {
        idempotentEventHandler.processarSeNovo(UUID.fromString(eventId), EVENT_TYPE, () -> {
            TransferRequested event = EventPayloadReader.ler(payload, TransferRequested.class);
            sagaObservability.executarEtapa("reserve", traceparent,
                    Map.of("transferId", event.transferId().toString(), "valor", event.valor().toString()),
                    () -> accountSagaService.reservarSaldo(event));
        });
    }
}

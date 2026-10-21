package com.bancodigital.account.infrastructure.messaging;

import com.bancodigital.account.application.AccountSagaService;
import com.bancodigital.account.domain.event.DebitConfirmationRequested;
import com.bancodigital.account.infrastructure.idempotency.IdempotentEventHandler;
import com.bancodigital.account.infrastructure.observability.SagaObservability;
import java.util.Map;
import java.util.UUID;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class DebitConfirmationRequestedConsumer {

    private static final String EVENT_TYPE = DebitConfirmationRequested.class.getSimpleName();

    private final AccountSagaService accountSagaService;
    private final IdempotentEventHandler idempotentEventHandler;
    private final SagaObservability sagaObservability;

    public DebitConfirmationRequestedConsumer(AccountSagaService accountSagaService, IdempotentEventHandler idempotentEventHandler,
                                                SagaObservability sagaObservability) {
        this.accountSagaService = accountSagaService;
        this.idempotentEventHandler = idempotentEventHandler;
        this.sagaObservability = sagaObservability;
    }

    @KafkaListener(topics = "transfer.debit", groupId = "account-service")
    public void consume(String payload, @Header("eventId") String eventId,
                         @Header(value = "traceparent", required = false) String traceparent) {
        idempotentEventHandler.processarSeNovo(UUID.fromString(eventId), EVENT_TYPE, () -> {
            DebitConfirmationRequested event = EventPayloadReader.ler(payload, DebitConfirmationRequested.class);
            sagaObservability.executarEtapa("debit", traceparent,
                    Map.of("transferId", event.transferId().toString(), "valor", event.valor().toString()),
                    () -> accountSagaService.confirmarDebito(event));
        });
    }
}

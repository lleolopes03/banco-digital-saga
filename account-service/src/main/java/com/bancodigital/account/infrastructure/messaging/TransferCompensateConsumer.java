package com.bancodigital.account.infrastructure.messaging;

import com.bancodigital.account.application.AccountSagaService;
import com.bancodigital.account.domain.event.ReleaseReservedAmount;
import com.bancodigital.account.domain.event.ReverseDebitRequested;
import com.bancodigital.account.infrastructure.idempotency.IdempotentEventHandler;
import com.bancodigital.account.infrastructure.observability.SagaObservability;
import java.util.Map;
import java.util.UUID;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class TransferCompensateConsumer {

    private final IdempotentEventHandler idempotentEventHandler;
    private final EventRouter router;

    public TransferCompensateConsumer(AccountSagaService accountSagaService, IdempotentEventHandler idempotentEventHandler,
            SagaObservability sagaObservability) {
        this.idempotentEventHandler = idempotentEventHandler;
        this.router = EventRouter.ignorandoDesconhecidos()
                .quando(ReleaseReservedAmount.class, (event, traceparent) -> sagaObservability.executarEtapa("compensate", traceparent,
                        Map.of("transferId", event.transferId().toString(), "valor", event.valor().toString()),
                        () -> accountSagaService.liberarReserva(event)))
                .quando(ReverseDebitRequested.class, (event, traceparent) -> sagaObservability.executarEtapa("compensate", traceparent,
                        Map.of("transferId", event.transferId().toString(), "valor", event.valor().toString()),
                        () -> accountSagaService.reverterDebito(event)));
    }

    // transfer.compensate carrega os dois comandos de compensação, que são a mesma etapa de negócio: desfazer dinheiro que não completou a transferência.
    @KafkaListener(topics = "transfer.compensate", groupId = "account-service")
    public void consume(String payload, @Header("eventType") String eventType, @Header("eventId") String eventId,
            @Header(value = "traceparent", required = false) String traceparent) {
        idempotentEventHandler.processarSeNovo(UUID.fromString(eventId), eventType,
                () -> router.encaminhar(eventType, payload, traceparent));
    }
}

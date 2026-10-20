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

    private final AccountSagaService accountSagaService;
    private final IdempotentEventHandler idempotentEventHandler;
    private final SagaObservability sagaObservability;

    public TransferCompensateConsumer(AccountSagaService accountSagaService, IdempotentEventHandler idempotentEventHandler,
                                       SagaObservability sagaObservability) {
        this.accountSagaService = accountSagaService;
        this.idempotentEventHandler = idempotentEventHandler;
        this.sagaObservability = sagaObservability;
    }

    // transfer.compensate carrega os comandos de compensação publicados pelo transfer-service:
    // ReleaseReservedAmount (cenário 1/timeout pré-débito) e ReverseDebitRequested (cenário
    // 2/timeout pós-débito), distinguidos pelo header eventType. Os dois ramos usam o mesmo nome
    // de span/etapa ("compensate") — são as duas formas de uma mesma etapa de negócio: desfazer
    // dinheiro que não completou a transferência.
    @KafkaListener(topics = "transfer.compensate", groupId = "account-service")
    public void consume(String payload, @Header("eventType") String eventType, @Header("eventId") String eventId,
                         @Header(value = "traceparent", required = false) String traceparent) {
        idempotentEventHandler.processarSeNovo(UUID.fromString(eventId), eventType, () -> {
            switch (eventType) {
                case "ReleaseReservedAmount" -> {
                    ReleaseReservedAmount event = EventPayloadReader.ler(payload, ReleaseReservedAmount.class);
                    sagaObservability.executarEtapa("compensate", traceparent,
                            Map.of("transferId", event.transferId().toString(), "valor", event.valor().toString()),
                            () -> accountSagaService.liberarReserva(event));
                }
                case "ReverseDebitRequested" -> {
                    ReverseDebitRequested event = EventPayloadReader.ler(payload, ReverseDebitRequested.class);
                    sagaObservability.executarEtapa("compensate", traceparent,
                            Map.of("transferId", event.transferId().toString(), "valor", event.valor().toString()),
                            () -> accountSagaService.reverterDebito(event));
                }
                default -> {
                    // eventType desconhecido: ignora.
                }
            }
        });
    }
}

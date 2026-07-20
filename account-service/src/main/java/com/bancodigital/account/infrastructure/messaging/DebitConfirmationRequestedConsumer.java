package com.bancodigital.account.infrastructure.messaging;

import com.bancodigital.account.application.AccountService;
import com.bancodigital.account.domain.event.DebitConfirmationRequested;
import com.bancodigital.account.infrastructure.idempotency.IdempotentEventHandler;
import com.bancodigital.account.infrastructure.observability.SagaObservability;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.Map;
import java.util.UUID;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class DebitConfirmationRequestedConsumer {

    private static final ObjectMapper PAYLOAD_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());
    private static final String EVENT_TYPE = "DebitConfirmationRequested";

    private final AccountService accountService;
    private final IdempotentEventHandler idempotentEventHandler;
    private final SagaObservability sagaObservability;

    public DebitConfirmationRequestedConsumer(AccountService accountService, IdempotentEventHandler idempotentEventHandler,
                                                SagaObservability sagaObservability) {
        this.accountService = accountService;
        this.idempotentEventHandler = idempotentEventHandler;
        this.sagaObservability = sagaObservability;
    }

    @KafkaListener(topics = "transfer.debit", groupId = "account-service")
    public void consume(String payload, @Header("eventId") String eventId,
                         @Header(value = "traceparent", required = false) String traceparent) {
        idempotentEventHandler.processarSeNovo(UUID.fromString(eventId), EVENT_TYPE, () -> {
            try {
                DebitConfirmationRequested event = PAYLOAD_MAPPER.readValue(payload, DebitConfirmationRequested.class);
                sagaObservability.executarEtapa("debit", traceparent,
                        Map.of("transferId", event.transferId().toString(), "valor", event.valor().toString()),
                        () -> accountService.confirmarDebito(event));
            } catch (Exception e) {
                throw new IllegalStateException("Falha ao processar DebitConfirmationRequested: " + payload, e);
            }
        });
    }
}

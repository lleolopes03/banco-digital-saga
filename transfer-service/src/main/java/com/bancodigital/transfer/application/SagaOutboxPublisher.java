package com.bancodigital.transfer.application;

import com.bancodigital.transfer.domain.OutboxEvent;
import com.bancodigital.transfer.domain.SagaInstance;
import com.bancodigital.transfer.domain.event.CreditRequested;
import com.bancodigital.transfer.domain.event.DebitConfirmationRequested;
import com.bancodigital.transfer.domain.event.ReleaseReservedAmount;
import com.bancodigital.transfer.domain.event.ReverseDebitRequested;
import com.bancodigital.transfer.domain.event.TransferCompleted;
import com.bancodigital.transfer.domain.event.TransferFailed;
import com.bancodigital.transfer.domain.event.TransferRequested;
import com.bancodigital.transfer.infrastructure.observability.SagaObservability;
import com.bancodigital.transfer.infrastructure.persistence.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Instant;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * Tudo o que a saga publica para o resto do sistema, com um método por comando/evento. Cada um grava
 * um OutboxEvent PENDING na transação corrente do chamador (nenhum código de negócio fala direto com o
 * Kafka); o OutboxPublisherScheduler publica depois.
 *
 * O tipo do evento é o nome simples da classe do payload (record em {@code domain.event}), então não
 * há string de tipo escrita à mão para divergir da classe. O traceparent do trace ATIVO neste momento
 * (o span HTTP de iniciarTransferencia, ou o do consumer que disparou o método de negócio) é gravado
 * junto, para o scheduler propagar o mesmo trace ao próximo hop via Kafka.
 */
@Component
public class SagaOutboxPublisher {

    private static final String AGGREGATE_TYPE_TRANSFER = "Transfer";
    private static final ObjectMapper PAYLOAD_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    private final OutboxEventRepository outboxEventRepository;
    private final SagaObservability sagaObservability;

    public SagaOutboxPublisher(OutboxEventRepository outboxEventRepository, SagaObservability sagaObservability) {
        this.outboxEventRepository = outboxEventRepository;
        this.sagaObservability = sagaObservability;
    }

    public void transferenciaSolicitada(SagaInstance saga) {
        gravar(saga, new TransferRequested(saga.getId(), saga.getAccountOrigemId(), saga.getAccountDestinoId(),
                saga.getValor(), Instant.now()));
    }

    public void debitoSolicitado(SagaInstance saga) {
        gravar(saga, new DebitConfirmationRequested(saga.getId(), saga.getAccountOrigemId(), saga.getValor(),
                Instant.now()));
    }

    public void creditoSolicitado(SagaInstance saga) {
        gravar(saga, new CreditRequested(saga.getId(), saga.getAccountDestinoId(), saga.getValor(), Instant.now()));
    }

    public void liberacaoDeReservaSolicitada(SagaInstance saga) {
        gravar(saga, new ReleaseReservedAmount(saga.getId(), saga.getAccountOrigemId(), saga.getValor(),
                Instant.now()));
    }

    public void reversaoDeDebitoSolicitada(SagaInstance saga) {
        gravar(saga, new ReverseDebitRequested(saga.getId(), saga.getAccountOrigemId(), saga.getValor(),
                Instant.now()));
    }

    public void transferenciaConcluida(SagaInstance saga) {
        gravar(saga, new TransferCompleted(saga.getId(), saga.getAccountOrigemId(), saga.getAccountDestinoId(),
                saga.getValor(), Instant.now()));
    }

    public void transferenciaFalhou(SagaInstance saga) {
        gravar(saga, new TransferFailed(saga.getId(), saga.getAccountOrigemId(), saga.getAccountDestinoId(),
                saga.getValor(), saga.getFailureReason(), Instant.now()));
    }

    private void gravar(SagaInstance saga, Object payload) {
        UUID transferId = saga.getId();
        outboxEventRepository.save(OutboxEvent.pending(UUID.randomUUID(), AGGREGATE_TYPE_TRANSFER, transferId,
                payload.getClass().getSimpleName(), serializar(payload), sagaObservability.traceparentAtual()));
    }

    private static String serializar(Object payload) {
        try {
            return PAYLOAD_MAPPER.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Falha ao serializar payload do outbox event", e);
        }
    }
}

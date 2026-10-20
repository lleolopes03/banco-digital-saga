package com.bancodigital.transfer.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bancodigital.transfer.domain.OutboxEvent;
import com.bancodigital.transfer.domain.SagaEvent;
import com.bancodigital.transfer.domain.SagaInstance;
import com.bancodigital.transfer.domain.SagaStateMachine;
import com.bancodigital.transfer.domain.SagaStatus;
import com.bancodigital.transfer.domain.event.AntiFraudApproved;
import com.bancodigital.transfer.domain.event.AntiFraudRejected;
import com.bancodigital.transfer.domain.event.CreditConfirmed;
import com.bancodigital.transfer.domain.event.CreditFailed;
import com.bancodigital.transfer.domain.event.DebitConfirmed;
import com.bancodigital.transfer.domain.event.DebitReversed;
import com.bancodigital.transfer.domain.event.ReservationReleased;
import com.bancodigital.transfer.domain.event.TransferReservationFailed;
import com.bancodigital.transfer.domain.event.TransferReserved;
import com.bancodigital.transfer.infrastructure.observability.SagaObservability;
import com.bancodigital.transfer.infrastructure.persistence.OutboxEventRepository;
import com.bancodigital.transfer.infrastructure.persistence.SagaInstanceRepository;
import com.bancodigital.transfer.infrastructure.websocket.SagaStatusPublisher;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SagaOrchestratorServiceTest {

    @Mock
    private SagaInstanceRepository sagaInstanceRepository;

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Mock
    private SagaObservability sagaObservability;

    @Mock
    private SagaStatusPublisher sagaStatusPublisher;

    private SagaOrchestratorService orchestrator;

    @BeforeEach
    void setUp() {
        // O publisher é o real, sobre o repositório mockado: assim os testes verificam o OutboxEvent
        // efetivamente gravado (tipo, agregado), não só que um método foi chamado.
        SagaOutboxPublisher outboxPublisher = new SagaOutboxPublisher(outboxEventRepository, sagaObservability);
        orchestrator = new SagaOrchestratorService(sagaInstanceRepository, outboxPublisher, sagaObservability,
                sagaStatusPublisher);
    }

    /** Avança a saga por transições reais até o estado desejado, sem reflexão. */
    private SagaInstance criarSagaNoEstado(SagaStatus estadoAlvo) {
        SagaInstance saga = SagaInstance.iniciar(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                new BigDecimal("300.00"));
        SagaStateMachine stateMachine = new SagaStateMachine();
        if (estadoAlvo == SagaStatus.STARTED) {
            return saga;
        }
        stateMachine.transicionar(saga, SagaEvent.RESERVAR_SALDO);
        if (estadoAlvo == SagaStatus.RESERVED) {
            return saga;
        }
        stateMachine.transicionar(saga, SagaEvent.SOLICITAR_ANTIFRAUDE);
        if (estadoAlvo == SagaStatus.ANTIFRAUD_PENDING) {
            return saga;
        }
        if (estadoAlvo == SagaStatus.ANTIFRAUD_REJECTED) {
            stateMachine.transicionar(saga, SagaEvent.REJEITAR_ANTIFRAUDE);
            return saga;
        }
        stateMachine.transicionar(saga, SagaEvent.APROVAR_ANTIFRAUDE);
        if (estadoAlvo == SagaStatus.ANTIFRAUD_APPROVED) {
            return saga;
        }
        stateMachine.transicionar(saga, SagaEvent.DEBITAR);
        if (estadoAlvo == SagaStatus.DEBITED) {
            return saga;
        }
        throw new IllegalArgumentException("Caminho de setup não definido para o estado " + estadoAlvo);
    }

    @Test
    void deveAtualizarSagaParaAntifraudPendingAoProcessarReservaConfirmada() {
        SagaInstance saga = criarSagaNoEstado(SagaStatus.STARTED);
        when(sagaInstanceRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

        TransferReserved event = new TransferReserved(saga.getId(), saga.getAccountOrigemId(),
                saga.getAccountDestinoId(), saga.getValor(), Instant.now());
        orchestrator.processarReservaConfirmada(event);

        assertThat(saga.getStatus()).isEqualTo(SagaStatus.ANTIFRAUD_PENDING);
        verify(sagaInstanceRepository).save(saga);
        verify(outboxEventRepository, never()).save(any());
    }

    @Test
    void deveAtualizarSagaParaAntifraudApprovedEPublicarDebitConfirmationRequested() {
        SagaInstance saga = criarSagaNoEstado(SagaStatus.ANTIFRAUD_PENDING);
        when(sagaInstanceRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

        orchestrator.processarAntifraudeAprovada(new AntiFraudApproved(saga.getId(), Instant.now()));

        assertThat(saga.getStatus()).isEqualTo(SagaStatus.ANTIFRAUD_APPROVED);
        verify(sagaInstanceRepository).save(saga);

        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(outboxCaptor.capture());
        assertThat(outboxCaptor.getValue().getEventType()).isEqualTo("DebitConfirmationRequested");
        assertThat(outboxCaptor.getValue().getAggregateId()).isEqualTo(saga.getId());
    }

    @Test
    void deveAtualizarSagaParaDebitedEPublicarCreditRequested() {
        SagaInstance saga = criarSagaNoEstado(SagaStatus.ANTIFRAUD_APPROVED);
        when(sagaInstanceRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

        orchestrator.processarDebitoConfirmado(new DebitConfirmed(saga.getId(), saga.getAccountOrigemId(),
                saga.getValor(), Instant.now()));

        assertThat(saga.getStatus()).isEqualTo(SagaStatus.DEBITED);
        verify(sagaInstanceRepository).save(saga);

        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(outboxCaptor.capture());
        assertThat(outboxCaptor.getValue().getEventType()).isEqualTo("CreditRequested");
        assertThat(outboxCaptor.getValue().getAggregateId()).isEqualTo(saga.getId());
    }

    @Test
    void deveAtualizarSagaParaCompletedEPublicarTransferCompletedAoProcessarCreditoConfirmado() {
        SagaInstance saga = criarSagaNoEstado(SagaStatus.DEBITED);
        when(sagaInstanceRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

        orchestrator.processarCreditoConfirmado(new CreditConfirmed(saga.getId(), saga.getAccountDestinoId(),
                saga.getValor(), Instant.now()));

        assertThat(saga.getStatus()).isEqualTo(SagaStatus.COMPLETED);
        verify(sagaInstanceRepository).save(saga);

        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(outboxCaptor.capture());
        assertThat(outboxCaptor.getValue().getEventType()).isEqualTo("TransferCompleted");
        assertThat(outboxCaptor.getValue().getAggregateId()).isEqualTo(saga.getId());
    }

    @Test
    void deveAtualizarSagaParaAntifraudRejectedRegistrarMotivoEPublicarReleaseReservedAmount() {
        SagaInstance saga = criarSagaNoEstado(SagaStatus.ANTIFRAUD_PENDING);
        when(sagaInstanceRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

        orchestrator.processarAntifraudeRejeitada(new AntiFraudRejected(saga.getId(), "LIMITE_EXCEDIDO", Instant.now()));

        assertThat(saga.getStatus()).isEqualTo(SagaStatus.ANTIFRAUD_REJECTED);
        assertThat(saga.getFailureReason()).isEqualTo("LIMITE_EXCEDIDO");
        verify(sagaInstanceRepository).save(saga);

        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(outboxCaptor.capture());
        assertThat(outboxCaptor.getValue().getEventType()).isEqualTo("ReleaseReservedAmount");
        assertThat(outboxCaptor.getValue().getAggregateId()).isEqualTo(saga.getId());
    }

    @Test
    void deveRegistrarMotivoEPublicarReverseDebitRequestedAoProcessarCreditoFalhou() {
        SagaInstance saga = criarSagaNoEstado(SagaStatus.DEBITED);
        when(sagaInstanceRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

        orchestrator.processarCreditoFalhou(new CreditFailed(saga.getId(), saga.getAccountDestinoId(),
                "CONTA_DESTINO_BLOCKED", Instant.now()));

        assertThat(saga.getStatus()).isEqualTo(SagaStatus.DEBITED);
        assertThat(saga.getFailureReason()).isEqualTo("CONTA_DESTINO_BLOCKED");
        verify(sagaInstanceRepository).save(saga);

        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(outboxCaptor.capture());
        assertThat(outboxCaptor.getValue().getEventType()).isEqualTo("ReverseDebitRequested");
        assertThat(outboxCaptor.getValue().getAggregateId()).isEqualTo(saga.getId());
    }

    @Test
    void deveAtualizarSagaParaFailedEPublicarTransferFailedAoProcessarReservaLiberada() {
        SagaInstance saga = criarSagaNoEstado(SagaStatus.ANTIFRAUD_REJECTED);
        when(sagaInstanceRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

        orchestrator.processarReservaLiberada(new ReservationReleased(saga.getId(), Instant.now()));

        assertThat(saga.getStatus()).isEqualTo(SagaStatus.FAILED);
        verify(sagaInstanceRepository).save(saga);

        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(outboxCaptor.capture());
        assertThat(outboxCaptor.getValue().getEventType()).isEqualTo("TransferFailed");
        assertThat(outboxCaptor.getValue().getAggregateId()).isEqualTo(saga.getId());
    }

    @Test
    void deveAtualizarSagaParaFailedEPublicarTransferFailedAoProcessarDebitoRevertido() {
        SagaInstance saga = criarSagaNoEstado(SagaStatus.DEBITED);
        when(sagaInstanceRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

        orchestrator.processarDebitoRevertido(new DebitReversed(saga.getId(), saga.getAccountOrigemId(), Instant.now()));

        assertThat(saga.getStatus()).isEqualTo(SagaStatus.FAILED);
        verify(sagaInstanceRepository).save(saga);

        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(outboxCaptor.capture());
        assertThat(outboxCaptor.getValue().getEventType()).isEqualTo("TransferFailed");
        assertThat(outboxCaptor.getValue().getAggregateId()).isEqualTo(saga.getId());
    }

    @Test
    void deveFalharDiretamenteSemCompensacaoEPublicarTransferFailedAoProcessarTimeoutEmStarted() {
        SagaInstance saga = criarSagaNoEstado(SagaStatus.STARTED);
        when(sagaInstanceRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

        orchestrator.processarTimeout(saga.getId());

        assertThat(saga.getStatus()).isEqualTo(SagaStatus.FAILED);
        assertThat(saga.getFailureReason()).isEqualTo("SAGA_TIMEOUT");

        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(outboxCaptor.capture());
        assertThat(outboxCaptor.getValue().getEventType()).isEqualTo("TransferFailed");
    }

    @Test
    void devePublicarReleaseReservedAmountAoProcessarTimeoutEmReserved() {
        SagaInstance saga = criarSagaNoEstado(SagaStatus.RESERVED);
        when(sagaInstanceRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

        orchestrator.processarTimeout(saga.getId());

        assertThat(saga.getStatus()).isEqualTo(SagaStatus.RESERVED);
        assertThat(saga.getFailureReason()).isEqualTo("SAGA_TIMEOUT");

        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(outboxCaptor.capture());
        assertThat(outboxCaptor.getValue().getEventType()).isEqualTo("ReleaseReservedAmount");
    }

    @Test
    void devePublicarReverseDebitRequestedAoProcessarTimeoutEmDebited() {
        SagaInstance saga = criarSagaNoEstado(SagaStatus.DEBITED);
        when(sagaInstanceRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

        orchestrator.processarTimeout(saga.getId());

        assertThat(saga.getStatus()).isEqualTo(SagaStatus.DEBITED);
        assertThat(saga.getFailureReason()).isEqualTo("SAGA_TIMEOUT");

        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(outboxCaptor.capture());
        assertThat(outboxCaptor.getValue().getEventType()).isEqualTo("ReverseDebitRequested");
    }

    @Test
    void deveFalharSagaComMotivoRealEPublicarTransferFailedQuandoReservaFalha() {
        SagaInstance saga = criarSagaNoEstado(SagaStatus.STARTED);
        when(sagaInstanceRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

        orchestrator.processarReservaFalhou(
                new TransferReservationFailed(saga.getId(), "SALDO_INSUFICIENTE", Instant.now()));

        assertThat(saga.getStatus()).isEqualTo(SagaStatus.FAILED);
        assertThat(saga.getFailureReason()).isEqualTo("SALDO_INSUFICIENTE");
        verify(sagaInstanceRepository).save(saga);
        verify(sagaStatusPublisher).publish(saga);

        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(outboxCaptor.capture());
        assertThat(outboxCaptor.getValue().getEventType()).isEqualTo("TransferFailed");
        assertThat(outboxCaptor.getValue().getAggregateId()).isEqualTo(saga.getId());
        // nenhum comando de compensacao: nao ha o que desfazer, nada foi reservado
        assertThat(outboxCaptor.getValue().getEventType()).isNotEqualTo("ReleaseReservedAmount");
    }

    @Test
    void deveIgnorarFalhaDeReservaQuandoSagaJaFoiFalhadaPorTimeout() {
        // o SagaTimeoutScheduler pode ter vencido a corrida: o evento (legitimo) chega depois
        SagaInstance saga = criarSagaNoEstado(SagaStatus.STARTED);
        saga.registrarMotivoFalha("SAGA_TIMEOUT");
        new SagaStateMachine().transicionar(saga, SagaEvent.FALHAR);
        when(sagaInstanceRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

        orchestrator.processarReservaFalhou(
                new TransferReservationFailed(saga.getId(), "SALDO_INSUFICIENTE", Instant.now()));

        assertThat(saga.getStatus()).isEqualTo(SagaStatus.FAILED);
        assertThat(saga.getFailureReason()).isEqualTo("SAGA_TIMEOUT");
        verify(sagaInstanceRepository, never()).save(any());
        verify(outboxEventRepository, never()).save(any());
    }

    @Test
    void deveIgnorarFalhaDeReservaQuandoSagaJaAvancouDeEstado() {
        SagaInstance saga = criarSagaNoEstado(SagaStatus.RESERVED);
        when(sagaInstanceRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

        orchestrator.processarReservaFalhou(
                new TransferReservationFailed(saga.getId(), "SALDO_INSUFICIENTE", Instant.now()));

        assertThat(saga.getStatus()).isEqualTo(SagaStatus.RESERVED);
        verify(sagaInstanceRepository, never()).save(any());
        verify(outboxEventRepository, never()).save(any());
    }
}

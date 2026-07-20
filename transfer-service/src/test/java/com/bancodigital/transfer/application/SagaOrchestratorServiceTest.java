package com.bancodigital.transfer.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bancodigital.transfer.domain.InsufficientBalanceException;
import com.bancodigital.transfer.domain.OutboxEvent;
import com.bancodigital.transfer.domain.SagaEvent;
import com.bancodigital.transfer.domain.SagaInstance;
import com.bancodigital.transfer.domain.SagaInstanceNotFoundException;
import com.bancodigital.transfer.domain.SagaStateMachine;
import com.bancodigital.transfer.domain.SagaStatus;
import com.bancodigital.transfer.domain.event.AntiFraudApproved;
import com.bancodigital.transfer.domain.event.AntiFraudRejected;
import com.bancodigital.transfer.domain.event.CreditConfirmed;
import com.bancodigital.transfer.domain.event.CreditFailed;
import com.bancodigital.transfer.domain.event.DebitConfirmed;
import com.bancodigital.transfer.domain.event.DebitReversed;
import com.bancodigital.transfer.domain.event.ReservationReleased;
import com.bancodigital.transfer.domain.event.TransferReserved;
import com.bancodigital.transfer.infrastructure.client.AccountServiceClient;
import com.bancodigital.transfer.infrastructure.client.BalanceResponse;
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
    private AccountServiceClient accountServiceClient;

    @Mock
    private SagaObservability sagaObservability;

    @Mock
    private SagaStatusPublisher sagaStatusPublisher;

    private SagaOrchestratorService orchestrator;

    @BeforeEach
    void setUp() {
        // accountServiceClient não é stubado explicitamente: Mockito já retorna Optional.empty()
        // por padrão, o que reproduz o comportamento de fallback (pular a pré-checagem de saldo)
        // sem precisar simular o circuito aqui — isso é coberto por TransferServiceCircuitBreakerTest.
        orchestrator = new SagaOrchestratorService(sagaInstanceRepository, outboxEventRepository, accountServiceClient,
                sagaObservability, sagaStatusPublisher);
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
    void deveIniciarTransferenciaCriandoSagaEPublicandoTransferRequested() {
        UUID accountOrigemId = UUID.randomUUID();
        UUID accountDestinoId = UUID.randomUUID();

        UUID transferId = orchestrator.iniciarTransferencia(accountOrigemId, accountDestinoId,
                new BigDecimal("300.00"), null);

        ArgumentCaptor<SagaInstance> sagaCaptor = ArgumentCaptor.forClass(SagaInstance.class);
        verify(sagaInstanceRepository).saveAndFlush(sagaCaptor.capture());
        assertThat(sagaCaptor.getValue().getId()).isEqualTo(transferId);
        assertThat(sagaCaptor.getValue().getStatus()).isEqualTo(SagaStatus.STARTED);

        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(outboxCaptor.capture());
        assertThat(outboxCaptor.getValue().getEventType()).isEqualTo("TransferRequested");
        assertThat(outboxCaptor.getValue().getAggregateType()).isEqualTo("Transfer");
        assertThat(outboxCaptor.getValue().getAggregateId()).isEqualTo(transferId);
    }

    @Test
    void deveCriarSagaQuandoConsultaDeSaldoIndicaSaldoSuficiente() {
        UUID accountOrigemId = UUID.randomUUID();
        when(accountServiceClient.consultarSaldo(accountOrigemId))
                .thenReturn(Optional.of(new BalanceResponse(accountOrigemId, new BigDecimal("1000.00"))));

        UUID transferId = orchestrator.iniciarTransferencia(accountOrigemId, UUID.randomUUID(),
                new BigDecimal("300.00"), null);

        verify(sagaInstanceRepository).saveAndFlush(any());
        assertThat(transferId).isNotNull();
    }

    @Test
    void deveRecusarComSaldoInsuficienteSemCriarSagaQuandoConsultaDeSaldoRespondeuComSucesso() {
        UUID accountOrigemId = UUID.randomUUID();
        when(accountServiceClient.consultarSaldo(accountOrigemId))
                .thenReturn(Optional.of(new BalanceResponse(accountOrigemId, new BigDecimal("10.00"))));

        assertThatThrownBy(() -> orchestrator.iniciarTransferencia(accountOrigemId, UUID.randomUUID(),
                new BigDecimal("300.00"), null))
                .isInstanceOf(InsufficientBalanceException.class);

        verify(sagaInstanceRepository, never()).saveAndFlush(any());
        verify(outboxEventRepository, never()).save(any());
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
    void deveAtualizarSagaParaCompletedAoProcessarCreditoConfirmado() {
        SagaInstance saga = criarSagaNoEstado(SagaStatus.DEBITED);
        when(sagaInstanceRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

        orchestrator.processarCreditoConfirmado(new CreditConfirmed(saga.getId(), saga.getAccountDestinoId(),
                saga.getValor(), Instant.now()));

        assertThat(saga.getStatus()).isEqualTo(SagaStatus.COMPLETED);
        verify(sagaInstanceRepository).save(saga);
        verify(outboxEventRepository, never()).save(any());
    }

    @Test
    void deveLancarExcecaoAoProcessarEventoDeSagaInexistente() {
        UUID transferId = UUID.randomUUID();
        when(sagaInstanceRepository.findById(transferId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> orchestrator.consultarSaga(transferId))
                .isInstanceOf(SagaInstanceNotFoundException.class);
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
    void deveAtualizarSagaParaFailedAoProcessarReservaLiberada() {
        SagaInstance saga = criarSagaNoEstado(SagaStatus.ANTIFRAUD_REJECTED);
        when(sagaInstanceRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

        orchestrator.processarReservaLiberada(new ReservationReleased(saga.getId(), Instant.now()));

        assertThat(saga.getStatus()).isEqualTo(SagaStatus.FAILED);
        verify(sagaInstanceRepository).save(saga);
        verify(outboxEventRepository, never()).save(any());
    }

    @Test
    void deveAtualizarSagaParaFailedAoProcessarDebitoRevertido() {
        SagaInstance saga = criarSagaNoEstado(SagaStatus.DEBITED);
        when(sagaInstanceRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

        orchestrator.processarDebitoRevertido(new DebitReversed(saga.getId(), saga.getAccountOrigemId(), Instant.now()));

        assertThat(saga.getStatus()).isEqualTo(SagaStatus.FAILED);
        verify(sagaInstanceRepository).save(saga);
        verify(outboxEventRepository, never()).save(any());
    }

    @Test
    void deveFalharDiretamenteSemCompensacaoAoProcessarTimeoutEmStarted() {
        SagaInstance saga = criarSagaNoEstado(SagaStatus.STARTED);
        when(sagaInstanceRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

        orchestrator.processarTimeout(saga.getId());

        assertThat(saga.getStatus()).isEqualTo(SagaStatus.FAILED);
        assertThat(saga.getFailureReason()).isEqualTo("SAGA_TIMEOUT");
        verify(outboxEventRepository, never()).save(any());
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
    void deveRetornarSagaExistenteEmVezDeCriarNovaQuandoIdempotencyKeyJaFoiUsada() {
        SagaInstance sagaExistente = criarSagaNoEstado(SagaStatus.STARTED);
        when(sagaInstanceRepository.findByIdempotencyKey("chave-123")).thenReturn(Optional.of(sagaExistente));

        UUID transferId = orchestrator.iniciarTransferencia(UUID.randomUUID(), UUID.randomUUID(),
                new BigDecimal("300.00"), "chave-123");

        assertThat(transferId).isEqualTo(sagaExistente.getId());
        verify(sagaInstanceRepository, never()).saveAndFlush(any());
        verify(outboxEventRepository, never()).save(any());
    }

    @Test
    void deveCriarSagaComIdempotencyKeyQuandoAindaNaoExiste() {
        when(sagaInstanceRepository.findByIdempotencyKey("chave-nova")).thenReturn(Optional.empty());

        UUID accountOrigemId = UUID.randomUUID();
        UUID accountDestinoId = UUID.randomUUID();
        UUID transferId = orchestrator.iniciarTransferencia(accountOrigemId, accountDestinoId,
                new BigDecimal("300.00"), "chave-nova");

        ArgumentCaptor<SagaInstance> sagaCaptor = ArgumentCaptor.forClass(SagaInstance.class);
        verify(sagaInstanceRepository).saveAndFlush(sagaCaptor.capture());
        assertThat(sagaCaptor.getValue().getId()).isEqualTo(transferId);
        assertThat(sagaCaptor.getValue().getIdempotencyKey()).isEqualTo("chave-nova");
        verify(outboxEventRepository).save(any());
    }
}

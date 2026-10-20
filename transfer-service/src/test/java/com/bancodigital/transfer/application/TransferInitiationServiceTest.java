package com.bancodigital.transfer.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bancodigital.transfer.domain.InsufficientBalanceException;
import com.bancodigital.transfer.domain.OutboxEvent;
import com.bancodigital.transfer.domain.SagaInstance;
import com.bancodigital.transfer.domain.SagaStatus;
import com.bancodigital.transfer.infrastructure.client.AccountServiceClient;
import com.bancodigital.transfer.infrastructure.client.BalanceResponse;
import com.bancodigital.transfer.infrastructure.observability.SagaObservability;
import com.bancodigital.transfer.infrastructure.persistence.OutboxEventRepository;
import com.bancodigital.transfer.infrastructure.persistence.SagaInstanceRepository;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TransferInitiationServiceTest {

    @Mock
    private SagaInstanceRepository sagaInstanceRepository;

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Mock
    private AccountServiceClient accountServiceClient;

    @Mock
    private SagaObservability sagaObservability;

    private TransferInitiationService initiation;

    @BeforeEach
    void setUp() {
        // accountServiceClient não é stubado explicitamente: Mockito já retorna Optional.empty() por
        // padrão, o que reproduz o comportamento de fallback (pular a pré-checagem de saldo) sem
        // precisar simular o circuito aqui — isso é coberto por TransferServiceCircuitBreakerTest.
        initiation = new TransferInitiationService(sagaInstanceRepository, accountServiceClient,
                new SagaOutboxPublisher(outboxEventRepository, sagaObservability));
    }
    @Test
    void deveIniciarTransferenciaCriandoSagaEPublicandoTransferRequested() {
        UUID accountOrigemId = UUID.randomUUID();
        UUID accountDestinoId = UUID.randomUUID();

        UUID transferId = initiation.iniciarTransferencia(accountOrigemId, accountDestinoId,
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

        UUID transferId = initiation.iniciarTransferencia(accountOrigemId, UUID.randomUUID(),
                new BigDecimal("300.00"), null);

        verify(sagaInstanceRepository).saveAndFlush(any());
        assertThat(transferId).isNotNull();
    }

    @Test
    void deveRecusarComSaldoInsuficienteSemCriarSagaQuandoConsultaDeSaldoRespondeuComSucesso() {
        UUID accountOrigemId = UUID.randomUUID();
        when(accountServiceClient.consultarSaldo(accountOrigemId))
                .thenReturn(Optional.of(new BalanceResponse(accountOrigemId, new BigDecimal("10.00"))));

        assertThatThrownBy(() -> initiation.iniciarTransferencia(accountOrigemId, UUID.randomUUID(),
                new BigDecimal("300.00"), null))
                .isInstanceOf(InsufficientBalanceException.class);

        verify(sagaInstanceRepository, never()).saveAndFlush(any());
        verify(outboxEventRepository, never()).save(any());
    }

    @Test
    void deveRetornarSagaExistenteEmVezDeCriarNovaQuandoIdempotencyKeyJaFoiUsada() {
        SagaInstance sagaExistente = SagaInstance.iniciar(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                new BigDecimal("300.00"));
        when(sagaInstanceRepository.findByIdempotencyKey("chave-123")).thenReturn(Optional.of(sagaExistente));

        UUID transferId = initiation.iniciarTransferencia(UUID.randomUUID(), UUID.randomUUID(),
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
        UUID transferId = initiation.iniciarTransferencia(accountOrigemId, accountDestinoId,
                new BigDecimal("300.00"), "chave-nova");

        ArgumentCaptor<SagaInstance> sagaCaptor = ArgumentCaptor.forClass(SagaInstance.class);
        verify(sagaInstanceRepository).saveAndFlush(sagaCaptor.capture());
        assertThat(sagaCaptor.getValue().getId()).isEqualTo(transferId);
        assertThat(sagaCaptor.getValue().getIdempotencyKey()).isEqualTo("chave-nova");
        verify(outboxEventRepository).save(any());
    }
}

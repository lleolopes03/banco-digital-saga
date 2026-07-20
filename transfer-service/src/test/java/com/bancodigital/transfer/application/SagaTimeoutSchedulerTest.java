package com.bancodigital.transfer.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bancodigital.transfer.domain.SagaInstance;
import com.bancodigital.transfer.domain.SagaStatus;
import com.bancodigital.transfer.infrastructure.persistence.SagaInstanceRepository;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SagaTimeoutSchedulerTest {

    @Mock
    private SagaInstanceRepository sagaInstanceRepository;

    @Mock
    private SagaOrchestratorService sagaOrchestratorService;

    private SagaTimeoutScheduler scheduler;

    private SagaInstance criarSagaComUpdatedAt(Instant updatedAt) throws Exception {
        SagaInstance saga = SagaInstance.iniciar(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                new BigDecimal("100.00"));
        // updatedAt só é alterado internamente pela própria SagaInstance (aplicarTransicao /
        // registrarMotivoFalha), então para simular uma saga "travada há X tempo" em teste
        // unitário (sem esperar o tempo real passar) é preciso sobrescrever o campo diretamente.
        Field field = SagaInstance.class.getDeclaredField("updatedAt");
        field.setAccessible(true);
        field.set(saga, updatedAt);
        return saga;
    }

    @Test
    void deveCompensarSagaComUpdatedAtMaisAntigoQueOTimeout() throws Exception {
        scheduler = new SagaTimeoutScheduler(sagaInstanceRepository, sagaOrchestratorService);
        SagaInstance sagaTravada = criarSagaComUpdatedAt(Instant.now().minus(5, ChronoUnit.MINUTES));
        when(sagaInstanceRepository.findByStatusIn(anyList())).thenReturn(List.of(sagaTravada));

        scheduler.detectarECompensarSagasTravadas();

        verify(sagaOrchestratorService).processarTimeout(sagaTravada.getId());
    }

    @Test
    void naoDeveCompensarSagaAtualizadaRecentemente() throws Exception {
        scheduler = new SagaTimeoutScheduler(sagaInstanceRepository, sagaOrchestratorService);
        SagaInstance sagaRecente = criarSagaComUpdatedAt(Instant.now());
        when(sagaInstanceRepository.findByStatusIn(anyList())).thenReturn(List.of(sagaRecente));

        scheduler.detectarECompensarSagasTravadas();

        verify(sagaOrchestratorService, never()).processarTimeout(sagaRecente.getId());
    }

    @Test
    void deveConsultarApenasStatusNaoTerminais() {
        scheduler = new SagaTimeoutScheduler(sagaInstanceRepository, sagaOrchestratorService);
        when(sagaInstanceRepository.findByStatusIn(anyList())).thenReturn(List.of());

        scheduler.detectarECompensarSagasTravadas();

        ArgumentCaptor<List<SagaStatus>> captor = ArgumentCaptor.forClass(List.class);
        verify(sagaInstanceRepository).findByStatusIn(captor.capture());
        assertThat(captor.getValue()).doesNotContain(SagaStatus.COMPLETED, SagaStatus.FAILED);
        assertThat(captor.getValue()).contains(SagaStatus.STARTED, SagaStatus.RESERVED,
                SagaStatus.ANTIFRAUD_PENDING, SagaStatus.ANTIFRAUD_APPROVED, SagaStatus.DEBITED);
    }

    @Test
    void falhaAoCompensarUmaSagaNaoDeveImpedirCompensacaoDasDemais() throws Exception {
        scheduler = new SagaTimeoutScheduler(sagaInstanceRepository, sagaOrchestratorService);
        SagaInstance sagaComErro = criarSagaComUpdatedAt(Instant.now().minus(5, ChronoUnit.MINUTES));
        SagaInstance sagaOk = criarSagaComUpdatedAt(Instant.now().minus(5, ChronoUnit.MINUTES));
        when(sagaInstanceRepository.findByStatusIn(anyList())).thenReturn(List.of(sagaComErro, sagaOk));
        org.mockito.Mockito.doThrow(new RuntimeException("falha simulada"))
                .when(sagaOrchestratorService).processarTimeout(sagaComErro.getId());

        scheduler.detectarECompensarSagasTravadas();

        verify(sagaOrchestratorService).processarTimeout(sagaComErro.getId());
        verify(sagaOrchestratorService).processarTimeout(sagaOk.getId());
    }
}

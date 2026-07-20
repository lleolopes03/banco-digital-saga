package com.bancodigital.transfer.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.bancodigital.transfer.AbstractIntegrationTest;
import com.bancodigital.transfer.domain.SagaEvent;
import com.bancodigital.transfer.domain.SagaInstance;
import com.bancodigital.transfer.domain.SagaStateMachine;
import com.bancodigital.transfer.domain.SagaStatus;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class SagaInstanceRepositoryIT extends AbstractIntegrationTest {

    @Autowired
    private SagaInstanceRepository sagaInstanceRepository;

    @Test
    void devePersistirERecuperarSagaInstancePorId() {
        SagaInstance saga = SagaInstance.iniciar(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                new BigDecimal("250.00"));

        sagaInstanceRepository.saveAndFlush(saga);

        Optional<SagaInstance> encontrada = sagaInstanceRepository.findById(saga.getId());
        assertThat(encontrada).isPresent();
        assertThat(encontrada.get().getStatus()).isEqualTo(SagaStatus.STARTED);
        assertThat(encontrada.get().getValor()).isEqualByComparingTo("250.00");
        assertThat(encontrada.get().getAccountOrigemId()).isEqualTo(saga.getAccountOrigemId());
    }

    @Test
    void deveBuscarSagasPorListaDeStatus() {
        SagaInstance emAndamento = SagaInstance.iniciar(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                new BigDecimal("50.00"));

        SagaInstance falha = SagaInstance.iniciar(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                new BigDecimal("75.00"));
        SagaStateMachine stateMachine = new SagaStateMachine();
        stateMachine.transicionar(falha, SagaEvent.RESERVAR_SALDO);
        stateMachine.transicionar(falha, SagaEvent.FALHAR);

        sagaInstanceRepository.saveAndFlush(emAndamento);
        sagaInstanceRepository.saveAndFlush(falha);

        List<SagaInstance> resultado = sagaInstanceRepository.findByStatusIn(List.of(SagaStatus.FAILED));

        assertThat(resultado).extracting(SagaInstance::getId).contains(falha.getId());
        assertThat(resultado).extracting(SagaInstance::getId).doesNotContain(emAndamento.getId());
    }
}

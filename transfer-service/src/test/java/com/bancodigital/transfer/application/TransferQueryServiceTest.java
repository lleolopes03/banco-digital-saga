package com.bancodigital.transfer.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.bancodigital.transfer.domain.SagaInstance;
import com.bancodigital.transfer.domain.SagaInstanceNotFoundException;
import com.bancodigital.transfer.infrastructure.persistence.SagaInstanceRepository;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TransferQueryServiceTest {

    @Mock
    private SagaInstanceRepository sagaInstanceRepository;

    @InjectMocks
    private TransferQueryService queryService;

    @Test
    void deveLancarExcecaoAoConsultarSagaInexistente() {
        UUID transferId = UUID.randomUUID();
        when(sagaInstanceRepository.findById(transferId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> queryService.consultarSaga(transferId))
                .isInstanceOf(SagaInstanceNotFoundException.class);
    }

    @Test
    void deveRetornarASagaExistente() {
        SagaInstance saga = SagaInstance.iniciar(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                new BigDecimal("10.00"));
        when(sagaInstanceRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

        assertThat(queryService.consultarSaga(saga.getId())).isSameAs(saga);
    }
}

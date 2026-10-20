package com.bancodigital.transfer.application;

import com.bancodigital.transfer.domain.SagaInstance;
import com.bancodigital.transfer.domain.SagaInstanceNotFoundException;
import com.bancodigital.transfer.infrastructure.persistence.SagaInstanceRepository;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Consulta o estado de uma transferência (a saga). Só leitura. */
@Service
public class TransferQueryService {

    private final SagaInstanceRepository sagaInstanceRepository;

    public TransferQueryService(SagaInstanceRepository sagaInstanceRepository) {
        this.sagaInstanceRepository = sagaInstanceRepository;
    }

    @Transactional(readOnly = true)
    public SagaInstance consultarSaga(UUID transferId) {
        return sagaInstanceRepository.findById(transferId)
                .orElseThrow(() -> new SagaInstanceNotFoundException(transferId));
    }
}

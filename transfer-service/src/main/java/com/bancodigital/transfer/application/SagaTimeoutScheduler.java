package com.bancodigital.transfer.application;

import com.bancodigital.transfer.domain.SagaInstance;
import com.bancodigital.transfer.domain.SagaStatus;
import com.bancodigital.transfer.infrastructure.persistence.SagaInstanceRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/*
 * Cenário 3 de compensação: detecta sagas travadas em um estado intermediário (nenhum evento
 * chegou dentro do tempo limite) e dispara a compensação apropriada. Roda a cada 30s e considera
 * travada qualquer saga não-terminal cujo updatedAt tenha mais de 2 minutos — o mesmo sinal que
 * SagaInstance.aplicarTransicao()/registrarMotivoFalha() já atualizam a cada progresso real da
 * saga, então "updatedAt parado" é exatamente "nenhum evento chegou".
 */
@Component
public class SagaTimeoutScheduler {

    private static final Logger log = LoggerFactory.getLogger(SagaTimeoutScheduler.class);
    private static final Duration TIMEOUT = Duration.ofMinutes(2);
    private static final Set<SagaStatus> STATUS_NAO_TERMINAIS =
            EnumSet.complementOf(EnumSet.of(SagaStatus.COMPLETED, SagaStatus.FAILED));

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaOrchestratorService sagaOrchestratorService;

    public SagaTimeoutScheduler(SagaInstanceRepository sagaInstanceRepository,
                                 SagaOrchestratorService sagaOrchestratorService) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.sagaOrchestratorService = sagaOrchestratorService;
    }

    @Scheduled(fixedDelay = 30_000)
    public void detectarECompensarSagasTravadas() {
        Instant limite = Instant.now().minus(TIMEOUT);
        List<SagaInstance> sagas = sagaInstanceRepository.findByStatusIn(List.copyOf(STATUS_NAO_TERMINAIS));

        for (SagaInstance saga : sagas) {
            if (saga.getUpdatedAt().isBefore(limite)) {
                try {
                    sagaOrchestratorService.processarTimeout(saga.getId());
                } catch (Exception ex) {
                    log.error("Falha ao compensar saga {} travada por timeout (status={}), tentando novamente no " +
                            "próximo ciclo", saga.getId(), saga.getStatus(), ex);
                }
            }
        }
    }
}

package com.bancodigital.transfer.application;

import com.bancodigital.transfer.domain.InsufficientBalanceException;
import com.bancodigital.transfer.domain.SagaInstance;
import com.bancodigital.transfer.infrastructure.client.AccountServiceClient;
import com.bancodigital.transfer.infrastructure.client.BalanceResponse;
import com.bancodigital.transfer.infrastructure.persistence.SagaInstanceRepository;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Porta de entrada de uma transferência: aplica a idempotência (Idempotency-Key), faz a pré-checagem
 * síncrona de saldo e abre a saga publicando {@code TransferRequested}. A condução dos passos
 * seguintes é do {@link SagaOrchestratorService}.
 */
@Service
public class TransferInitiationService {

    private final SagaInstanceRepository sagaInstanceRepository;
    private final AccountServiceClient accountServiceClient;
    private final SagaOutboxPublisher outboxPublisher;

    public TransferInitiationService(SagaInstanceRepository sagaInstanceRepository,
                                      AccountServiceClient accountServiceClient,
                                      SagaOutboxPublisher outboxPublisher) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.accountServiceClient = accountServiceClient;
        this.outboxPublisher = outboxPublisher;
    }

    @Transactional
    public UUID iniciarTransferencia(UUID accountOrigemId, UUID accountDestinoId, BigDecimal valor,
                                      String idempotencyKey) {
        boolean temIdempotencyKey = idempotencyKey != null && !idempotencyKey.isBlank();
        if (temIdempotencyKey) {
            Optional<SagaInstance> existente = sagaInstanceRepository.findByIdempotencyKey(idempotencyKey);
            if (existente.isPresent()) {
                return existente.get().getId();
            }
        }

        validarSaldoPrevio(accountOrigemId, valor);

        SagaInstance saga = SagaInstance.iniciar(UUID.randomUUID(), accountOrigemId, accountDestinoId, valor,
                temIdempotencyKey ? idempotencyKey : null);

        try {
            // saveAndFlush: a violação do índice único de idempotency_key precisa estourar aqui, não no commit.
            sagaInstanceRepository.saveAndFlush(saga);
        } catch (DataIntegrityViolationException ex) {
            if (!temIdempotencyKey) {
                throw ex;
            }
            // Corrida entre duas requisições com a mesma Idempotency-Key: devolve a saga que venceu.
            return sagaInstanceRepository.findByIdempotencyKey(idempotencyKey)
                    .map(SagaInstance::getId)
                    .orElseThrow(() -> ex);
        }

        outboxPublisher.transferenciaSolicitada(saga);
        return saga.getId();
    }

    /*
     * Best-effort: evita abrir uma saga para um saldo obviamente insuficiente. Se o account-service estiver
     * indisponível a checagem é pulada; a validação definitiva é a reserva assíncrona da saga.
     */
    private void validarSaldoPrevio(UUID accountOrigemId, BigDecimal valor) {
        Optional<BalanceResponse> saldo = accountServiceClient.consultarSaldo(accountOrigemId);
        if (saldo.isPresent() && saldo.get().balance().compareTo(valor) < 0) {
            throw new InsufficientBalanceException(accountOrigemId, valor, saldo.get().balance());
        }
    }
}

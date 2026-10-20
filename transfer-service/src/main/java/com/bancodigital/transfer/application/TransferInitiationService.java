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
            // saveAndFlush (não save) para que uma violação do índice único parcial de
            // idempotency_key seja lançada já aqui dentro, e não só no commit da transação —
            // só assim dá pra capturá-la e cair no caminho de corrida abaixo.
            sagaInstanceRepository.saveAndFlush(saga);
        } catch (DataIntegrityViolationException ex) {
            if (!temIdempotencyKey) {
                throw ex;
            }
            // Corrida rara: duas requisições concorrentes com a mesma Idempotency-Key. O índice
            // único garantiu que só uma delas persistiu; devolvemos a saga que realmente venceu em
            // vez de propagar o erro.
            return sagaInstanceRepository.findByIdempotencyKey(idempotencyKey)
                    .map(SagaInstance::getId)
                    .orElseThrow(() -> ex);
        }

        outboxPublisher.transferenciaSolicitada(saga);
        return saga.getId();
    }

    /*
     * Validação síncrona e best-effort: evita abrir uma saga inteira para um caso óbvio de saldo
     * insuficiente. Se o account-service estiver indisponível, AccountServiceClient degrada
     * graciosamente (Optional vazio) e esta checagem é simplesmente pulada — a validação definitiva
     * continua sendo a reserva assíncrona feita mais adiante na saga.
     */
    private void validarSaldoPrevio(UUID accountOrigemId, BigDecimal valor) {
        Optional<BalanceResponse> saldo = accountServiceClient.consultarSaldo(accountOrigemId);
        if (saldo.isPresent() && saldo.get().balance().compareTo(valor) < 0) {
            throw new InsufficientBalanceException(accountOrigemId, valor, saldo.get().balance());
        }
    }
}

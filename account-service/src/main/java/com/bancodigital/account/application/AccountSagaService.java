package com.bancodigital.account.application;

import com.bancodigital.account.domain.Account;
import com.bancodigital.account.domain.AccountNotFoundException;
import com.bancodigital.account.domain.AccountStatus;
import com.bancodigital.account.domain.SagaStepConflictException;
import com.bancodigital.account.domain.TransactionLogEntry;
import com.bancodigital.account.domain.event.CreditConfirmed;
import com.bancodigital.account.domain.event.CreditFailed;
import com.bancodigital.account.domain.event.CreditRequested;
import com.bancodigital.account.domain.event.DebitConfirmationRequested;
import com.bancodigital.account.domain.event.DebitConfirmed;
import com.bancodigital.account.domain.event.DebitReversed;
import com.bancodigital.account.domain.event.ReleaseReservedAmount;
import com.bancodigital.account.domain.event.ReservationReleased;
import com.bancodigital.account.domain.event.ReverseDebitRequested;
import com.bancodigital.account.domain.event.TransferRequested;
import com.bancodigital.account.domain.event.TransferReservationFailed;
import com.bancodigital.account.domain.event.TransferReserved;
import com.bancodigital.account.infrastructure.persistence.AccountRepository;
import com.bancodigital.account.infrastructure.persistence.TransactionLogRepository;
import java.time.Instant;
import java.util.UUID;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.retry.annotation.Recover;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Os passos da saga do lado das contas: reservar, debitar, creditar e as duas compensações. Cada passo é uma
 * transação que altera a conta, registra o extrato (quando há) e grava o evento de resposta no Outbox.
 *
 * Concorrência por lock otimista com retry ({@link RetryOnOptimisticLock}); esgotadas as tentativas, o
 * {@code @Recover} lança {@link SagaStepConflictException} e a mensagem é reentregue. A transação é programática
 * porque @Retryable e @Transactional no mesmo método dependem da ordem dos proxies AOP. O cache de saldo só é
 * invalidado depois do commit: antes, uma leitura concorrente repopularia o cache com o saldo antigo.
 */
@Service
public class AccountSagaService {

    private static final String SALDO_INSUFICIENTE = "SALDO_INSUFICIENTE";

    private final AccountRepository accountRepository;
    private final TransactionLogRepository transactionLogRepository;
    private final OutboxEventWriter outboxEventWriter;
    private final BalanceCache balanceCache;
    private final TransactionTemplate transactionTemplate;

    public AccountSagaService(AccountRepository accountRepository, TransactionLogRepository transactionLogRepository,
                               OutboxEventWriter outboxEventWriter, BalanceCache balanceCache,
                               PlatformTransactionManager transactionManager) {
        this.accountRepository = accountRepository;
        this.transactionLogRepository = transactionLogRepository;
        this.outboxEventWriter = outboxEventWriter;
        this.balanceCache = balanceCache;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    /** Primeiro passo da saga. Saldo insuficiente é uma recusa de negócio (TransferReservationFailed), não uma exceção. */
    @RetryOnOptimisticLock
    public void reservarSaldo(TransferRequested event) {
        executarPasso(event.accountOrigemId(), () -> {
            Account account = buscarConta(event.accountOrigemId());
            boolean reservado = account.reservar(event.valor());
            accountRepository.saveAndFlush(account);

            Object resposta = reservado ? reservaConfirmada(event) : reservaRejeitada(event);
            outboxEventWriter.gravarParaTransferencia(event.transferId(), resposta);
        });
    }

    @Recover
    public void recuperarReservaDeFalhaDeConcorrencia(OptimisticLockingFailureException ex, TransferRequested event) {
        throw new SagaStepConflictException("reservarSaldo", event.accountOrigemId());
    }

    @RetryOnOptimisticLock
    public void confirmarDebito(DebitConfirmationRequested event) {
        executarPasso(event.accountOrigemId(), () -> {
            Account account = buscarConta(event.accountOrigemId());
            account.confirmarDebito(event.valor());
            Account updated = accountRepository.saveAndFlush(account);

            transactionLogRepository.save(TransactionLogEntry.transferenciaEnviada(event.transferId(), event.accountOrigemId(),
                    event.valor(), updated.getBalance(), "Transferência enviada"));
            outboxEventWriter.gravarParaTransferencia(event.transferId(),
                    new DebitConfirmed(event.transferId(), event.accountOrigemId(), event.valor(), Instant.now()));
        });
    }

    @Recover
    public void recuperarDebitoDeFalhaDeConcorrencia(OptimisticLockingFailureException ex,
                                                       DebitConfirmationRequested event) {
        throw new SagaStepConflictException("confirmarDebito", event.accountOrigemId());
    }

    /** Conta destino bloqueada/fechada é uma recusa de negócio (CreditFailed), que dispara a reversão do débito. */
    @RetryOnOptimisticLock
    public void creditarPorTransferencia(CreditRequested event) {
        executarPasso(event.accountDestinoId(), () -> {
            Account account = buscarConta(event.accountDestinoId());
            if (account.getStatus() != AccountStatus.ACTIVE) {
                outboxEventWriter.gravarParaTransferencia(event.transferId(), creditoRecusado(event, account));
                return;
            }

            account.depositar(event.valor());
            Account updated = accountRepository.saveAndFlush(account);

            transactionLogRepository.save(TransactionLogEntry.transferenciaRecebida(event.transferId(),
                    event.accountDestinoId(), event.valor(), updated.getBalance(), "Transferência recebida"));
            outboxEventWriter.gravarParaTransferencia(event.transferId(),
                    new CreditConfirmed(event.transferId(), event.accountDestinoId(), event.valor(), Instant.now()));
        });
    }

    @Recover
    public void recuperarCreditoDeFalhaDeConcorrencia(OptimisticLockingFailureException ex, CreditRequested event) {
        throw new SagaStepConflictException("creditarPorTransferencia", event.accountDestinoId());
    }

    /** Compensação 1: devolve ao saldo o valor só reservado. É este passo que deixa o rastro no extrato. */
    @RetryOnOptimisticLock
    public void liberarReserva(ReleaseReservedAmount event) {
        executarPasso(event.accountOrigemId(), () -> {
            Account account = buscarConta(event.accountOrigemId());
            account.liberarReserva(event.valor());
            Account updated = accountRepository.saveAndFlush(account);

            transactionLogRepository.save(TransactionLogEntry.estorno(event.transferId(), event.accountOrigemId(),
                    event.valor(), updated.getBalance(), "Estorno de reserva: transferência não concluída"));
            outboxEventWriter.gravarParaTransferencia(event.transferId(),
                    new ReservationReleased(event.transferId(), Instant.now()));
        });
    }

    @Recover
    public void recuperarLiberacaoDeFalhaDeConcorrencia(OptimisticLockingFailureException ex,
                                                          ReleaseReservedAmount event) {
        throw new SagaStepConflictException("liberarReserva", event.accountOrigemId());
    }

    /** Compensação 2: o débito já estava confirmado, então devolver o dinheiro equivale a um depósito. */
    @RetryOnOptimisticLock
    public void reverterDebito(ReverseDebitRequested event) {
        executarPasso(event.accountOrigemId(), () -> {
            Account account = buscarConta(event.accountOrigemId());
            account.depositar(event.valor());
            Account updated = accountRepository.saveAndFlush(account);

            transactionLogRepository.save(TransactionLogEntry.estorno(event.transferId(), event.accountOrigemId(),
                    event.valor(), updated.getBalance(), "Estorno de débito: crédito ao destinatário falhou"));
            outboxEventWriter.gravarParaTransferencia(event.transferId(),
                    new DebitReversed(event.transferId(), event.accountOrigemId(), Instant.now()));
        });
    }

    @Recover
    public void recuperarReversaoDeFalhaDeConcorrencia(OptimisticLockingFailureException ex,
                                                         ReverseDebitRequested event) {
        throw new SagaStepConflictException("reverterDebito", event.accountOrigemId());
    }

    /** Roda o passo numa transação e só então invalida o cache de saldo da conta afetada. */
    private void executarPasso(UUID contaAfetada, Runnable passo) {
        transactionTemplate.executeWithoutResult(status -> passo.run());
        balanceCache.invalidar(contaAfetada);
    }

    private Account buscarConta(UUID accountId) {
        return accountRepository.findById(accountId).orElseThrow(() -> new AccountNotFoundException(accountId));
    }

    private static TransferReserved reservaConfirmada(TransferRequested event) {
        return new TransferReserved(event.transferId(), event.accountOrigemId(), event.accountDestinoId(),
                event.valor(), Instant.now());
    }

    private static TransferReservationFailed reservaRejeitada(TransferRequested event) {
        return new TransferReservationFailed(event.transferId(), SALDO_INSUFICIENTE, Instant.now());
    }

    private static CreditFailed creditoRecusado(CreditRequested event, Account contaDestino) {
        return new CreditFailed(event.transferId(), event.accountDestinoId(),
                "CONTA_DESTINO_" + contaDestino.getStatus(), Instant.now());
    }
}

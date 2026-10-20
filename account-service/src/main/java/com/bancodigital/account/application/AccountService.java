package com.bancodigital.account.application;

import com.bancodigital.account.domain.Account;
import com.bancodigital.account.domain.AccountNotFoundException;
import com.bancodigital.account.domain.ConcurrentDepositConflictException;
import com.bancodigital.account.domain.DuplicateOwnerDocumentException;
import com.bancodigital.account.domain.TransactionLogEntry;
import com.bancodigital.account.domain.event.DepositCompleted;
import com.bancodigital.account.infrastructure.persistence.AccountRepository;
import com.bancodigital.account.infrastructure.persistence.TransactionLogRepository;
import com.bancodigital.account.web.dto.AccountResponse;
import com.bancodigital.account.web.dto.BalanceResponse;
import com.bancodigital.account.web.dto.CreateAccountRequest;
import com.bancodigital.account.web.dto.DepositRequest;
import com.bancodigital.account.web.dto.PageResponse;
import com.bancodigital.account.web.dto.TransactionResponse;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.UUID;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.retry.annotation.Recover;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Casos de uso de conta voltados ao cliente (HTTP): abrir conta, consultar saldo e extrato e
 * depositar. Os passos da saga de transferência ficam em {@link AccountSagaService}.
 */
@Service
public class AccountService {

    private static final Instant EXTRATO_INICIO_PADRAO = Instant.EPOCH;
    private static final Instant EXTRATO_FIM_PADRAO = Instant.parse("9999-12-31T23:59:59.999999999Z");

    private final AccountRepository accountRepository;
    private final TransactionLogRepository transactionLogRepository;
    private final OutboxEventWriter outboxEventWriter;
    private final BalanceCache balanceCache;
    private final TransactionTemplate transactionTemplate;

    public AccountService(AccountRepository accountRepository, TransactionLogRepository transactionLogRepository,
                           OutboxEventWriter outboxEventWriter, BalanceCache balanceCache,
                           PlatformTransactionManager transactionManager) {
        this.accountRepository = accountRepository;
        this.transactionLogRepository = transactionLogRepository;
        this.outboxEventWriter = outboxEventWriter;
        this.balanceCache = balanceCache;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @Transactional
    public AccountResponse criarConta(CreateAccountRequest request) {
        accountRepository.findByOwnerDocument(request.ownerDocument()).ifPresent(existing -> {
            throw new DuplicateOwnerDocumentException(request.ownerDocument());
        });

        Account saved = accountRepository.save(Account.open(request.ownerName(), request.ownerDocument()));
        return AccountResponse.from(saved);
    }

    /*
     * Cache-aside: a consulta de saldo é a leitura mais frequente do sistema, então evita bater no
     * Postgres a cada chamada. Em caso de miss (ou de o cache estar fora do ar) consulta o banco e
     * grava no cache antes de retornar.
     */
    @Transactional(readOnly = true)
    public BalanceResponse consultarSaldo(UUID accountId) {
        return balanceCache.buscar(accountId)
                .map(saldo -> new BalanceResponse(accountId, saldo))
                .orElseGet(() -> consultarSaldoNoBancoEPopularCache(accountId));
    }

    private BalanceResponse consultarSaldoNoBancoEPopularCache(UUID accountId) {
        BalanceResponse response = BalanceResponse.from(buscarPorId(accountId));
        balanceCache.guardar(accountId, response.balance());
        return response;
    }

    @Transactional(readOnly = true)
    public Account buscarPorId(UUID accountId) {
        return accountRepository.findById(accountId)
                .orElseThrow(() -> new AccountNotFoundException(accountId));
    }

    /*
     * Concorrência por lock otimista (@Version em Account) + retry, não lock pessimista: depósitos
     * são operações curtas e o conflito é raro; um SELECT ... FOR UPDATE serializaria até escritas
     * em contas diferentes e derrubaria o throughput sem necessidade.
     *
     * Saldo, lançamento no extrato e evento de saída (Outbox) são gravados na mesma transação
     * programática (TransactionTemplate): @Retryable e @Transactional no mesmo método dependem da
     * ordem de composição dos proxies AOP, que não é garantida. O cache só é invalidado depois do
     * commit — invalidar antes deixaria uma leitura concorrente repopular o cache com o saldo antigo.
     */
    @RetryOnOptimisticLock
    public AccountResponse depositar(UUID accountId, DepositRequest request) {
        UUID transactionId = UUID.randomUUID();

        Account saved = transactionTemplate.execute(status -> {
            Account account = buscarPorId(accountId);
            account.deposit(request.valor());
            Account updated = accountRepository.saveAndFlush(account);

            transactionLogRepository.save(TransactionLogEntry.deposit(transactionId, accountId, request.valor(),
                    updated.getBalance(), request.description()));
            outboxEventWriter.gravarParaConta(accountId,
                    new DepositCompleted(transactionId, accountId, request.valor(), Instant.now()));
            return updated;
        });

        balanceCache.invalidar(accountId);
        return AccountResponse.from(saved);
    }

    @Recover
    public AccountResponse recuperarDeFalhaDeConcorrencia(OptimisticLockingFailureException ex, UUID accountId,
                                                            DepositRequest request) {
        throw new ConcurrentDepositConflictException(accountId);
    }

    @Transactional(readOnly = true)
    public PageResponse<TransactionResponse> consultarExtrato(UUID accountId, LocalDate de, LocalDate ate,
                                                                Pageable pageable) {
        buscarPorId(accountId);

        // Postgres não consegue inferir o tipo de um bind parameter usado apenas como NULL, então em
        // vez de passar from/to nulos usamos limites-sentinela que cobrem todo o intervalo
        // representável quando de/ate não são informados.
        Instant from = de != null ? de.atStartOfDay(ZoneOffset.UTC).toInstant() : EXTRATO_INICIO_PADRAO;
        Instant to = ate != null ? ate.atTime(23, 59, 59, 999_999_999).toInstant(ZoneOffset.UTC) : EXTRATO_FIM_PADRAO;

        Page<TransactionLogEntry> page = transactionLogRepository
                .findByAccountIdAndCreatedAtBetweenOrderByCreatedAtDesc(accountId, from, to, pageable);
        return PageResponse.from(page, TransactionResponse::from);
    }
}

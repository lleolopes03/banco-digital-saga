package com.bancodigital.account.application;

import com.bancodigital.account.domain.Account;
import com.bancodigital.account.domain.AccountNotFoundException;
import com.bancodigital.account.domain.AccountStatus;
import com.bancodigital.account.domain.ConcurrentDepositConflictException;
import com.bancodigital.account.domain.DuplicateOwnerDocumentException;
import com.bancodigital.account.domain.OutboxEvent;
import com.bancodigital.account.domain.SagaStepConflictException;
import com.bancodigital.account.domain.TransactionLogEntry;
import com.bancodigital.account.domain.event.CreditConfirmed;
import com.bancodigital.account.domain.event.CreditFailed;
import com.bancodigital.account.domain.event.CreditRequested;
import com.bancodigital.account.domain.event.DebitConfirmationRequested;
import com.bancodigital.account.domain.event.DebitConfirmed;
import com.bancodigital.account.domain.event.DebitReversed;
import com.bancodigital.account.domain.event.DepositCompleted;
import com.bancodigital.account.domain.event.ReleaseReservedAmount;
import com.bancodigital.account.domain.event.ReservationReleased;
import com.bancodigital.account.domain.event.ReverseDebitRequested;
import com.bancodigital.account.domain.event.TransferRequested;
import com.bancodigital.account.domain.event.TransferReservationFailed;
import com.bancodigital.account.domain.event.TransferReserved;
import com.bancodigital.account.infrastructure.observability.SagaObservability;
import com.bancodigital.account.infrastructure.persistence.AccountRepository;
import com.bancodigital.account.infrastructure.persistence.OutboxEventRepository;
import com.bancodigital.account.infrastructure.persistence.TransactionLogRepository;
import com.bancodigital.account.web.dto.AccountResponse;
import com.bancodigital.account.web.dto.BalanceResponse;
import com.bancodigital.account.web.dto.CreateAccountRequest;
import com.bancodigital.account.web.dto.DepositRequest;
import com.bancodigital.account.web.dto.PageResponse;
import com.bancodigital.account.web.dto.TransactionResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.UUID;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class AccountService {

    private static final Instant EXTRATO_INICIO_PADRAO = Instant.EPOCH;
    private static final Instant EXTRATO_FIM_PADRAO = Instant.parse("9999-12-31T23:59:59.999999999Z");
    private static final String AGGREGATE_TYPE_ACCOUNT = "Account";
    private static final String AGGREGATE_TYPE_TRANSFER = "Transfer";
    private static final ObjectMapper OUTBOX_PAYLOAD_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());
    private static final String BALANCE_CACHE_KEY_PREFIX = "balance:";
    // TTL curto de propósito: saldo é dinheiro, então prioriza consistência razoável (no máximo
    // 30s de defasagem) sobre o máximo de performance/hit-rate possível do cache.
    private static final Duration BALANCE_CACHE_TTL = Duration.ofSeconds(30);

    private final AccountRepository accountRepository;
    private final TransactionLogRepository transactionLogRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final TransactionTemplate transactionTemplate;
    private final StringRedisTemplate redisTemplate;
    private final SagaObservability sagaObservability;

    public AccountService(AccountRepository accountRepository, TransactionLogRepository transactionLogRepository,
                           OutboxEventRepository outboxEventRepository, PlatformTransactionManager transactionManager,
                           StringRedisTemplate redisTemplate, SagaObservability sagaObservability) {
        this.accountRepository = accountRepository;
        this.transactionLogRepository = transactionLogRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.redisTemplate = redisTemplate;
        this.sagaObservability = sagaObservability;
    }

    @Transactional
    public AccountResponse criarConta(CreateAccountRequest request) {
        accountRepository.findByOwnerDocument(request.ownerDocument()).ifPresent(existing -> {
            throw new DuplicateOwnerDocumentException(request.ownerDocument());
        });

        Account account = Account.open(request.ownerName(), request.ownerDocument());
        Account saved = accountRepository.save(account);
        return AccountResponse.from(saved);
    }

    /*
     * Cache-aside: consulta de saldo é a leitura mais frequente do sistema, então evita bater no
     * Postgres a cada chamada. Em caso de miss (ou de o Redis estar fora do ar — get() retorna
     * null tanto para chave ausente quanto para falha de conexão do StringRedisTemplate, então o
     * caminho de leitura sempre cai de volta no Postgres, nunca quebra), consulta o banco e
     * grava no cache antes de retornar.
     */
    @Transactional(readOnly = true)
    public BalanceResponse consultarSaldo(UUID accountId) {
        String chaveCache = BALANCE_CACHE_KEY_PREFIX + accountId;
        String saldoEmCache = redisTemplate.opsForValue().get(chaveCache);
        if (saldoEmCache != null) {
            return new BalanceResponse(accountId, new BigDecimal(saldoEmCache));
        }

        BalanceResponse response = BalanceResponse.from(buscarPorId(accountId));
        redisTemplate.opsForValue().set(chaveCache, response.balance().toPlainString(), BALANCE_CACHE_TTL);
        return response;
    }

    /*
     * Chamado ao final de toda operação que altera saldo (depósito, reserva, débito, crédito,
     * reversão), depois que a transação já commitou — nunca de dentro do TransactionTemplate.
     * Invalidar antes do commit abriria uma janela onde uma leitura concorrente populariam o
     * cache com o saldo antigo (ainda válido do ponto de vista do Postgres até o commit),
     * deixando esse valor desatualizado preso no cache pelos 30s inteiros do TTL, mesmo depois
     * da nossa transação commitar.
     */
    private void invalidarCacheSaldo(UUID accountId) {
        redisTemplate.delete(BALANCE_CACHE_KEY_PREFIX + accountId);
    }

    @Transactional(readOnly = true)
    public Account buscarPorId(UUID accountId) {
        return accountRepository.findById(accountId)
                .orElseThrow(() -> new AccountNotFoundException(accountId));
    }

    /*
     * Concorrência tratada via lock otimista (@Version em Account) + retry, não lock pessimista:
     * depósitos são operações curtas e o conflito (duas escritas na mesma conta ao mesmo tempo)
     * é raro comparado ao volume de leituras/depósitos em contas distintas. Lock pessimista
     * (SELECT ... FOR UPDATE) serializaria até escritas em contas diferentes que competem pela
     * mesma linha de índice/conexão de pool, derrubando o throughput sem necessidade. Com lock
     * otimista, o custo de conflito (uma nova tentativa) só é pago quando o conflito realmente
     * acontece.
     *
     * O saldo, o lançamento no extrato e o evento de saída (outbox) são gravados via
     * TransactionTemplate (transação programática) em vez de @Transactional no próprio método:
     * @Retryable e @Transactional no mesmo método dependem da ordem em que os dois proxies AOP
     * são compostos, que não é garantida. Com TransactionTemplate a transação é aberta e fechada
     * de forma explícita dentro do método já interceptado pelo @Retryable, sem ambiguidade de
     * proxy.
     *
     * Nenhum código de negócio publica direto no Kafka: o evento é gravado como OutboxEvent
     * PENDING na mesma transação do banco que atualiza saldo e extrato (atomicidade garantida
     * pelo próprio commit/rollback da transação), e o OutboxPublisherScheduler é quem
     * efetivamente publica no Kafka de forma assíncrona.
     */
    @Retryable(retryFor = OptimisticLockingFailureException.class, maxAttempts = 3,
            backoff = @Backoff(delay = 20, multiplier = 2))
    public AccountResponse depositar(UUID accountId, DepositRequest request) {
        UUID transactionId = UUID.randomUUID();

        Account saved = transactionTemplate.execute(status -> {
            Account account = buscarPorId(accountId);
            account.deposit(request.valor());
            Account updated = accountRepository.saveAndFlush(account);

            TransactionLogEntry entry = TransactionLogEntry.deposit(transactionId, accountId,
                    request.valor(), updated.getBalance(), request.description());
            transactionLogRepository.save(entry);

            DepositCompleted event = new DepositCompleted(transactionId, accountId, request.valor(), Instant.now());
            OutboxEvent outboxEvent = criarOutboxEvent(AGGREGATE_TYPE_ACCOUNT, accountId, "DepositCompleted", event);
            outboxEventRepository.save(outboxEvent);

            return updated;
        });

        invalidarCacheSaldo(accountId);
        return AccountResponse.from(saved);
    }

    private String serializePayload(Object event) {
        try {
            return OUTBOX_PAYLOAD_MAPPER.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Falha ao serializar payload do outbox event", e);
        }
    }

    /*
     * Captura o traceparent do trace ATIVO neste exato momento (o span aberto pelo consumer que
     * disparou este método de negócio, ou o span HTTP de criarConta/depositar) e grava junto do
     * OutboxEvent — é isso que permite ao OutboxPublisherScheduler, rodando depois numa thread
     * sem relação com esta chamada, propagar o mesmo trace para o próximo hop via Kafka.
     */
    private OutboxEvent criarOutboxEvent(String aggregateType, UUID aggregateId, String eventType, Object payload) {
        return OutboxEvent.pending(UUID.randomUUID(), aggregateType, aggregateId, eventType, serializePayload(payload),
                sagaObservability.traceparentAtual());
    }

    @Recover
    public AccountResponse recuperarDeFalhaDeConcorrencia(OptimisticLockingFailureException ex, UUID accountId,
                                                            DepositRequest request) {
        throw new ConcurrentDepositConflictException(accountId);
    }

    /*
     * Reserva de saldo é sempre o primeiro passo da saga (antes de qualquer checagem de
     * fraude): se não há saldo, não faz sentido gastar uma consulta de antifraude com uma
     * transferência que já não pode acontecer. Mesmo raciocínio de lock otimista + retry do
     * depositar() se aplica aqui, já que é a mesma conta (Account) sendo lida e escrita.
     */
    @Retryable(retryFor = OptimisticLockingFailureException.class, maxAttempts = 3,
            backoff = @Backoff(delay = 20, multiplier = 2))
    public void reservarSaldo(TransferRequested event) {
        transactionTemplate.execute(status -> {
            Account account = buscarPorId(event.accountOrigemId());
            boolean reservado = account.reservar(event.valor());
            accountRepository.saveAndFlush(account);

            OutboxEvent outboxEvent;
            if (reservado) {
                TransferReserved payload = new TransferReserved(event.transferId(), event.accountOrigemId(),
                        event.accountDestinoId(), event.valor(), Instant.now());
                outboxEvent = criarOutboxEvent(AGGREGATE_TYPE_TRANSFER, event.transferId(), "TransferReserved", payload);
            } else {
                TransferReservationFailed payload = new TransferReservationFailed(event.transferId(),
                        "SALDO_INSUFICIENTE", Instant.now());
                outboxEvent = criarOutboxEvent(AGGREGATE_TYPE_TRANSFER, event.transferId(), "TransferReservationFailed",
                        payload);
            }
            outboxEventRepository.save(outboxEvent);
            return null;
        });

        invalidarCacheSaldo(event.accountOrigemId());
    }

    @Recover
    public void recuperarReservaDeFalhaDeConcorrencia(OptimisticLockingFailureException ex, TransferRequested event) {
        throw new SagaStepConflictException("reservarSaldo", event.accountOrigemId());
    }

    @Retryable(retryFor = OptimisticLockingFailureException.class, maxAttempts = 3,
            backoff = @Backoff(delay = 20, multiplier = 2))
    public void confirmarDebito(DebitConfirmationRequested event) {
        transactionTemplate.execute(status -> {
            Account account = buscarPorId(event.accountOrigemId());
            account.confirmarDebito(event.valor());
            Account updated = accountRepository.saveAndFlush(account);

            TransactionLogEntry entry = TransactionLogEntry.transferOut(event.transferId(), event.accountOrigemId(),
                    event.valor(), updated.getBalance(), "Transferência enviada");
            transactionLogRepository.save(entry);

            DebitConfirmed payload = new DebitConfirmed(event.transferId(), event.accountOrigemId(), event.valor(),
                    Instant.now());
            OutboxEvent outboxEvent = criarOutboxEvent(AGGREGATE_TYPE_TRANSFER, event.transferId(), "DebitConfirmed",
                    payload);
            outboxEventRepository.save(outboxEvent);
            return null;
        });

        invalidarCacheSaldo(event.accountOrigemId());
    }

    @Recover
    public void recuperarDebitoDeFalhaDeConcorrencia(OptimisticLockingFailureException ex, DebitConfirmationRequested event) {
        throw new SagaStepConflictException("confirmarDebito", event.accountOrigemId());
    }

    /*
     * Reaproveita Account.deposit(): creditar o destino de uma transferência é, em termos de
     * saldo, a mesma operação que um depósito. O que muda é o tipo de lançamento no extrato
     * (TRANSFER_IN em vez de DEPOSIT) e o evento publicado ao final (CreditConfirmed).
     */
    @Retryable(retryFor = OptimisticLockingFailureException.class, maxAttempts = 3,
            backoff = @Backoff(delay = 20, multiplier = 2))
    public void creditarPorTransferencia(CreditRequested event) {
        transactionTemplate.execute(status -> {
            Account account = buscarPorId(event.accountDestinoId());

            // Cenário 2 de compensação: conta destino bloqueada/fechada não pode receber
            // crédito. Isso é uma recusa de negócio normal (vira CreditFailed, disparando a
            // reversão do débito já confirmado na origem), não uma exceção de programação —
            // mesmo raciocínio de reservar() retornando false em vez de lançar exceção.
            if (account.getStatus() != AccountStatus.ACTIVE) {
                CreditFailed payload = new CreditFailed(event.transferId(), event.accountDestinoId(),
                        "CONTA_DESTINO_" + account.getStatus(), Instant.now());
                OutboxEvent outboxEvent = criarOutboxEvent(AGGREGATE_TYPE_TRANSFER, event.transferId(), "CreditFailed",
                        payload);
                outboxEventRepository.save(outboxEvent);
                return null;
            }

            account.deposit(event.valor());
            Account updated = accountRepository.saveAndFlush(account);

            TransactionLogEntry entry = TransactionLogEntry.transferIn(event.transferId(), event.accountDestinoId(),
                    event.valor(), updated.getBalance(), "Transferência recebida");
            transactionLogRepository.save(entry);

            CreditConfirmed payload = new CreditConfirmed(event.transferId(), event.accountDestinoId(), event.valor(),
                    Instant.now());
            OutboxEvent outboxEvent = criarOutboxEvent(AGGREGATE_TYPE_TRANSFER, event.transferId(), "CreditConfirmed",
                    payload);
            outboxEventRepository.save(outboxEvent);
            return null;
        });

        invalidarCacheSaldo(event.accountDestinoId());
    }

    @Recover
    public void recuperarCreditoDeFalhaDeConcorrencia(OptimisticLockingFailureException ex, CreditRequested event) {
        throw new SagaStepConflictException("creditarPorTransferencia", event.accountDestinoId());
    }

    /*
     * Cenário 1 de compensação: devolve para balance o valor que ainda estava só reservado
     * (rejeição por antifraude ou timeout antes do débito ser confirmado). A reserva original
     * não gera lançamento no extrato (reservarSaldo() só move saldo, sem TransactionLogEntry),
     * então é esta liberação que precisa deixar o rastro — sem ela o dinheiro simplesmente
     * "reaparece" na conta sem nenhuma entrada visível no extrato, violando a regra de que toda
     * compensação é auditável.
     */
    @Retryable(retryFor = OptimisticLockingFailureException.class, maxAttempts = 3,
            backoff = @Backoff(delay = 20, multiplier = 2))
    public void liberarReserva(ReleaseReservedAmount event) {
        transactionTemplate.execute(status -> {
            Account account = buscarPorId(event.accountOrigemId());
            account.liberarReserva(event.valor());
            Account updated = accountRepository.saveAndFlush(account);

            TransactionLogEntry entry = TransactionLogEntry.reversal(event.transferId(), event.accountOrigemId(),
                    event.valor(), updated.getBalance(), "Estorno de reserva: transferência não concluída");
            transactionLogRepository.save(entry);

            ReservationReleased payload = new ReservationReleased(event.transferId(), Instant.now());
            OutboxEvent outboxEvent = criarOutboxEvent(AGGREGATE_TYPE_TRANSFER, event.transferId(),
                    "ReservationReleased", payload);
            outboxEventRepository.save(outboxEvent);
            return null;
        });

        invalidarCacheSaldo(event.accountOrigemId());
    }

    @Recover
    public void recuperarLiberacaoDeFalhaDeConcorrencia(OptimisticLockingFailureException ex, ReleaseReservedAmount event) {
        throw new SagaStepConflictException("liberarReserva", event.accountOrigemId());
    }

    /*
     * Cenário 2 de compensação: o débito na origem já tinha sido confirmado (removido de
     * reservedBalance em confirmarDebito()) quando o crédito no destino falhou, então devolver o
     * dinheiro é literalmente a mesma operação que um depósito — reaproveita Account.deposit(),
     * igual creditarPorTransferencia() já reaproveita para o crédito no destino.
     */
    @Retryable(retryFor = OptimisticLockingFailureException.class, maxAttempts = 3,
            backoff = @Backoff(delay = 20, multiplier = 2))
    public void reverterDebito(ReverseDebitRequested event) {
        transactionTemplate.execute(status -> {
            Account account = buscarPorId(event.accountOrigemId());
            account.deposit(event.valor());
            Account updated = accountRepository.saveAndFlush(account);

            TransactionLogEntry entry = TransactionLogEntry.reversal(event.transferId(), event.accountOrigemId(),
                    event.valor(), updated.getBalance(), "Estorno de débito: crédito ao destinatário falhou");
            transactionLogRepository.save(entry);

            DebitReversed payload = new DebitReversed(event.transferId(), event.accountOrigemId(), Instant.now());
            OutboxEvent outboxEvent = criarOutboxEvent(AGGREGATE_TYPE_TRANSFER, event.transferId(), "DebitReversed",
                    payload);
            outboxEventRepository.save(outboxEvent);
            return null;
        });

        invalidarCacheSaldo(event.accountOrigemId());
    }

    @Recover
    public void recuperarReversaoDeFalhaDeConcorrencia(OptimisticLockingFailureException ex, ReverseDebitRequested event) {
        throw new SagaStepConflictException("reverterDebito", event.accountOrigemId());
    }

    @Transactional(readOnly = true)
    public PageResponse<TransactionResponse> consultarExtrato(UUID accountId, LocalDate de, LocalDate ate,
                                                                Pageable pageable) {
        buscarPorId(accountId);

        // Postgres não consegue inferir o tipo de um bind parameter usado apenas como NULL,
        // então em vez de passar from/to nulos para a query usamos limites-sentinela que
        // cobrem todo o intervalo representável quando de/ate não são informados.
        Instant from = de != null ? de.atStartOfDay(ZoneOffset.UTC).toInstant() : EXTRATO_INICIO_PADRAO;
        Instant to = ate != null ? ate.atTime(23, 59, 59, 999_999_999).toInstant(ZoneOffset.UTC) : EXTRATO_FIM_PADRAO;

        Page<TransactionLogEntry> page = transactionLogRepository
                .findByAccountIdAndCreatedAtBetweenOrderByCreatedAtDesc(accountId, from, to, pageable);
        return PageResponse.from(page, TransactionResponse::from);
    }
}

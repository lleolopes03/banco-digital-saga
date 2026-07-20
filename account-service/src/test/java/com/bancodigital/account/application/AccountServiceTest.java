package com.bancodigital.account.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bancodigital.account.domain.Account;
import com.bancodigital.account.domain.AccountNotFoundException;
import com.bancodigital.account.domain.AccountStatus;
import com.bancodigital.account.domain.DuplicateOwnerDocumentException;
import com.bancodigital.account.domain.InvalidDepositAmountException;
import com.bancodigital.account.domain.OutboxEvent;
import com.bancodigital.account.domain.OutboxEventStatus;
import com.bancodigital.account.domain.TransactionLogEntry;
import com.bancodigital.account.domain.TransactionType;
import com.bancodigital.account.domain.event.CreditRequested;
import com.bancodigital.account.domain.event.DebitConfirmationRequested;
import com.bancodigital.account.domain.event.DepositCompleted;
import com.bancodigital.account.domain.event.ReleaseReservedAmount;
import com.bancodigital.account.domain.event.ReverseDebitRequested;
import com.bancodigital.account.domain.event.TransferRequested;
import com.bancodigital.account.infrastructure.observability.SagaObservability;
import com.bancodigital.account.infrastructure.persistence.AccountRepository;
import com.bancodigital.account.infrastructure.persistence.OutboxEventRepository;
import com.bancodigital.account.infrastructure.persistence.TransactionLogRepository;
import com.bancodigital.account.web.dto.AccountResponse;
import com.bancodigital.account.web.dto.CreateAccountRequest;
import com.bancodigital.account.web.dto.DepositRequest;
import com.bancodigital.account.web.dto.PageResponse;
import com.bancodigital.account.web.dto.TransactionResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.SimpleTransactionStatus;

@ExtendWith(MockitoExtension.class)
class AccountServiceTest {

    @Mock
    private AccountRepository accountRepository;

    @Mock
    private TransactionLogRepository transactionLogRepository;

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Mock
    private PlatformTransactionManager transactionManager;

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @Mock
    private SagaObservability sagaObservability;

    @InjectMocks
    private AccountService accountService;

    @BeforeEach
    void stubTransactionManager() {
        lenient().when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
        // Mock "cru" de StringRedisTemplate não responde opsForValue() sem stub explícito; os
        // testes aqui não validam comportamento de cache (isso é coberto por
        // AccountServiceCacheTest com Redis real via Testcontainers), então tratamos como
        // sempre-miss por padrão (get() retorna null).
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);
    }

    @Test
    void deveCriarContaComCpfNovoComSaldoZero() {
        when(accountRepository.findByOwnerDocument("11122233344")).thenReturn(Optional.empty());
        when(accountRepository.save(any(Account.class))).thenAnswer(invocation -> invocation.getArgument(0));

        AccountResponse response = accountService.criarConta(new CreateAccountRequest("Ana Lima", "11122233344"));

        assertThat(response.balance()).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(response.status()).isEqualTo(AccountStatus.ACTIVE);
        assertThat(response.ownerDocument()).isEqualTo("11122233344");
        verify(accountRepository).save(any(Account.class));
    }

    @Test
    void deveLancarExcecaoAoCriarContaComCpfDuplicado() {
        when(accountRepository.findByOwnerDocument("11122233344"))
                .thenReturn(Optional.of(Account.open("Ana Lima", "11122233344")));

        assertThatThrownBy(() -> accountService.criarConta(new CreateAccountRequest("Outra Pessoa", "11122233344")))
                .isInstanceOf(DuplicateOwnerDocumentException.class);

        verify(accountRepository, never()).save(any());
    }

    @Test
    void deveLancarExcecaoAoConsultarSaldoDeContaInexistente() {
        UUID accountId = UUID.randomUUID();
        when(accountRepository.findById(accountId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> accountService.consultarSaldo(accountId))
                .isInstanceOf(AccountNotFoundException.class);
    }

    @Test
    void deveAtualizarSaldoCorretamenteAoDepositarValorPositivo() {
        UUID accountId = UUID.randomUUID();
        Account account = Account.open("Ana Lima", "11122233344");
        when(accountRepository.findById(accountId)).thenReturn(Optional.of(account));
        when(accountRepository.saveAndFlush(any(Account.class))).thenAnswer(invocation -> invocation.getArgument(0));

        AccountResponse response = accountService.depositar(accountId, new DepositRequest(new BigDecimal("150.00"), "salário"));

        assertThat(response.balance()).isEqualByComparingTo("150.00");
        verify(accountRepository).saveAndFlush(account);
        verify(outboxEventRepository).save(any(OutboxEvent.class));
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-10.50"})
    void deveLancarExcecaoAoDepositarValorNegativoOuZero(String valorInvalido) {
        UUID accountId = UUID.randomUUID();
        when(accountRepository.findById(accountId)).thenReturn(Optional.of(Account.open("Ana Lima", "11122233344")));

        assertThatThrownBy(() -> accountService.depositar(accountId, new DepositRequest(new BigDecimal(valorInvalido), null)))
                .isInstanceOf(InvalidDepositAmountException.class);

        verify(accountRepository, never()).saveAndFlush(any());
    }

    @Test
    void doisDepositosConcorrentesNaMesmaContaDevemSerAplicadosCorretamente() throws Exception {
        UUID accountId = UUID.randomUUID();
        when(accountRepository.findById(accountId))
                .thenAnswer(invocation -> Optional.of(Account.open("Ana Lima", "11122233344")));
        when(accountRepository.saveAndFlush(any(Account.class))).thenAnswer(invocation -> invocation.getArgument(0));

        CompletableFuture<AccountResponse> deposito1 = CompletableFuture.supplyAsync(
                () -> accountService.depositar(accountId, new DepositRequest(new BigDecimal("100.00"), null)));
        CompletableFuture<AccountResponse> deposito2 = CompletableFuture.supplyAsync(
                () -> accountService.depositar(accountId, new DepositRequest(new BigDecimal("50.00"), null)));

        AccountResponse resultado1 = deposito1.get(5, TimeUnit.SECONDS);
        AccountResponse resultado2 = deposito2.get(5, TimeUnit.SECONDS);

        assertThat(resultado1.balance()).isEqualByComparingTo("100.00");
        assertThat(resultado2.balance()).isEqualByComparingTo("50.00");
        verify(accountRepository, times(2)).saveAndFlush(any(Account.class));
        verify(outboxEventRepository, times(2)).save(any(OutboxEvent.class));
    }

    @Test
    void deveGerarEntradaNoExtratoComTipoDepositoAoDepositar() {
        UUID accountId = UUID.randomUUID();
        when(accountRepository.findById(accountId)).thenReturn(Optional.of(Account.open("Ana Lima", "11122233344")));
        when(accountRepository.saveAndFlush(any(Account.class))).thenAnswer(invocation -> invocation.getArgument(0));

        accountService.depositar(accountId, new DepositRequest(new BigDecimal("150.00"), "salário"));

        ArgumentCaptor<TransactionLogEntry> captor = ArgumentCaptor.forClass(TransactionLogEntry.class);
        verify(transactionLogRepository).save(captor.capture());
        TransactionLogEntry entry = captor.getValue();
        assertThat(entry.getType()).isEqualTo(TransactionType.DEPOSIT);
        assertThat(entry.getAccountId()).isEqualTo(accountId);
        assertThat(entry.getAmount()).isEqualByComparingTo("150.00");
        assertThat(entry.getBalanceAfter()).isEqualByComparingTo("150.00");
    }

    @Test
    void deveFiltrarExtratoPorPeriodoInformado() {
        UUID accountId = UUID.randomUUID();
        Pageable pageable = PageRequest.of(0, 20);
        when(accountRepository.findById(accountId)).thenReturn(Optional.of(Account.open("Ana Lima", "11122233344")));
        TransactionLogEntry entry = TransactionLogEntry.deposit(UUID.randomUUID(), accountId,
                new BigDecimal("10.00"), new BigDecimal("10.00"), null);
        when(transactionLogRepository.findByAccountIdAndCreatedAtBetweenOrderByCreatedAtDesc(eq(accountId), any(), any(), eq(pageable)))
                .thenReturn(new PageImpl<>(List.of(entry), pageable, 1));

        LocalDate de = LocalDate.of(2026, 1, 1);
        LocalDate ate = LocalDate.of(2026, 1, 31);
        PageResponse<TransactionResponse> response = accountService.consultarExtrato(accountId, de, ate, pageable);

        assertThat(response.conteudo()).hasSize(1);
        ArgumentCaptor<Instant> fromCaptor = ArgumentCaptor.forClass(Instant.class);
        ArgumentCaptor<Instant> toCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(transactionLogRepository).findByAccountIdAndCreatedAtBetweenOrderByCreatedAtDesc(
                eq(accountId), fromCaptor.capture(), toCaptor.capture(), eq(pageable));
        assertThat(fromCaptor.getValue()).isEqualTo(de.atStartOfDay(ZoneOffset.UTC).toInstant());
        assertThat(toCaptor.getValue()).isEqualTo(ate.atTime(23, 59, 59, 999_999_999).toInstant(ZoneOffset.UTC));
    }

    @Test
    void deveRetornarListaVaziaParaContaSemMovimentacoes() {
        UUID accountId = UUID.randomUUID();
        Pageable pageable = PageRequest.of(0, 20);
        when(accountRepository.findById(accountId)).thenReturn(Optional.of(Account.open("Ana Lima", "11122233344")));
        when(transactionLogRepository.findByAccountIdAndCreatedAtBetweenOrderByCreatedAtDesc(eq(accountId), any(), any(), eq(pageable)))
                .thenReturn(new PageImpl<>(List.of(), pageable, 0));

        PageResponse<TransactionResponse> response = accountService.consultarExtrato(accountId, null, null, pageable);

        assertThat(response.conteudo()).isEmpty();
        assertThat(response.totalElementos()).isZero();
    }

    @Test
    void deveGravarOutboxEventComPayloadCorretoNaMesmaTransacaoDoDeposito() throws Exception {
        UUID accountId = UUID.randomUUID();
        when(accountRepository.findById(accountId)).thenReturn(Optional.of(Account.open("Ana Lima", "11122233344")));
        when(accountRepository.saveAndFlush(any(Account.class))).thenAnswer(invocation -> invocation.getArgument(0));

        accountService.depositar(accountId, new DepositRequest(new BigDecimal("150.00"), "salário"));

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        OutboxEvent outboxEvent = captor.getValue();

        assertThat(outboxEvent.getAggregateType()).isEqualTo("Account");
        assertThat(outboxEvent.getAggregateId()).isEqualTo(accountId);
        assertThat(outboxEvent.getEventType()).isEqualTo("DepositCompleted");
        assertThat(outboxEvent.getStatus()).isEqualTo(OutboxEventStatus.PENDING);
        assertThat(outboxEvent.getPublishedAt()).isNull();

        ObjectMapper payloadMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        DepositCompleted payload = payloadMapper.readValue(outboxEvent.getPayload(), DepositCompleted.class);
        assertThat(payload.accountId()).isEqualTo(accountId);
        assertThat(payload.amount()).isEqualByComparingTo("150.00");
    }

    @Test
    void deveFazerRollbackDoDepositoSeGravacaoDoOutboxFalhar() {
        UUID accountId = UUID.randomUUID();
        when(accountRepository.findById(accountId)).thenReturn(Optional.of(Account.open("Ana Lima", "11122233344")));
        when(accountRepository.saveAndFlush(any(Account.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(outboxEventRepository.save(any(OutboxEvent.class))).thenThrow(new RuntimeException("falha simulada ao gravar outbox"));

        assertThatThrownBy(() -> accountService.depositar(accountId, new DepositRequest(new BigDecimal("50.00"), null)))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("falha simulada ao gravar outbox");

        verify(transactionManager).rollback(any());
        verify(transactionManager, never()).commit(any());
    }

    @Test
    void deveReservarSaldoEPublicarTransferReservedQuandoHaSaldoSuficiente() {
        Account account = Account.open("Ana Lima", "11122233344");
        account.deposit(new BigDecimal("1000.00"));
        when(accountRepository.findById(account.getId())).thenReturn(Optional.of(account));
        when(accountRepository.saveAndFlush(any(Account.class))).thenAnswer(invocation -> invocation.getArgument(0));

        UUID transferId = UUID.randomUUID();
        TransferRequested event = new TransferRequested(transferId, account.getId(), UUID.randomUUID(),
                new BigDecimal("300.00"), Instant.now());

        accountService.reservarSaldo(event);

        assertThat(account.getBalance()).isEqualByComparingTo("700.00");
        assertThat(account.getReservedBalance()).isEqualByComparingTo("300.00");

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        OutboxEvent outboxEvent = captor.getValue();
        assertThat(outboxEvent.getAggregateType()).isEqualTo("Transfer");
        assertThat(outboxEvent.getAggregateId()).isEqualTo(transferId);
        assertThat(outboxEvent.getEventType()).isEqualTo("TransferReserved");
    }

    @Test
    void deveRejeitarReservaEPublicarTransferReservationFailedQuandoSaldoInsuficiente() {
        Account account = Account.open("Ana Lima", "11122233344");
        when(accountRepository.findById(account.getId())).thenReturn(Optional.of(account));
        when(accountRepository.saveAndFlush(any(Account.class))).thenAnswer(invocation -> invocation.getArgument(0));

        UUID transferId = UUID.randomUUID();
        TransferRequested event = new TransferRequested(transferId, account.getId(), UUID.randomUUID(),
                new BigDecimal("300.00"), Instant.now());

        accountService.reservarSaldo(event);

        assertThat(account.getBalance()).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(account.getReservedBalance()).isEqualByComparingTo(BigDecimal.ZERO);

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        assertThat(captor.getValue().getEventType()).isEqualTo("TransferReservationFailed");
    }

    @Test
    void deveConfirmarDebitoEGravarTransactionLogEntryTransferOut() {
        Account account = Account.open("Ana Lima", "11122233344");
        account.deposit(new BigDecimal("1000.00"));
        account.reservar(new BigDecimal("300.00"));
        when(accountRepository.findById(account.getId())).thenReturn(Optional.of(account));
        when(accountRepository.saveAndFlush(any(Account.class))).thenAnswer(invocation -> invocation.getArgument(0));

        UUID transferId = UUID.randomUUID();
        DebitConfirmationRequested event = new DebitConfirmationRequested(transferId, account.getId(),
                new BigDecimal("300.00"), Instant.now());

        accountService.confirmarDebito(event);

        assertThat(account.getReservedBalance()).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(account.getBalance()).isEqualByComparingTo("700.00");

        ArgumentCaptor<TransactionLogEntry> logCaptor = ArgumentCaptor.forClass(TransactionLogEntry.class);
        verify(transactionLogRepository).save(logCaptor.capture());
        assertThat(logCaptor.getValue().getType()).isEqualTo(TransactionType.TRANSFER_OUT);
        assertThat(logCaptor.getValue().getRelatedTransactionId()).isEqualTo(transferId);
        assertThat(logCaptor.getValue().getAmount()).isEqualByComparingTo("300.00");

        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(outboxCaptor.capture());
        assertThat(outboxCaptor.getValue().getEventType()).isEqualTo("DebitConfirmed");
        assertThat(outboxCaptor.getValue().getAggregateId()).isEqualTo(transferId);
    }

    @Test
    void deveCreditarContaDestinoEGravarTransactionLogEntryTransferIn() {
        Account account = Account.open("Carlos Souza", "99988877766");
        when(accountRepository.findById(account.getId())).thenReturn(Optional.of(account));
        when(accountRepository.saveAndFlush(any(Account.class))).thenAnswer(invocation -> invocation.getArgument(0));

        UUID transferId = UUID.randomUUID();
        CreditRequested event = new CreditRequested(transferId, account.getId(), new BigDecimal("300.00"), Instant.now());

        accountService.creditarPorTransferencia(event);

        assertThat(account.getBalance()).isEqualByComparingTo("300.00");

        ArgumentCaptor<TransactionLogEntry> logCaptor = ArgumentCaptor.forClass(TransactionLogEntry.class);
        verify(transactionLogRepository).save(logCaptor.capture());
        assertThat(logCaptor.getValue().getType()).isEqualTo(TransactionType.TRANSFER_IN);
        assertThat(logCaptor.getValue().getRelatedTransactionId()).isEqualTo(transferId);

        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(outboxCaptor.capture());
        assertThat(outboxCaptor.getValue().getEventType()).isEqualTo("CreditConfirmed");
        assertThat(outboxCaptor.getValue().getAggregateId()).isEqualTo(transferId);
    }

    @Test
    void devePublicarCreditFailedEmVezDeCreditarQuandoContaDestinoEstaBloqueada() {
        Account account = Account.open("Carlos Souza", "99988877766");
        account.bloquear();
        when(accountRepository.findById(account.getId())).thenReturn(Optional.of(account));

        UUID transferId = UUID.randomUUID();
        CreditRequested event = new CreditRequested(transferId, account.getId(), new BigDecimal("300.00"), Instant.now());

        accountService.creditarPorTransferencia(event);

        assertThat(account.getBalance()).isEqualByComparingTo(BigDecimal.ZERO);
        verify(accountRepository, never()).saveAndFlush(any());
        verify(transactionLogRepository, never()).save(any());

        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(outboxCaptor.capture());
        assertThat(outboxCaptor.getValue().getEventType()).isEqualTo("CreditFailed");
        assertThat(outboxCaptor.getValue().getAggregateId()).isEqualTo(transferId);
    }

    @Test
    void deveLiberarReservaDevolvendoValorParaBalanceEGravarReversalNoExtrato() {
        Account account = Account.open("Ana Lima", "11122233344");
        account.deposit(new BigDecimal("1000.00"));
        account.reservar(new BigDecimal("300.00"));
        when(accountRepository.findById(account.getId())).thenReturn(Optional.of(account));
        when(accountRepository.saveAndFlush(any(Account.class))).thenAnswer(invocation -> invocation.getArgument(0));

        UUID transferId = UUID.randomUUID();
        ReleaseReservedAmount event = new ReleaseReservedAmount(transferId, account.getId(),
                new BigDecimal("300.00"), Instant.now());

        accountService.liberarReserva(event);

        assertThat(account.getBalance()).isEqualByComparingTo("1000.00");
        assertThat(account.getReservedBalance()).isEqualByComparingTo(BigDecimal.ZERO);

        ArgumentCaptor<TransactionLogEntry> logCaptor = ArgumentCaptor.forClass(TransactionLogEntry.class);
        verify(transactionLogRepository).save(logCaptor.capture());
        assertThat(logCaptor.getValue().getType()).isEqualTo(TransactionType.REVERSAL);
        assertThat(logCaptor.getValue().getRelatedTransactionId()).isEqualTo(transferId);
        assertThat(logCaptor.getValue().getAmount()).isEqualByComparingTo("300.00");

        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(outboxCaptor.capture());
        assertThat(outboxCaptor.getValue().getEventType()).isEqualTo("ReservationReleased");
        assertThat(outboxCaptor.getValue().getAggregateId()).isEqualTo(transferId);
    }

    @Test
    void deveReverterDebitoCreditandoDeVoltaEGravarReversalNoExtrato() {
        Account account = Account.open("Ana Lima", "11122233344");
        account.deposit(new BigDecimal("700.00"));
        when(accountRepository.findById(account.getId())).thenReturn(Optional.of(account));
        when(accountRepository.saveAndFlush(any(Account.class))).thenAnswer(invocation -> invocation.getArgument(0));

        UUID transferId = UUID.randomUUID();
        ReverseDebitRequested event = new ReverseDebitRequested(transferId, account.getId(),
                new BigDecimal("300.00"), Instant.now());

        accountService.reverterDebito(event);

        assertThat(account.getBalance()).isEqualByComparingTo("1000.00");

        ArgumentCaptor<TransactionLogEntry> logCaptor = ArgumentCaptor.forClass(TransactionLogEntry.class);
        verify(transactionLogRepository).save(logCaptor.capture());
        assertThat(logCaptor.getValue().getType()).isEqualTo(TransactionType.REVERSAL);
        assertThat(logCaptor.getValue().getRelatedTransactionId()).isEqualTo(transferId);
        assertThat(logCaptor.getValue().getAmount()).isEqualByComparingTo("300.00");

        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(outboxCaptor.capture());
        assertThat(outboxCaptor.getValue().getEventType()).isEqualTo("DebitReversed");
        assertThat(outboxCaptor.getValue().getAggregateId()).isEqualTo(transferId);
    }
}

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
import com.bancodigital.account.domain.event.DepositCompleted;
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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
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
    private BalanceCache balanceCache;

    @Mock
    private SagaObservability sagaObservability;

    private AccountService accountService;

    @BeforeEach
    void montarServico() {
        lenient().when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
        // O cache de saldo é mockado (sempre-miss por padrão): o comportamento real do cache é
        // coberto por AccountServiceCacheTest, com Redis real via Testcontainers.
        OutboxEventWriter outboxEventWriter = new OutboxEventWriter(outboxEventRepository, sagaObservability);
        accountService = new AccountService(accountRepository, transactionLogRepository, outboxEventWriter,
                balanceCache, transactionManager);
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
                .thenReturn(Optional.of(Account.abrir("Ana Lima", "11122233344")));

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
        Account account = Account.abrir("Ana Lima", "11122233344");
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
        when(accountRepository.findById(accountId)).thenReturn(Optional.of(Account.abrir("Ana Lima", "11122233344")));

        assertThatThrownBy(() -> accountService.depositar(accountId, new DepositRequest(new BigDecimal(valorInvalido), null)))
                .isInstanceOf(InvalidDepositAmountException.class);

        verify(accountRepository, never()).saveAndFlush(any());
    }

    @Test
    void doisDepositosConcorrentesNaMesmaContaDevemSerAplicadosCorretamente() throws Exception {
        UUID accountId = UUID.randomUUID();
        when(accountRepository.findById(accountId))
                .thenAnswer(invocation -> Optional.of(Account.abrir("Ana Lima", "11122233344")));
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
        when(accountRepository.findById(accountId)).thenReturn(Optional.of(Account.abrir("Ana Lima", "11122233344")));
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
        when(accountRepository.findById(accountId)).thenReturn(Optional.of(Account.abrir("Ana Lima", "11122233344")));
        TransactionLogEntry entry = TransactionLogEntry.deposito(UUID.randomUUID(), accountId,
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
        when(accountRepository.findById(accountId)).thenReturn(Optional.of(Account.abrir("Ana Lima", "11122233344")));
        when(transactionLogRepository.findByAccountIdAndCreatedAtBetweenOrderByCreatedAtDesc(eq(accountId), any(), any(), eq(pageable)))
                .thenReturn(new PageImpl<>(List.of(), pageable, 0));

        PageResponse<TransactionResponse> response = accountService.consultarExtrato(accountId, null, null, pageable);

        assertThat(response.conteudo()).isEmpty();
        assertThat(response.totalElementos()).isZero();
    }

    @Test
    void deveGravarOutboxEventComPayloadCorretoNaMesmaTransacaoDoDeposito() throws Exception {
        UUID accountId = UUID.randomUUID();
        when(accountRepository.findById(accountId)).thenReturn(Optional.of(Account.abrir("Ana Lima", "11122233344")));
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
        when(accountRepository.findById(accountId)).thenReturn(Optional.of(Account.abrir("Ana Lima", "11122233344")));
        when(accountRepository.saveAndFlush(any(Account.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(outboxEventRepository.save(any(OutboxEvent.class))).thenThrow(new RuntimeException("falha simulada ao gravar outbox"));

        assertThatThrownBy(() -> accountService.depositar(accountId, new DepositRequest(new BigDecimal("50.00"), null)))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("falha simulada ao gravar outbox");

        verify(transactionManager).rollback(any());
        verify(transactionManager, never()).commit(any());
    }
}

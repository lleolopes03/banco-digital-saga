package com.bancodigital.account.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bancodigital.account.domain.Account;
import com.bancodigital.account.domain.OutboxEvent;
import com.bancodigital.account.domain.TransactionLogEntry;
import com.bancodigital.account.domain.TransactionType;
import com.bancodigital.account.domain.event.CreditRequested;
import com.bancodigital.account.domain.event.DebitConfirmationRequested;
import com.bancodigital.account.domain.event.ReleaseReservedAmount;
import com.bancodigital.account.domain.event.ReverseDebitRequested;
import com.bancodigital.account.domain.event.TransferRequested;
import com.bancodigital.account.infrastructure.observability.SagaObservability;
import com.bancodigital.account.infrastructure.persistence.AccountRepository;
import com.bancodigital.account.infrastructure.persistence.OutboxEventRepository;
import com.bancodigital.account.infrastructure.persistence.TransactionLogRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.SimpleTransactionStatus;

@ExtendWith(MockitoExtension.class)
class AccountSagaServiceTest {

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

    private AccountSagaService accountService;

    @BeforeEach
    void montarServico() {
        lenient().when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
        OutboxEventWriter outboxEventWriter = new OutboxEventWriter(outboxEventRepository, sagaObservability);
        accountService = new AccountSagaService(accountRepository, transactionLogRepository, outboxEventWriter,
                balanceCache, transactionManager);
    }
    @Test
    void deveReservarSaldoEPublicarTransferReservedQuandoHaSaldoSuficiente() {
        Account account = Account.abrir("Ana Lima", "11122233344");
        account.depositar(new BigDecimal("1000.00"));
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
        Account account = Account.abrir("Ana Lima", "11122233344");
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
        Account account = Account.abrir("Ana Lima", "11122233344");
        account.depositar(new BigDecimal("1000.00"));
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
        Account account = Account.abrir("Carlos Souza", "99988877766");
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
        Account account = Account.abrir("Carlos Souza", "99988877766");
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
        Account account = Account.abrir("Ana Lima", "11122233344");
        account.depositar(new BigDecimal("1000.00"));
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
        Account account = Account.abrir("Ana Lima", "11122233344");
        account.depositar(new BigDecimal("700.00"));
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

    @Test
    void deveInvalidarOCacheDeSaldoDaContaAfetadaDepoisDoPasso() {
        Account account = Account.abrir("Ana Lima", "11122233344");
        account.depositar(new BigDecimal("1000.00"));
        when(accountRepository.findById(account.getId())).thenReturn(Optional.of(account));
        when(accountRepository.saveAndFlush(any(Account.class))).thenAnswer(invocation -> invocation.getArgument(0));

        accountService.reservarSaldo(new TransferRequested(UUID.randomUUID(), account.getId(), UUID.randomUUID(),
                new BigDecimal("300.00"), Instant.now()));

        verify(balanceCache).invalidar(account.getId());
    }

    @Test
    void naoDeveInvalidarOCacheSeOPassoFalhar() {
        UUID accountId = UUID.randomUUID();
        when(accountRepository.findById(accountId)).thenReturn(Optional.empty());

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> accountService.reservarSaldo(new TransferRequested(
                UUID.randomUUID(), accountId, UUID.randomUUID(), new BigDecimal("300.00"), Instant.now())))
                .isInstanceOf(com.bancodigital.account.domain.AccountNotFoundException.class);

        verify(balanceCache, never()).invalidar(any());
    }
}

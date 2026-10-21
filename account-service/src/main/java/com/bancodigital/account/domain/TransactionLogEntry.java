package com.bancodigital.account.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "transaction_log_entries")
public class TransactionLogEntry {

    @Id
    private UUID id;

    @Column(name = "account_id", nullable = false)
    private UUID accountId;

    @Enumerated(EnumType.STRING)
    @Column(name = "type", nullable = false)
    private TransactionType type;

    @Column(name = "amount", nullable = false, precision = 19, scale = 2)
    private BigDecimal amount;

    @Column(name = "balance_after", nullable = false, precision = 19, scale = 2)
    private BigDecimal balanceAfter;

    @Column(name = "related_transaction_id")
    private UUID relatedTransactionId;

    @Column(name = "description")
    private String description;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected TransactionLogEntry() {
    }

    private TransactionLogEntry(UUID id, UUID accountId, TransactionType type, BigDecimal amount,
                                 BigDecimal balanceAfter, UUID relatedTransactionId, String description,
                                 Instant createdAt) {
        this.id = id;
        this.accountId = accountId;
        this.type = type;
        this.amount = amount;
        this.balanceAfter = balanceAfter;
        this.relatedTransactionId = relatedTransactionId;
        this.description = description;
        this.createdAt = createdAt;
    }

    public static TransactionLogEntry deposito(UUID transactionId, UUID accountId, BigDecimal amount,
                                                BigDecimal balanceAfter, String description) {
        return new TransactionLogEntry(transactionId, accountId, TransactionType.DEPOSIT, amount, balanceAfter,
                null, description, Instant.now());
    }

    public static TransactionLogEntry transferenciaEnviada(UUID transferId, UUID accountId, BigDecimal amount,
                                                    BigDecimal balanceAfter, String description) {
        return new TransactionLogEntry(UUID.randomUUID(), accountId, TransactionType.TRANSFER_OUT, amount,
                balanceAfter, transferId, description, Instant.now());
    }

    public static TransactionLogEntry transferenciaRecebida(UUID transferId, UUID accountId, BigDecimal amount,
                                                   BigDecimal balanceAfter, String description) {
        return new TransactionLogEntry(UUID.randomUUID(), accountId, TransactionType.TRANSFER_IN, amount,
                balanceAfter, transferId, description, Instant.now());
    }

    public static TransactionLogEntry estorno(UUID transferId, UUID accountId, BigDecimal amount,
                                                 BigDecimal balanceAfter, String description) {
        return new TransactionLogEntry(UUID.randomUUID(), accountId, TransactionType.REVERSAL, amount,
                balanceAfter, transferId, description, Instant.now());
    }

    public UUID getId() {
        return id;
    }

    public UUID getAccountId() {
        return accountId;
    }

    public TransactionType getType() {
        return type;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public BigDecimal getBalanceAfter() {
        return balanceAfter;
    }

    public UUID getRelatedTransactionId() {
        return relatedTransactionId;
    }

    public String getDescription() {
        return description;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}

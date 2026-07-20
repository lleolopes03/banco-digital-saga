package com.bancodigital.account.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "accounts")
public class Account {

    @Id
    private UUID id;

    @Column(name = "owner_name", nullable = false)
    private String ownerName;

    @Column(name = "owner_document", nullable = false, unique = true)
    private String ownerDocument;

    @Column(name = "balance", nullable = false, precision = 19, scale = 2)
    private BigDecimal balance;

    @Column(name = "reserved_balance", nullable = false, precision = 19, scale = 2)
    private BigDecimal reservedBalance;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private AccountStatus status;

    @Version
    @Column(name = "version", nullable = false)
    private Long version;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected Account() {
    }

    private Account(UUID id, String ownerName, String ownerDocument, BigDecimal balance,
                     BigDecimal reservedBalance, AccountStatus status, Instant createdAt) {
        this.id = id;
        this.ownerName = ownerName;
        this.ownerDocument = ownerDocument;
        this.balance = balance;
        this.reservedBalance = reservedBalance;
        this.status = status;
        this.createdAt = createdAt;
    }

    public static Account open(String ownerName, String ownerDocument) {
        return new Account(UUID.randomUUID(), ownerName, ownerDocument, BigDecimal.ZERO, BigDecimal.ZERO,
                AccountStatus.ACTIVE, Instant.now());
    }

    public UUID getId() {
        return id;
    }

    public String getOwnerName() {
        return ownerName;
    }

    public String getOwnerDocument() {
        return ownerDocument;
    }

    public BigDecimal getBalance() {
        return balance;
    }

    public BigDecimal getReservedBalance() {
        return reservedBalance;
    }

    public AccountStatus getStatus() {
        return status;
    }

    public Long getVersion() {
        return version;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void deposit(BigDecimal amount) {
        if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new InvalidDepositAmountException(amount);
        }
        this.balance = this.balance.add(amount);
    }

    /**
     * Reserva um valor para uma transferência: tira o valor de balance e coloca em
     * reservedBalance ("saldo preso", ainda não é débito definitivo). Retorna false em vez de
     * lançar exceção quando o saldo é insuficiente, porque essa é uma recusa de negócio normal
     * (vira um evento TransferReservationFailed), não um erro de programação.
     */
    public boolean reservar(BigDecimal valor) {
        if (balance.compareTo(valor) < 0) {
            return false;
        }
        this.balance = this.balance.subtract(valor);
        this.reservedBalance = this.reservedBalance.add(valor);
        return true;
    }

    /**
     * Confirma o débito de um valor já reservado: apenas remove de reservedBalance, já que
     * balance já foi decrementado no momento da reserva.
     */
    public void confirmarDebito(BigDecimal valor) {
        if (reservedBalance.compareTo(valor) < 0) {
            throw new IllegalStateException(
                    "Saldo reservado insuficiente para confirmar débito: reservado=" + reservedBalance + ", valor=" + valor);
        }
        this.reservedBalance = this.reservedBalance.subtract(valor);
    }

    /**
     * Desfaz uma reserva ainda não confirmada (compensação dos cenários de rejeição por
     * antifraude e de timeout pré-débito): devolve o valor de reservedBalance para balance, o
     * inverso exato de {@link #reservar(BigDecimal)}.
     */
    public void liberarReserva(BigDecimal valor) {
        if (reservedBalance.compareTo(valor) < 0) {
            throw new IllegalStateException(
                    "Saldo reservado insuficiente para liberar: reservado=" + reservedBalance + ", valor=" + valor);
        }
        this.reservedBalance = this.reservedBalance.subtract(valor);
        this.balance = this.balance.add(valor);
    }

    public void bloquear() {
        this.status = AccountStatus.BLOCKED;
    }
}

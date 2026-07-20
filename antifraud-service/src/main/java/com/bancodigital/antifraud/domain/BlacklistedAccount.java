package com.bancodigital.antifraud.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "blacklisted_accounts")
public class BlacklistedAccount {

    @Id
    @Column(name = "account_id")
    private UUID accountId;

    @Column(name = "reason", nullable = false)
    private String reason;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected BlacklistedAccount() {
    }

    public UUID getAccountId() {
        return accountId;
    }

    public String getReason() {
        return reason;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}

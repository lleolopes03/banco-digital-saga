package com.bancodigital.account.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.UUID;

@Entity
@Table(name = "users")
public class User {

    @Id
    private UUID id;

    @Column(name = "email", nullable = false, unique = true)
    private String email;

    @Column(name = "password_hash", nullable = false)
    private String passwordHash;

    @Column(name = "account_id", nullable = false)
    private UUID accountId;

    protected User() {
    }

    private User(UUID id, String email, String passwordHash, UUID accountId) {
        this.id = id;
        this.email = email;
        this.passwordHash = passwordHash;
        this.accountId = accountId;
    }

    public static User criar(String email, String passwordHash, UUID accountId) {
        return new User(UUID.randomUUID(), email, passwordHash, accountId);
    }

    public UUID getId() {
        return id;
    }

    public String getEmail() {
        return email;
    }

    public String getPasswordHash() {
        return passwordHash;
    }

    public UUID getAccountId() {
        return accountId;
    }
}

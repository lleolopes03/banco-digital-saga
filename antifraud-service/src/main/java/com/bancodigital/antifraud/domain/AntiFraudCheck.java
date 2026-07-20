package com.bancodigital.antifraud.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "antifraud_checks")
public class AntiFraudCheck {

    @Id
    private UUID id;

    @Column(name = "transfer_id", nullable = false)
    private UUID transferId;

    @Enumerated(EnumType.STRING)
    @Column(name = "resultado", nullable = false)
    private AntiFraudResult resultado;

    @Column(name = "motivo")
    private String motivo;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected AntiFraudCheck() {
    }

    private AntiFraudCheck(UUID id, UUID transferId, AntiFraudResult resultado, String motivo, Instant createdAt) {
        this.id = id;
        this.transferId = transferId;
        this.resultado = resultado;
        this.motivo = motivo;
        this.createdAt = createdAt;
    }

    public static AntiFraudCheck aprovado(UUID transferId) {
        return new AntiFraudCheck(UUID.randomUUID(), transferId, AntiFraudResult.APPROVED, null, Instant.now());
    }

    public static AntiFraudCheck rejeitado(UUID transferId, String motivo) {
        return new AntiFraudCheck(UUID.randomUUID(), transferId, AntiFraudResult.REJECTED, motivo, Instant.now());
    }

    public UUID getId() {
        return id;
    }

    public UUID getTransferId() {
        return transferId;
    }

    public AntiFraudResult getResultado() {
        return resultado;
    }

    public String getMotivo() {
        return motivo;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}

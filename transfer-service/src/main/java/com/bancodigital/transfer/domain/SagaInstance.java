package com.bancodigital.transfer.domain;

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
@Table(name = "saga_instances")
public class SagaInstance {

    @Id
    private UUID id;

    @Column(name = "account_origem_id", nullable = false)
    private UUID accountOrigemId;

    @Column(name = "account_destino_id", nullable = false)
    private UUID accountDestinoId;

    @Column(name = "valor", nullable = false, precision = 19, scale = 2)
    private BigDecimal valor;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private SagaStatus status;

    @Column(name = "current_step")
    private String currentStep;

    @Column(name = "failure_reason")
    private String failureReason;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Column(name = "idempotency_key")
    private String idempotencyKey;

    protected SagaInstance() {
    }

    private SagaInstance(UUID id, UUID accountOrigemId, UUID accountDestinoId, BigDecimal valor, SagaStatus status,
                          String currentStep, Instant createdAt, Instant updatedAt, String idempotencyKey) {
        this.id = id;
        this.accountOrigemId = accountOrigemId;
        this.accountDestinoId = accountDestinoId;
        this.valor = valor;
        this.status = status;
        this.currentStep = currentStep;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.idempotencyKey = idempotencyKey;
    }

    public static SagaInstance iniciar(UUID transferId, UUID accountOrigemId, UUID accountDestinoId, BigDecimal valor) {
        return iniciar(transferId, accountOrigemId, accountDestinoId, valor, null);
    }

    public static SagaInstance iniciar(UUID transferId, UUID accountOrigemId, UUID accountDestinoId, BigDecimal valor,
                                        String idempotencyKey) {
        Instant agora = Instant.now();
        return new SagaInstance(transferId, accountOrigemId, accountDestinoId, valor, SagaStatus.STARTED,
                SagaStatus.STARTED.name(), agora, agora, idempotencyKey);
    }

    void aplicarTransicao(SagaStatus novoStatus, String currentStep) {
        this.status = novoStatus;
        this.currentStep = currentStep;
        this.updatedAt = Instant.now();
    }

    public void registrarMotivoFalha(String motivo) {
        this.failureReason = motivo;
        this.updatedAt = Instant.now();
    }

    public UUID getId() {
        return id;
    }

    public UUID getAccountOrigemId() {
        return accountOrigemId;
    }

    public UUID getAccountDestinoId() {
        return accountDestinoId;
    }

    public BigDecimal getValor() {
        return valor;
    }

    public SagaStatus getStatus() {
        return status;
    }

    public String getCurrentStep() {
        return currentStep;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }
}

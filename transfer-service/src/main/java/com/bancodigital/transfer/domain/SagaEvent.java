package com.bancodigital.transfer.domain;

public enum SagaEvent {
    RESERVAR_SALDO(SagaStatus.RESERVED),
    SOLICITAR_ANTIFRAUDE(SagaStatus.ANTIFRAUD_PENDING),
    APROVAR_ANTIFRAUDE(SagaStatus.ANTIFRAUD_APPROVED),
    REJEITAR_ANTIFRAUDE(SagaStatus.ANTIFRAUD_REJECTED),
    DEBITAR(SagaStatus.DEBITED),
    CREDITAR(SagaStatus.CREDITED),
    COMPLETAR(SagaStatus.COMPLETED),
    FALHAR(SagaStatus.FAILED);

    private final SagaStatus proximoEstado;

    SagaEvent(SagaStatus proximoEstado) {
        this.proximoEstado = proximoEstado;
    }

    public SagaStatus getProximoEstado() {
        return proximoEstado;
    }
}

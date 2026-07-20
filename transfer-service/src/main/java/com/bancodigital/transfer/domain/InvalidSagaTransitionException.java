package com.bancodigital.transfer.domain;

public class InvalidSagaTransitionException extends RuntimeException {

    public InvalidSagaTransitionException(SagaStatus estadoAtual, SagaEvent evento, SagaStatus proximoEstado) {
        super("Transição inválida: não é possível ir de " + estadoAtual + " para " + proximoEstado
                + " através do evento " + evento);
    }
}

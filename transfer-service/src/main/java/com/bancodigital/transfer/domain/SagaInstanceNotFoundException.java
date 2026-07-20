package com.bancodigital.transfer.domain;

import java.util.UUID;

public class SagaInstanceNotFoundException extends RuntimeException {

    public SagaInstanceNotFoundException(UUID transferId) {
        super("Saga não encontrada: " + transferId);
    }
}

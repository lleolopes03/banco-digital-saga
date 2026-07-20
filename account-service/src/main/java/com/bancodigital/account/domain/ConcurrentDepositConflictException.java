package com.bancodigital.account.domain;

import java.util.UUID;

public class ConcurrentDepositConflictException extends RuntimeException {

    public ConcurrentDepositConflictException(UUID accountId) {
        super("Não foi possível concluir o depósito na conta " + accountId
                + " após múltiplas tentativas: alta concorrência de atualizações de saldo");
    }
}

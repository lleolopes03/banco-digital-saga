package com.bancodigital.transfer.domain;

import java.math.BigDecimal;
import java.util.UUID;

public class InsufficientBalanceException extends RuntimeException {

    public InsufficientBalanceException(UUID accountOrigemId, BigDecimal valor, BigDecimal saldoDisponivel) {
        super("Saldo insuficiente na conta " + accountOrigemId + ": disponível=" + saldoDisponivel
                + ", solicitado=" + valor);
    }
}

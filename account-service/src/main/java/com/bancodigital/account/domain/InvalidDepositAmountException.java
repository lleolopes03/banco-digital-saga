package com.bancodigital.account.domain;

import java.math.BigDecimal;

public class InvalidDepositAmountException extends RuntimeException {

    public InvalidDepositAmountException(BigDecimal amount) {
        super("Valor de depósito inválido, deve ser maior que zero: " + amount);
    }
}

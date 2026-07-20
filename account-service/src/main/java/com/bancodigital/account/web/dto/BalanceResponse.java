package com.bancodigital.account.web.dto;

import com.bancodigital.account.domain.Account;
import java.math.BigDecimal;
import java.util.UUID;

public record BalanceResponse(UUID accountId, BigDecimal balance) {

    public static BalanceResponse from(Account account) {
        return new BalanceResponse(account.getId(), account.getBalance());
    }
}

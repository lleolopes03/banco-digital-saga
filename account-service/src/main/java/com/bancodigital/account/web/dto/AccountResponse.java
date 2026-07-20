package com.bancodigital.account.web.dto;

import com.bancodigital.account.domain.Account;
import com.bancodigital.account.domain.AccountStatus;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record AccountResponse(
        UUID id,
        String ownerName,
        String ownerDocument,
        BigDecimal balance,
        AccountStatus status,
        Instant createdAt
) {

    public static AccountResponse from(Account account) {
        return new AccountResponse(
                account.getId(),
                account.getOwnerName(),
                account.getOwnerDocument(),
                account.getBalance(),
                account.getStatus(),
                account.getCreatedAt()
        );
    }
}

package com.bancodigital.account.web.dto;

import com.bancodigital.account.domain.TransactionLogEntry;
import com.bancodigital.account.domain.TransactionType;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record TransactionResponse(
        UUID id,
        UUID accountId,
        TransactionType type,
        BigDecimal amount,
        BigDecimal balanceAfter,
        UUID relatedTransactionId,
        String description,
        Instant createdAt
) {

    public static TransactionResponse from(TransactionLogEntry entry) {
        return new TransactionResponse(
                entry.getId(),
                entry.getAccountId(),
                entry.getType(),
                entry.getAmount(),
                entry.getBalanceAfter(),
                entry.getRelatedTransactionId(),
                entry.getDescription(),
                entry.getCreatedAt()
        );
    }
}

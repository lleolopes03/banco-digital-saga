package com.bancodigital.account.domain.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record DepositCompleted(
        UUID transactionId,
        UUID accountId,
        BigDecimal amount,
        Instant occurredAt
) {
}

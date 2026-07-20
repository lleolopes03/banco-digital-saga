package com.bancodigital.account.domain.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record DebitConfirmationRequested(UUID transferId, UUID accountOrigemId, BigDecimal valor, Instant timestamp) {
}

package com.bancodigital.transfer.domain.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record DebitConfirmed(UUID transferId, UUID accountOrigemId, BigDecimal valor, Instant timestamp) {
}

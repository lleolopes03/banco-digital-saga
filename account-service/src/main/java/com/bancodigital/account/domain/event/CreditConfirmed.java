package com.bancodigital.account.domain.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record CreditConfirmed(UUID transferId, UUID accountDestinoId, BigDecimal valor, Instant timestamp) {
}

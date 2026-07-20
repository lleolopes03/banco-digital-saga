package com.bancodigital.antifraud.domain.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record TransferReserved(
        UUID transferId,
        UUID accountOrigemId,
        UUID accountDestinoId,
        BigDecimal valor,
        Instant timestamp
) {
}

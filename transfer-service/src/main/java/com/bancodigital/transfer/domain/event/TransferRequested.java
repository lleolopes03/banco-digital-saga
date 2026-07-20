package com.bancodigital.transfer.domain.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record TransferRequested(
        UUID transferId,
        UUID accountOrigemId,
        UUID accountDestinoId,
        BigDecimal valor,
        Instant timestamp
) {
}

package com.bancodigital.transfer.domain.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record TransferFailed(UUID transferId, UUID accountOrigemId, UUID accountDestinoId, BigDecimal valor,
                              String motivo, Instant timestamp) {
}

package com.bancodigital.transfer.domain.event;

import java.time.Instant;
import java.util.UUID;

public record CreditFailed(UUID transferId, UUID accountDestinoId, String motivo, Instant timestamp) {
}

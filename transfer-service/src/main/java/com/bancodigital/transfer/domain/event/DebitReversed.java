package com.bancodigital.transfer.domain.event;

import java.time.Instant;
import java.util.UUID;

public record DebitReversed(UUID transferId, UUID accountOrigemId, Instant timestamp) {
}

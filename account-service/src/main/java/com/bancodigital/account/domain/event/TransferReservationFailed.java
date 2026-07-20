package com.bancodigital.account.domain.event;

import java.time.Instant;
import java.util.UUID;

public record TransferReservationFailed(UUID transferId, String motivo, Instant timestamp) {
}

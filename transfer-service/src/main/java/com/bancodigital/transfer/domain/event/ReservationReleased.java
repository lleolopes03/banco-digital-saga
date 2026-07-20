package com.bancodigital.transfer.domain.event;

import java.time.Instant;
import java.util.UUID;

public record ReservationReleased(UUID transferId, Instant timestamp) {
}

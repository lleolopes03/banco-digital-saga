package com.bancodigital.transfer.domain.event;

import java.time.Instant;
import java.util.UUID;

public record AntiFraudRejected(UUID transferId, String motivo, Instant timestamp) {
}

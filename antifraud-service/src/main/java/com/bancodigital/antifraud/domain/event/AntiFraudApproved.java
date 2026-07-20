package com.bancodigital.antifraud.domain.event;

import java.time.Instant;
import java.util.UUID;

public record AntiFraudApproved(UUID transferId, Instant timestamp) {
}

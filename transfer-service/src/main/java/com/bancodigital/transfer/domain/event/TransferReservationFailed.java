package com.bancodigital.transfer.domain.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Publicado pelo account-service quando não consegue reservar o saldo (hoje: SALDO_INSUFICIENTE).
 * Chega no mesmo tópico (transfer.reserved) de {@link TransferReserved}, distinguido pelo header
 * eventType.
 */
public record TransferReservationFailed(UUID transferId, String motivo, Instant timestamp) {
}

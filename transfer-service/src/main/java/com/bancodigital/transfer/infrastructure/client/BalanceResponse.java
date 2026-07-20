package com.bancodigital.transfer.infrastructure.client;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Espelha o {@code BalanceResponse} do account-service (GET /api/accounts/{id}/balance). Cópia
 * local proposital: transfer-service não deve depender do jar do account-service, só do
 * contrato HTTP entre os dois.
 */
public record BalanceResponse(UUID accountId, BigDecimal balance) {
}

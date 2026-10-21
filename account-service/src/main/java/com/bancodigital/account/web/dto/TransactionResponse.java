package com.bancodigital.account.web.dto;

import com.bancodigital.account.domain.TransactionLogEntry;
import com.bancodigital.account.domain.TransactionType;
import io.swagger.v3.oas.annotations.media.Schema;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Schema(description = "Lançamento do extrato.")
public record TransactionResponse(
        @Schema(description = "Identificador do lançamento.") UUID id,
        @Schema(description = "Conta do lançamento.") UUID accountId,
        @Schema(description = "Tipo do lançamento.", example = "DEPOSIT") TransactionType type,
        @Schema(description = "Valor movimentado, em reais.", example = "150.00") BigDecimal amount,
        @Schema(description = "Saldo da conta depois do lançamento.", example = "300.00") BigDecimal balanceAfter,
        @Schema(description = "Transferência ou depósito de origem do lançamento, quando houver.") UUID relatedTransactionId,
        @Schema(description = "Descrição informada no lançamento.", example = "Salário") String description,
        @Schema(description = "Momento do lançamento (UTC).", example = "2026-10-21T14:05:00Z") Instant createdAt
) {

    public static TransactionResponse from(TransactionLogEntry entry) {
        return new TransactionResponse(
                entry.getId(),
                entry.getAccountId(),
                entry.getType(),
                entry.getAmount(),
                entry.getBalanceAfter(),
                entry.getRelatedTransactionId(),
                entry.getDescription(),
                entry.getCreatedAt()
        );
    }
}

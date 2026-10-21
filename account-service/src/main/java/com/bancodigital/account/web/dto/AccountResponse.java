package com.bancodigital.account.web.dto;

import com.bancodigital.account.domain.Account;
import com.bancodigital.account.domain.AccountStatus;
import io.swagger.v3.oas.annotations.media.Schema;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Schema(description = "Dados e saldo de uma conta.")
public record AccountResponse(
        @Schema(description = "Identificador da conta.", example = "7d9f3c52-0a1b-4c3d-8e2f-5a6b7c8d9e0f") UUID id,
        @Schema(description = "Nome do titular.", example = "Maria Silva") String ownerName,
        @Schema(description = "CPF do titular, 11 dígitos.", example = "12345678901") String ownerDocument,
        @Schema(description = "Saldo disponível, em reais.", example = "150.00") BigDecimal balance,
        @Schema(description = "Situação da conta.", example = "ACTIVE") AccountStatus status,
        @Schema(description = "Momento da abertura (UTC).", example = "2026-10-21T14:05:00Z") Instant createdAt
) {

    public static AccountResponse from(Account account) {
        return new AccountResponse(
                account.getId(),
                account.getOwnerName(),
                account.getOwnerDocument(),
                account.getBalance(),
                account.getStatus(),
                account.getCreatedAt()
        );
    }
}

package com.bancodigital.account.web.dto;

import com.bancodigital.account.domain.Account;
import io.swagger.v3.oas.annotations.media.Schema;
import java.math.BigDecimal;
import java.util.UUID;

@Schema(description = "Saldo disponível de uma conta.")
public record BalanceResponse(
        @Schema(description = "Identificador da conta.", example = "7d9f3c52-0a1b-4c3d-8e2f-5a6b7c8d9e0f") UUID accountId,
        @Schema(description = "Saldo disponível, em reais.", example = "150.00") BigDecimal balance) {

    public static BalanceResponse from(Account account) {
        return new BalanceResponse(account.getId(), account.getBalance());
    }
}

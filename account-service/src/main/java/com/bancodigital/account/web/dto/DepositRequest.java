package com.bancodigital.account.web.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;

@Schema(description = "Dados de um depósito.")
public record DepositRequest(
        @Schema(description = "Valor a creditar, maior que zero.", example = "150.00") @NotNull BigDecimal valor,
        @Schema(description = "Descrição que aparece no extrato (opcional).", example = "Salário") String description) {
}

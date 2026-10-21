package com.bancodigital.transfer.web.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.util.UUID;

@Schema(description = "Dados de uma transferência entre contas.")
public record CreateTransferRequest(
        @Schema(description = "Conta de origem; deve ser a do usuário autenticado.", example = "7d9f3c52-0a1b-4c3d-8e2f-5a6b7c8d9e0f")
        @NotNull UUID accountOrigemId,
        @Schema(description = "Conta de destino.", example = "3a8e1f20-6c4d-4b5a-9e7f-1d2c3b4a5f60")
        @NotNull UUID accountDestinoId,
        @Schema(description = "Valor a transferir, em reais, no mínimo 0,01.", example = "100.00")
        @NotNull @DecimalMin(value = "0.01") BigDecimal valor
) {
}

package com.bancodigital.transfer.web.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.util.UUID;

public record CreateTransferRequest(
        @NotNull UUID accountOrigemId,
        @NotNull UUID accountDestinoId,
        @NotNull @DecimalMin(value = "0.01") BigDecimal valor
) {
}

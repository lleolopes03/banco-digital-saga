package com.bancodigital.account.web.dto;

import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;

public record DepositRequest(@NotNull BigDecimal valor, String description) {
}

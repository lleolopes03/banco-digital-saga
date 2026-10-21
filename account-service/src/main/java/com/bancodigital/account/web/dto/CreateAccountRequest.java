package com.bancodigital.account.web.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

@Schema(description = "Dados para abrir uma conta.")
public record CreateAccountRequest(
        @Schema(description = "Nome do titular.", example = "Maria Silva") @NotBlank String ownerName,
        @Schema(description = "CPF do titular, 11 dígitos numéricos, único por conta.", example = "12345678901")
        @NotBlank @Pattern(regexp = "\\d{11}", message = "ownerDocument deve conter 11 dígitos numéricos (CPF)")
        String ownerDocument
) {
}

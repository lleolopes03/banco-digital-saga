package com.bancodigital.account.web.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;

@Schema(description = "Credenciais de acesso.")
public record LoginRequest(
        @Schema(description = "E-mail do usuário.", example = "maria@exemplo.com") @NotBlank String email,
        @Schema(description = "Senha do usuário.", example = "senha-forte-123") @NotBlank String senha
) {
}

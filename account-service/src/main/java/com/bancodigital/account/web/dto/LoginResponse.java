package com.bancodigital.account.web.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.UUID;

@Schema(description = "Token de acesso emitido no login.")
public record LoginResponse(
        @Schema(description = "Token JWT (HS256) para o header Authorization: Bearer.") String token,
        @Schema(description = "Conta do usuário autenticado.", example = "7d9f3c52-0a1b-4c3d-8e2f-5a6b7c8d9e0f") UUID accountId,
        @Schema(description = "Validade do token, em segundos.", example = "900") long expiresInSeconds
) {
}

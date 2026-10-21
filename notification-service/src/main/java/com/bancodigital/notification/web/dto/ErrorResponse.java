package com.bancodigital.notification.web.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import java.time.Instant;
import org.springframework.http.HttpStatus;

@Schema(description = "Formato padrão de erro da API, o mesmo para validação, regra de negócio, segurança e limite de uso.")
public record ErrorResponse(
        @Schema(description = "Momento do erro (ISO-8601, UTC).", example = "2026-10-21T14:05:00.123Z")
        String timestamp,
        @Schema(description = "Código de status HTTP.", example = "403")
        int status,
        @Schema(description = "Frase padrão do código HTTP.", example = "Forbidden")
        String error,
        @Schema(description = "Motivo legível do erro.", example = "A conta 7d9f3c52-0a1b-4c3d-8e2f-5a6b7c8d9e0f não pertence ao usuário autenticado.")
        String message
) {

    public static ErrorResponse of(HttpStatus status, String message) {
        return new ErrorResponse(Instant.now().toString(), status.value(), status.getReasonPhrase(), message);
    }
}

package com.bancodigital.transfer.web.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import java.time.Instant;
import org.springframework.http.HttpStatus;

@Schema(description = "Formato padrão de erro da API, o mesmo para validação, regra de negócio, segurança e limite de uso.")
public record ErrorResponse(
        @Schema(description = "Momento do erro (ISO-8601, UTC).", example = "2026-10-21T14:05:00.123Z")
        String timestamp,
        @Schema(description = "Código de status HTTP.", example = "404")
        int status,
        @Schema(description = "Frase padrão do código HTTP.", example = "Not Found")
        String error,
        @Schema(description = "Motivo legível do erro.", example = "Transferência 5b1c2d3e-4f50-4a61-8b72-93a4b5c6d7e8 não encontrada.")
        String message
) {

    public static ErrorResponse of(HttpStatus status, String message) {
        return new ErrorResponse(Instant.now().toString(), status.value(), status.getReasonPhrase(), message);
    }
}

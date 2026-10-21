package com.bancodigital.transfer.web.openapi;

import com.bancodigital.transfer.web.dto.ErrorResponse;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Documenta a resposta 404 no OpenAPI. Existe para não repetir o mesmo bloco em cada endpoint. */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@ApiResponses(@ApiResponse(responseCode = "404", description = "Transferência não encontrada.",
        content = @Content(schema = @Schema(implementation = ErrorResponse.class))))
public @interface RespostaTransferenciaNaoEncontrada {
}

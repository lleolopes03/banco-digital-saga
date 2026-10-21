package com.bancodigital.account.web.openapi;

import com.bancodigital.account.web.dto.ErrorResponse;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Documenta a resposta 403 no OpenAPI. Existe para não repetir o mesmo bloco em cada endpoint. */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@ApiResponses(@ApiResponse(responseCode = "403", description = "A conta informada não pertence ao usuário autenticado.",
        content = @Content(schema = @Schema(implementation = ErrorResponse.class))))
public @interface RespostaAcessoNegado {
}

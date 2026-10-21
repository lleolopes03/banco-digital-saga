package com.bancodigital.transfer.infrastructure.config;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Metadados da spec OpenAPI (Swagger UI em /swagger-ui.html, spec em /v3/api-docs) e o esquema Bearer JWT do botão
 * Authorize. Resumos, respostas e exemplos ficam nas anotações dos controllers e DTOs; o OpenApiContractTest
 * garante que a spec continue igual ao comportamento real.
 */
@Configuration
public class OpenApiConfig {

    private static final String BEARER_SCHEME = "bearerAuth";

    @Bean
    OpenAPI transferServiceOpenApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("transfer-service — Banco Digital Saga")
                        .description("Orquestrador da saga de transferência (POST /api/transfers) e consulta "
                                + "de status. O progresso completo da saga é assíncrono via WebSocket/STOMP — "
                                + "ver README, seção \"Transferência em tempo real\".")
                        .version("v1"))
                .components(new Components().addSecuritySchemes(BEARER_SCHEME,
                        new SecurityScheme()
                                .type(SecurityScheme.Type.HTTP)
                                .scheme("bearer")
                                .bearerFormat("JWT")))
                .addSecurityItem(new SecurityRequirement().addList(BEARER_SCHEME));
    }
}

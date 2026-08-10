package com.bancodigital.transfer.infrastructure.config;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/*
 * Swagger UI em /swagger-ui.html, spec OpenAPI em /v3/api-docs. Duplicado por serviço (mesma
 * convenção já usada para SecurityConfig/JwtService), já que não existe um módulo compartilhado
 * no projeto. A saga em si (o que acontece depois do 202 Accepted) não aparece aqui — isso é
 * assíncrono via Kafka, documentado no README, não numa spec REST.
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

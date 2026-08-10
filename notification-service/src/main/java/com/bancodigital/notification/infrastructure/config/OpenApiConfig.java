package com.bancodigital.notification.infrastructure.config;

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
 * no projeto.
 */
@Configuration
public class OpenApiConfig {

    private static final String BEARER_SCHEME = "bearerAuth";

    @Bean
    OpenAPI notificationServiceOpenApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("notification-service — Banco Digital Saga")
                        .description("Histórico de notificações geradas ao fim de cada saga (sucesso ou "
                                + "falha), consumidas via Kafka de transfer-service — ver README, seção "
                                + "\"Notificações (MongoDB)\".")
                        .version("v1"))
                .components(new Components().addSecuritySchemes(BEARER_SCHEME,
                        new SecurityScheme()
                                .type(SecurityScheme.Type.HTTP)
                                .scheme("bearer")
                                .bearerFormat("JWT")))
                .addSecurityItem(new SecurityRequirement().addList(BEARER_SCHEME));
    }
}

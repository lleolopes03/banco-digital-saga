package com.bancodigital.account.infrastructure.config;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/*
 * Swagger UI em /swagger-ui.html, spec OpenAPI em /v3/api-docs — springdoc gera os dois a partir
 * dos @RestController/DTOs já existentes, sem anotação extra na maioria dos endpoints. O único
 * código deste arquivo é o esquema "Authorize" (Bearer JWT): sem ele, o botão apareceria mas sem
 * indicar que os endpoints exigem token. Duplicado por serviço (mesma convenção já usada para
 * SecurityConfig/JwtService), já que não existe um módulo compartilhado no projeto.
 */
@Configuration
public class OpenApiConfig {

    private static final String BEARER_SCHEME = "bearerAuth";

    @Bean
    OpenAPI accountServiceOpenApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("account-service — Banco Digital Saga")
                        .description("Contas, saldo, depósitos, extrato e autenticação (login/logout). "
                                + "Faça login em POST /api/auth/login, clique em Authorize acima e cole o "
                                + "token para testar os demais endpoints.")
                        .version("v1"))
                .components(new Components().addSecuritySchemes(BEARER_SCHEME,
                        new SecurityScheme()
                                .type(SecurityScheme.Type.HTTP)
                                .scheme("bearer")
                                .bearerFormat("JWT")))
                .addSecurityItem(new SecurityRequirement().addList(BEARER_SCHEME));
    }
}

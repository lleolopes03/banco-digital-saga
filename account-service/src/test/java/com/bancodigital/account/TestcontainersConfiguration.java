package com.bancodigital.account;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@TestConfiguration(proxyBeanMethods = false)
class TestcontainersConfiguration {

	// Kafka e Grafana/OTel não são necessários aqui: este contexto só verifica que a aplicação
	// sobe (contextLoads), sem publicar eventos nem exportar tracing. Postgres é o único
	// obrigatório porque o Flyway roda migrations no startup. Isso também evita concorrência de
	// recursos com o Postgres + Kafka já usados em AbstractIntegrationTest.
	@Bean
	@ServiceConnection
	PostgreSQLContainer postgresContainer() {
		return new PostgreSQLContainer(DockerImageName.parse("postgres:latest"));
	}

}

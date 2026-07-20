package com.bancodigital.antifraud;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@TestConfiguration(proxyBeanMethods = false)
class TestcontainersConfiguration {

	// Kafka, Redis e Grafana/OTel não são necessários aqui: este contexto só verifica que a
	// aplicação sobe (contextLoads). Postgres é o único obrigatório porque o Flyway roda
	// migrations no startup. Isso também evita concorrência de recursos com os containers já
	// usados em AbstractIntegrationTest.
	@Bean
	@ServiceConnection
	PostgreSQLContainer postgresContainer() {
		return new PostgreSQLContainer(DockerImageName.parse("postgres:latest"));
	}

}

package com.bancodigital.account;

import com.bancodigital.account.domain.User;
import com.bancodigital.account.infrastructure.persistence.UserRepository;
import com.bancodigital.account.web.dto.LoginRequest;
import com.bancodigital.account.web.dto.LoginResponse;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@Import(AbstractIntegrationTest.Containers.class)
@AutoConfigureTestRestTemplate
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class AbstractIntegrationTest {

    protected static final String SENHA_PADRAO_TESTE = "senha123";

    @Autowired
    protected UserRepository userRepository;

    @Autowired
    protected PasswordEncoder passwordEncoder;

    /*
     * Provisiona um User vinculado a accountId diretamente via repository (não existe endpoint
     * de signup nesta fatia) e faz login de verdade via /api/auth/login, para que os testes
     * usem um token real gerado pelo fluxo de produção em vez de montar um JWT à mão.
     */
    protected String criarUsuarioEObterToken(TestRestTemplate restTemplate, UUID accountId) {
        String email = "user-" + UUID.randomUUID() + "@teste.com";
        userRepository.save(User.criar(email, passwordEncoder.encode(SENHA_PADRAO_TESTE), accountId));
        LoginResponse response = restTemplate.postForObject("/api/auth/login",
                new LoginRequest(email, SENHA_PADRAO_TESTE), LoginResponse.class);
        return response.token();
    }

    protected <T> HttpEntity<T> autenticado(T body, String token) {
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(token);
        return new HttpEntity<>(body, headers);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class Containers {

        @Bean
        @ServiceConnection
        PostgreSQLContainer postgresContainer() {
            return new PostgreSQLContainer(DockerImageName.parse("postgres:16"));
        }

        @Bean
        @ServiceConnection
        KafkaContainer kafkaContainer() {
            return new KafkaContainer(DockerImageName.parse("apache/kafka-native:latest"));
        }

        @Bean
        @ServiceConnection(name = "redis")
        GenericContainer<?> redisContainer() {
            return new GenericContainer<>(DockerImageName.parse("redis:latest")).withExposedPorts(6379);
        }
    }
}

package com.bancodigital.e2e;

import com.bancodigital.account.AccountApplication;
import com.bancodigital.account.domain.User;
import com.bancodigital.account.infrastructure.persistence.UserRepository;
import com.bancodigital.account.web.dto.AccountResponse;
import com.bancodigital.account.web.dto.BalanceResponse;
import com.bancodigital.account.web.dto.CreateAccountRequest;
import com.bancodigital.account.web.dto.DepositRequest;
import com.bancodigital.account.web.dto.LoginRequest;
import com.bancodigital.account.web.dto.LoginResponse;
import com.bancodigital.account.web.dto.PageResponse;
import com.bancodigital.account.web.dto.TransactionResponse;
import com.bancodigital.antifraud.AntifraudApplication;
import com.bancodigital.transfer.TransferApplication;
import com.bancodigital.transfer.domain.SagaStatus;
import com.bancodigital.transfer.web.dto.CreateTransferRequest;
import com.bancodigital.transfer.web.dto.TransferAcceptedResponse;
import com.bancodigital.transfer.web.dto.TransferResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.client.RestTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/*
 * "Cartão de visita" do projeto: prova, de ponta a ponta e sem nenhuma chamada síncrona entre
 * serviços, que uma transferência percorre os três serviços via Kafka até ficar COMPLETED, com
 * o dinheiro efetivamente saindo da conta de origem e chegando na conta de destino.
 *
 * Como account-service, antifraud-service e transfer-service são projetos Maven independentes
 * (cada um o seu próprio deploy), não há um "modo nativo" de rodar os três dentro do mesmo
 * teste. A abordagem aqui — depender deles como biblioteca e subir cada um como um
 * ApplicationContext Spring separado, na mesma JVM, todos apontando para a mesma infraestrutura
 * (Kafka real + um Postgres por serviço + Redis) via Testcontainers — é o jeito mais fiel de
 * reproduzir automaticamente, em CI, o que rodar os três com `docker-compose up` +
 * `mvn spring-boot:run` faz manualmente.
 *
 * Pré-requisito: os três serviços precisam estar instalados no repositório Maven local
 * (`mvn install` em cada um) antes de rodar este teste — veja o pom.xml deste módulo.
 */
class TransferSagaHappyPathIT {

    private static final Duration TIMEOUT_SAGA = Duration.ofSeconds(30);
    private static final Duration INTERVALO_POLLING = Duration.ofMillis(500);

    private static PostgreSQLContainer accountDb;
    private static PostgreSQLContainer transferDb;
    private static PostgreSQLContainer antifraudDb;
    private static KafkaContainer kafka;
    private static GenericContainer<?> redis;

    private static ConfigurableApplicationContext accountContext;
    private static ConfigurableApplicationContext antifraudContext;
    private static ConfigurableApplicationContext transferContext;

    private static final String ACCOUNT_BASE_URL = "http://localhost:18081";
    private static final String TRANSFER_BASE_URL = "http://localhost:18082";

    private static final RestTemplate restTemplate = new RestTemplate();

    @BeforeAll
    static void subirInfraestruturaETresServicos() {
        accountDb = new PostgreSQLContainer(DockerImageName.parse("postgres:16"));
        transferDb = new PostgreSQLContainer(DockerImageName.parse("postgres:16"));
        antifraudDb = new PostgreSQLContainer(DockerImageName.parse("postgres:16"));
        kafka = new KafkaContainer(DockerImageName.parse("apache/kafka-native:latest"));
        redis = new GenericContainer<>(DockerImageName.parse("redis:latest")).withExposedPorts(6379);

        accountDb.start();
        transferDb.start();
        antifraudDb.start();
        kafka.start();
        redis.start();

        // Desde a Fatia 10 (idempotência), account-service e transfer-service também dependem
        // de Redis (IdempotentEventHandler), não só antifraud-service — por isso a propriedade
        // é setada antes de subir QUALQUER um dos três contexts, não só antes do antifraud.
        System.setProperty("spring.data.redis.host", redis.getHost());
        System.setProperty("spring.data.redis.port", redis.getMappedPort(6379).toString());
        // Property de sistema (não application.yml) de propósito: como só um application.yml
        // "vence" a resolução de classpath (ver javadoc de definirPropriedadesDoServico), sem
        // isso o account-service/transfer-service poderiam subir sem jwt.secret caso o
        // application.yml do antifraud-service (que não declara essa chave) vencesse a corrida.
        System.setProperty("jwt.secret", "banco-digital-saga-jwt-secret-desenvolvimento-nao-usar-em-producao");

        definirPropriedadesDoServico("account-service", "18081", "account", accountDb);
        accountContext = new SpringApplicationBuilder(AccountApplication.class).run();

        definirPropriedadesDoServico("antifraud-service", "18083", "antifraud", antifraudDb);
        antifraudContext = new SpringApplicationBuilder(AntifraudApplication.class).run();

        definirPropriedadesDoServico("transfer-service", "18082", "transfer", transferDb);
        transferContext = new SpringApplicationBuilder(TransferApplication.class).run();
    }

    @AfterAll
    static void pararTresServicosEInfraestrutura() {
        fecharSilenciosamente(transferContext);
        fecharSilenciosamente(antifraudContext);
        fecharSilenciosamente(accountContext);
        pararSilenciosamente(redis);
        pararSilenciosamente(kafka);
        pararSilenciosamente(antifraudDb);
        pararSilenciosamente(transferDb);
        pararSilenciosamente(accountDb);
    }

    /*
     * Como os três serviços entram como dependência no mesmo classpath deste módulo, cada um
     * tem um application.yml no MESMO caminho ("classpath:/application.yml") dentro do seu
     * próprio jar — e a resolução de "classpath:application.yml" do Spring só enxerga UM deles
     * (o primeiro encontrado), não os três. Por isso não dá pra confiar no application.yml de
     * nenhum serviço aqui: toda propriedade que precisa ser diferente por serviço (nome,
     * migrations do Flyway, group-id do Kafka) precisa ser property de sistema, não
     * SpringApplicationBuilder.properties(...) — essas viram "default properties", a MENOR
     * precedência do Spring, perdendo justamente para o application.yml que "venceu" a corrida
     * do classpath. Property de sistema tem precedência bem mais alta que application.yml, e
     * como os três contexts sobem em sequência (cada .run() bloqueia até terminar), não há
     * risco de um contexto vazar a configuração do outro.
     */
    private static void definirPropriedadesDoServico(String nomeAplicacao, String porta,
                                                       String pastaMigrationFlyway, PostgreSQLContainer db) {
        System.setProperty("spring.application.name", nomeAplicacao);
        System.setProperty("spring.kafka.consumer.group-id", nomeAplicacao);
        System.setProperty("spring.flyway.locations", "classpath:db/migration/" + pastaMigrationFlyway);
        System.setProperty("server.port", porta);
        System.setProperty("spring.datasource.url", db.getJdbcUrl());
        System.setProperty("spring.datasource.username", db.getUsername());
        System.setProperty("spring.datasource.password", db.getPassword());
        System.setProperty("spring.kafka.bootstrap-servers", kafka.getBootstrapServers());
    }

    private static void fecharSilenciosamente(ConfigurableApplicationContext context) {
        if (context != null) {
            try {
                context.close();
            } catch (Exception ignored) {
                // encerramento best-effort, não deve derrubar o teste
            }
        }
    }

    private static void pararSilenciosamente(org.testcontainers.lifecycle.Startable container) {
        if (container != null) {
            try {
                container.stop();
            } catch (Exception ignored) {
                // encerramento best-effort, não deve derrubar o teste
            }
        }
    }

    @Test
    void deveCompletarTransferenciaDeContaParaContaAtravesDosTresServicosViaKafka() {
        UUID contaOrigemId = criarConta("Origem Teste", "11122233301");
        String tokenOrigem = obterToken(contaOrigemId);
        depositar(contaOrigemId, new BigDecimal("1000.00"), tokenOrigem);

        UUID contaDestinoId = criarConta("Destino Teste", "11122233302");
        String tokenDestino = obterToken(contaDestinoId);

        UUID transferId = iniciarTransferencia(contaOrigemId, contaDestinoId, new BigDecimal("300.00"), tokenOrigem);

        TransferResponse sagaFinal = aguardarStatusFinal(transferId, tokenOrigem);
        assertThat(sagaFinal.status()).isEqualTo(SagaStatus.COMPLETED);

        BigDecimal saldoOrigem = consultarSaldo(contaOrigemId, tokenOrigem);
        BigDecimal saldoDestino = consultarSaldo(contaDestinoId, tokenDestino);
        assertThat(saldoOrigem).isEqualByComparingTo("700.00");
        assertThat(saldoDestino).isEqualByComparingTo("300.00");

        PageResponse<TransactionResponse> extratoOrigem = consultarExtrato(contaOrigemId, tokenOrigem);
        assertThat(extratoOrigem.conteudo())
                .anyMatch(t -> t.type().name().equals("TRANSFER_OUT") && t.amount().compareTo(new BigDecimal("300.00")) == 0);

        PageResponse<TransactionResponse> extratoDestino = consultarExtrato(contaDestinoId, tokenDestino);
        assertThat(extratoDestino.conteudo())
                .anyMatch(t -> t.type().name().equals("TRANSFER_IN") && t.amount().compareTo(new BigDecimal("300.00")) == 0);
    }

    /*
     * Não há endpoint de signup nesta fatia: um usuário "de abertura de conta" avulso (accountId
     * aleatório, não vinculado a nenhuma conta real) é o suficiente para autenticar
     * POST /api/accounts, já que essa rota não é escopada por accountId — só as rotas com
     * {id} no caminho (balance/deposits/statement) e o accountOrigemId de POST /api/transfers
     * exigem que o token pertença especificamente àquela conta.
     */
    private UUID criarConta(String ownerName, String ownerDocument) {
        String tokenAbertura = obterToken(UUID.randomUUID());
        ResponseEntity<AccountResponse> response = restTemplate.exchange(ACCOUNT_BASE_URL + "/api/accounts",
                HttpMethod.POST, autenticado(new CreateAccountRequest(ownerName, ownerDocument), tokenAbertura),
                AccountResponse.class);
        return response.getBody().id();
    }

    private String obterToken(UUID accountId) {
        UserRepository userRepository = accountContext.getBean(UserRepository.class);
        PasswordEncoder passwordEncoder = accountContext.getBean(PasswordEncoder.class);
        String email = "e2e-" + UUID.randomUUID() + "@teste.com";
        String senha = "senha123";
        userRepository.save(User.criar(email, passwordEncoder.encode(senha), accountId));

        ResponseEntity<LoginResponse> response = restTemplate.postForEntity(ACCOUNT_BASE_URL + "/api/auth/login",
                new LoginRequest(email, senha), LoginResponse.class);
        return response.getBody().token();
    }

    private <T> HttpEntity<T> autenticado(T body, String token) {
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(token);
        return new HttpEntity<>(body, headers);
    }

    private void depositar(UUID accountId, BigDecimal valor, String token) {
        restTemplate.exchange(ACCOUNT_BASE_URL + "/api/accounts/" + accountId + "/deposits", HttpMethod.POST,
                autenticado(new DepositRequest(valor, "depósito inicial para teste E2E"), token), AccountResponse.class);
    }

    private UUID iniciarTransferencia(UUID origemId, UUID destinoId, BigDecimal valor, String token) {
        ResponseEntity<TransferAcceptedResponse> response = restTemplate.exchange(TRANSFER_BASE_URL + "/api/transfers",
                HttpMethod.POST, autenticado(new CreateTransferRequest(origemId, destinoId, valor), token),
                TransferAcceptedResponse.class);
        return response.getBody().transferId();
    }

    private BigDecimal consultarSaldo(UUID accountId, String token) {
        ResponseEntity<BalanceResponse> response = restTemplate.exchange(
                ACCOUNT_BASE_URL + "/api/accounts/" + accountId + "/balance", HttpMethod.GET, autenticado(null, token),
                BalanceResponse.class);
        return response.getBody().balance();
    }

    private PageResponse<TransactionResponse> consultarExtrato(UUID accountId, String token) {
        ResponseEntity<PageResponse<TransactionResponse>> response = restTemplate.exchange(
                ACCOUNT_BASE_URL + "/api/accounts/" + accountId + "/statement", HttpMethod.GET, autenticado(null, token),
                new ParameterizedTypeReference<PageResponse<TransactionResponse>>() {
                });
        return response.getBody();
    }

    private TransferResponse aguardarStatusFinal(UUID transferId, String token) {
        Instant prazo = Instant.now().plus(TIMEOUT_SAGA);
        TransferResponse ultimaResposta = null;
        while (Instant.now().isBefore(prazo)) {
            ultimaResposta = restTemplate.exchange(TRANSFER_BASE_URL + "/api/transfers/" + transferId, HttpMethod.GET,
                    autenticado(null, token), TransferResponse.class).getBody();
            if (ultimaResposta.status() == SagaStatus.COMPLETED || ultimaResposta.status() == SagaStatus.FAILED) {
                return ultimaResposta;
            }
            sleep(INTERVALO_POLLING);
        }
        fail("Saga " + transferId + " não chegou a um estado final dentro do timeout. Último status observado: "
                + (ultimaResposta != null ? ultimaResposta.status() : "nenhum"));
        return null;
    }

    private void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}

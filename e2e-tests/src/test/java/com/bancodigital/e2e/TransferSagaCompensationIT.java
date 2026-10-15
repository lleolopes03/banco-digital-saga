package com.bancodigital.e2e;

import com.bancodigital.account.AccountApplication;
import com.bancodigital.account.domain.Account;
import com.bancodigital.account.domain.User;
import com.bancodigital.account.infrastructure.persistence.AccountRepository;
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
import com.bancodigital.transfer.application.SagaTimeoutScheduler;
import com.bancodigital.transfer.domain.SagaEvent;
import com.bancodigital.transfer.domain.SagaInstance;
import com.bancodigital.transfer.domain.SagaStateMachine;
import com.bancodigital.transfer.domain.SagaStatus;
import com.bancodigital.transfer.infrastructure.persistence.SagaInstanceRepository;
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/*
 * Cobre os três cenários de compensação da saga de transferência (Fatia 9): antifraude
 * rejeitando, crédito falhando por conta destino bloqueada, e saga travada detectada por
 * timeout. Mesma abordagem de infraestrutura que TransferSagaHappyPathIT — os três serviços
 * sobem como ApplicationContexts Spring independentes na mesma JVM, contra Kafka/Postgres
 * reais via Testcontainers — mas aqui a asserção central não é "chegou em COMPLETED", é
 * "o dinheiro voltou e ficou rastro auditável no extrato".
 *
 * Pré-requisito idêntico ao da suíte feliz: os três serviços precisam estar instalados no
 * repositório Maven local (mvn install em cada um) antes de rodar este teste.
 */
class TransferSagaCompensationIT {

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

    private static final String ACCOUNT_BASE_URL = "http://localhost:18091";
    private static final String TRANSFER_BASE_URL = "http://localhost:18092";

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

        definirPropriedadesDoServico("account-service", "18091", "account", accountDb);
        accountContext = new SpringApplicationBuilder(AccountApplication.class).run();

        definirPropriedadesDoServico("antifraud-service", "18093", "antifraud", antifraudDb);
        antifraudContext = new SpringApplicationBuilder(AntifraudApplication.class).run();

        definirPropriedadesDoServico("transfer-service", "18092", "transfer", transferDb);
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
    void deveReverterReservaEFalharSagaQuandoAntifraudeRejeitaTransferencia() {
        UUID contaOrigemId = criarConta("Origem Antifraude", "22233344401");
        String tokenOrigem = obterToken(contaOrigemId);
        depositar(contaOrigemId, new BigDecimal("20000.00"), tokenOrigem);
        UUID contaDestinoId = criarConta("Destino Antifraude", "22233344402");

        // Acima de LimiteValorRule.LIMITE (10000): antifraud-service rejeita com LIMITE_EXCEDIDO.
        UUID transferId = iniciarTransferencia(contaOrigemId, contaDestinoId, new BigDecimal("15000.00"), tokenOrigem);

        TransferResponse sagaFinal = aguardarStatusFinal(transferId, tokenOrigem);
        assertThat(sagaFinal.status()).isEqualTo(SagaStatus.FAILED);
        assertThat(sagaFinal.failureReason()).isEqualTo("LIMITE_EXCEDIDO");

        assertThat(consultarSaldo(contaOrigemId, tokenOrigem)).isEqualByComparingTo("20000.00");

        PageResponse<TransactionResponse> extratoOrigem = consultarExtrato(contaOrigemId, tokenOrigem);
        assertThat(extratoOrigem.conteudo())
                .anyMatch(t -> t.type().name().equals("REVERSAL") && t.amount().compareTo(new BigDecimal("15000.00")) == 0);
    }

    @Test
    void deveReverterDebitoQuandoContaDestinoEstaBloqueada() {
        UUID contaOrigemId = criarConta("Origem Bloqueio", "22233344403");
        String tokenOrigem = obterToken(contaOrigemId);
        depositar(contaOrigemId, new BigDecimal("1000.00"), tokenOrigem);
        UUID contaDestinoId = criarConta("Destino Bloqueado", "22233344404");
        bloquearConta(contaDestinoId);

        UUID transferId = iniciarTransferencia(contaOrigemId, contaDestinoId, new BigDecimal("300.00"), tokenOrigem);

        TransferResponse sagaFinal = aguardarStatusFinal(transferId, tokenOrigem);
        assertThat(sagaFinal.status()).isEqualTo(SagaStatus.FAILED);
        assertThat(sagaFinal.failureReason()).isEqualTo("CONTA_DESTINO_BLOCKED");

        assertThat(consultarSaldo(contaOrigemId, tokenOrigem)).isEqualByComparingTo("1000.00");

        PageResponse<TransactionResponse> extratoOrigem = consultarExtrato(contaOrigemId, tokenOrigem);
        assertThat(extratoOrigem.conteudo())
                .anyMatch(t -> t.type().name().equals("REVERSAL") && t.amount().compareTo(new BigDecimal("300.00")) == 0);
    }

    @Test
    void deveDetectarSagaTravadaEDispararCompensacaoViaTimeout() throws Exception {
        UUID contaOrigemId = criarConta("Origem Timeout", "22233344405");
        String tokenOrigem = obterToken(contaOrigemId);
        depositar(contaOrigemId, new BigDecimal("500.00"), tokenOrigem);
        UUID contaDestinoId = criarConta("Destino Timeout", "22233344406");
        BigDecimal valor = new BigDecimal("200.00");

        // Trava a saga manualmente em RESERVED: reserva o saldo na origem e persiste a saga
        // nesse estado via transições reais da própria state machine (sem passar pelo fluxo
        // Kafka), simulando um evento TransferReserved que nunca chegou ao antifraud-service.
        reservarSaldoDiretamente(contaOrigemId, valor);
        UUID transferId = criarSagaTravadaEmReserved(contaOrigemId, contaDestinoId, valor);
        backdateUpdatedAt(transferId);

        transferContext.getBean(SagaTimeoutScheduler.class).detectarECompensarSagasTravadas();

        TransferResponse sagaFinal = aguardarStatusFinal(transferId, tokenOrigem);
        assertThat(sagaFinal.status()).isEqualTo(SagaStatus.FAILED);
        assertThat(sagaFinal.failureReason()).isEqualTo("SAGA_TIMEOUT");

        assertThat(consultarSaldo(contaOrigemId, tokenOrigem)).isEqualByComparingTo("500.00");

        PageResponse<TransactionResponse> extratoOrigem = consultarExtrato(contaOrigemId, tokenOrigem);
        assertThat(extratoOrigem.conteudo())
                .anyMatch(t -> t.type().name().equals("REVERSAL") && t.amount().compareTo(valor) == 0);
    }

    /*
     * A reserva falha por saldo insuficiente no account-service (nesta harness a pre-checagem
     * sincrona de saldo do transfer-service nao enxerga o account-service, entao a saga abre e a
     * reserva e que recusa). Antes do tratamento de TransferReservationFailed, a saga ficava em
     * STARTED ate o SagaTimeoutScheduler falha-la como SAGA_TIMEOUT 2 minutos depois — fora da
     * janela de 30s deste teste. Agora ela falha na hora, com o motivo REAL.
     */
    @Test
    void deveFalharRapidoComMotivoRealQuandoSaldoEInsuficienteNaReserva() {
        UUID contaOrigemId = criarConta("Origem Sem Saldo", "22233344407");
        String tokenOrigem = obterToken(contaOrigemId);
        depositar(contaOrigemId, new BigDecimal("100.00"), tokenOrigem);
        UUID contaDestinoId = criarConta("Destino Sem Saldo", "22233344408");

        UUID transferId = iniciarTransferencia(contaOrigemId, contaDestinoId, new BigDecimal("500.00"), tokenOrigem);

        TransferResponse sagaFinal = aguardarStatusFinal(transferId, tokenOrigem);
        assertThat(sagaFinal.status()).isEqualTo(SagaStatus.FAILED);
        assertThat(sagaFinal.failureReason()).isEqualTo("SALDO_INSUFICIENTE");
        assertThat(consultarSaldo(contaOrigemId, tokenOrigem)).isEqualByComparingTo("100.00");
    }

    private void bloquearConta(UUID contaId) {
        AccountRepository accountRepository = accountContext.getBean(AccountRepository.class);
        Account conta = accountRepository.findById(contaId).orElseThrow();
        conta.bloquear();
        accountRepository.saveAndFlush(conta);
    }

    private void reservarSaldoDiretamente(UUID contaId, BigDecimal valor) {
        AccountRepository accountRepository = accountContext.getBean(AccountRepository.class);
        Account conta = accountRepository.findById(contaId).orElseThrow();
        conta.reservar(valor);
        accountRepository.saveAndFlush(conta);
    }

    private UUID criarSagaTravadaEmReserved(UUID contaOrigemId, UUID contaDestinoId, BigDecimal valor) {
        UUID transferId = UUID.randomUUID();
        SagaInstance saga = SagaInstance.iniciar(transferId, contaOrigemId, contaDestinoId, valor);
        new SagaStateMachine().transicionar(saga, SagaEvent.RESERVAR_SALDO);

        SagaInstanceRepository sagaInstanceRepository = transferContext.getBean(SagaInstanceRepository.class);
        sagaInstanceRepository.saveAndFlush(saga);
        return transferId;
    }

    private void backdateUpdatedAt(UUID transferId) throws Exception {
        try (Connection conn = DriverManager.getConnection(transferDb.getJdbcUrl(), transferDb.getUsername(),
                transferDb.getPassword());
             PreparedStatement ps = conn.prepareStatement(
                     "UPDATE saga_instances SET updated_at = now() - interval '5 minutes' WHERE id = ?")) {
            ps.setObject(1, transferId);
            ps.executeUpdate();
        }
    }

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

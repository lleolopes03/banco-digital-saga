package com.bancodigital.transfer.infrastructure.client;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.assertj.core.api.Assertions.assertThat;

import com.bancodigital.transfer.AbstractIntegrationTest;
import com.bancodigital.transfer.web.dto.CreateTransferRequest;
import com.bancodigital.transfer.web.dto.TransferAcceptedResponse;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.math.BigDecimal;
import java.util.UUID;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/*
 * Os três testes rodam em ordem e compartilham o mesmo contexto Spring (e portanto o mesmo
 * CircuitBreaker "accountService"): juntos eles contam a história completa do circuito ao longo
 * do tempo — fechado, abre após falhas consecutivas, e volta a half-open depois do
 * wait-duration-in-open-state — exatamente como se comportaria em produção. Não resetamos o
 * estado do circuito entre os métodos de propósito.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TransferServiceCircuitBreakerTest extends AbstractIntegrationTest {

    @RegisterExtension
    static WireMockExtension accountServiceMock = WireMockExtension.newInstance()
            .options(com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig().dynamicPort())
            .build();

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private CircuitBreakerRegistry circuitBreakerRegistry;

    @DynamicPropertySource
    static void accountServiceBaseUrl(DynamicPropertyRegistry registry) {
        registry.add("account-service.base-url", () -> "http://localhost:" + accountServiceMock.getPort());
    }

    private CircuitBreaker circuitBreaker() {
        return circuitBreakerRegistry.circuitBreaker("accountService");
    }

    private void stubBalanceOk() {
        accountServiceMock.stubFor(get(urlPathMatching("/api/accounts/.*/balance"))
                .willReturn(aResponse().withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"accountId\":\"" + UUID.randomUUID() + "\",\"balance\":1000.00}")));
    }

    private void stubBalanceError() {
        accountServiceMock.stubFor(get(urlPathMatching("/api/accounts/.*/balance"))
                .willReturn(aResponse().withStatus(500)));
    }

    private ResponseEntity<TransferAcceptedResponse> postarTransferencia() {
        UUID accountOrigemId = UUID.randomUUID();
        CreateTransferRequest request = new CreateTransferRequest(accountOrigemId, UUID.randomUUID(),
                new BigDecimal("100.00"));
        return restTemplate.exchange("/api/transfers", HttpMethod.POST, autenticado(request, accountOrigemId),
                TransferAcceptedResponse.class);
    }

    @Test
    @Order(1)
    void chamadaSincronaFuncionaSemAcionarFallbackQuandoAccountServiceRespondeNormalmente() {
        stubBalanceOk();

        ResponseEntity<TransferAcceptedResponse> response = postarTransferencia();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody().transferId()).isNotNull();
        assertThat(circuitBreaker().getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        assertThat(circuitBreaker().getMetrics().getNumberOfFailedCalls()).isZero();
        accountServiceMock.verify(1, getRequestedFor(urlPathMatching("/api/accounts/.*/balance")));
    }

    @Test
    @Order(2)
    void falhasConsecutivasAbremOCircuitoEFallbackDeixaASagaSeguir() {
        // Reseta a janela deslizante para que a chamada de sucesso do teste anterior não conte
        // para a taxa de falha calculada aqui — este teste quer uma janela limpa de 10 chamadas.
        circuitBreaker().reset();
        accountServiceMock.resetRequests();
        stubBalanceError();

        // sliding-window-size e minimum-number-of-calls estão configurados em 10: com 100% de
        // falha, o circuito abre exatamente na décima chamada.
        for (int i = 0; i < 10; i++) {
            ResponseEntity<TransferAcceptedResponse> response = postarTransferencia();
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        }
        assertThat(circuitBreaker().getState()).isEqualTo(CircuitBreaker.State.OPEN);
        accountServiceMock.verify(10, getRequestedFor(urlPathMatching("/api/accounts/.*/balance")));

        // Com o circuito aberto, o Resilience4j nem tenta a chamada real: aciona o fallback
        // diretamente. A saga, mesmo assim, é criada normalmente.
        ResponseEntity<TransferAcceptedResponse> comCircuitoAberto = postarTransferencia();
        assertThat(comCircuitoAberto.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(comCircuitoAberto.getBody().transferId()).isNotNull();
        accountServiceMock.verify(10, getRequestedFor(urlPathMatching("/api/accounts/.*/balance")));
    }

    @Test
    @Order(3)
    void circuitoVoltaAHalfOpenAposWaitDurationETentaChamadaRealNovamente() throws InterruptedException {
        assertThat(circuitBreaker().getState()).isEqualTo(CircuitBreaker.State.OPEN);

        accountServiceMock.resetRequests();
        stubBalanceOk();

        // wait-duration-in-open-state é 10s. A transição OPEN -> HALF_OPEN é avaliada de forma
        // preguiçosa na próxima tentativa de chamada (não há timer em background por padrão), por
        // isso esperamos um pouco além do configurado antes de tentar de novo.
        Thread.sleep(10_500);

        ResponseEntity<TransferAcceptedResponse> response = postarTransferencia();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(circuitBreaker().getState())
                .isIn(CircuitBreaker.State.HALF_OPEN, CircuitBreaker.State.CLOSED);
        // O ponto central deste teste: a chamada real ao account-service (mockado) foi tentada
        // de novo, em vez de cair direto no fallback.
        accountServiceMock.verify(1, getRequestedFor(urlPathMatching("/api/accounts/.*/balance")));
    }
}

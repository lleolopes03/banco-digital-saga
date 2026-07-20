package com.bancodigital.transfer.infrastructure.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import com.bancodigital.transfer.AbstractIntegrationTest;
import com.bancodigital.transfer.web.dto.CreateTransferRequest;
import java.math.BigDecimal;
import java.util.UUID;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/*
 * A janela de produção é 60s (rate-limit.transfer.window em application.yml); aqui ela é
 * sobrescrita para 3s só para o teste "volta a aceitar após expirar" não precisar esperar um
 * minuto inteiro — o código exercitado é exatamente o mesmo, só o parâmetro de configuração
 * muda. O limite de 10 requisições continua o mesmo do requisito.
 *
 * Testes ordenados de propósito, reaproveitando a MESMA accountOrigemId (e portanto a mesma
 * chave no Redis) para contar a história completa da janela: 10 passam -> 11ª é limitada -> após
 * expirar, volta a aceitar.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RateLimitFilterTest extends AbstractIntegrationTest {

    private static final UUID ACCOUNT_ORIGEM_ID = UUID.randomUUID();

    @Autowired
    private TestRestTemplate restTemplate;

    @DynamicPropertySource
    static void rateLimitWindow(DynamicPropertyRegistry registry) {
        registry.add("rate-limit.transfer.window", () -> "3s");
    }

    private ResponseEntity<String> postarTransferencia() {
        CreateTransferRequest request = new CreateTransferRequest(ACCOUNT_ORIGEM_ID, UUID.randomUUID(),
                new BigDecimal("10.00"));
        return restTemplate.exchange("/api/transfers", HttpMethod.POST, autenticado(request, ACCOUNT_ORIGEM_ID),
                String.class);
    }

    @Test
    @Order(1)
    void dezRequisicoesDentroDaJanelaTodasPassamEA11aEhLimitada() {
        for (int i = 0; i < 10; i++) {
            ResponseEntity<String> response = postarTransferencia();
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        }

        ResponseEntity<String> decimaPrimeira = postarTransferencia();

        assertThat(decimaPrimeira.getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(decimaPrimeira.getHeaders().getFirst("Retry-After")).isNotNull();
        assertThat(decimaPrimeira.getBody()).contains("Limite de 10 transferências");
    }

    @Test
    @Order(2)
    void aposExpirarAJanelaVoltaAAceitarNovasRequisicoes() throws InterruptedException {
        ResponseEntity<String> aindaLimitada = postarTransferencia();
        assertThat(aindaLimitada.getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);

        Thread.sleep(3_500); // janela (sobrescrita para 3s neste teste) expira

        ResponseEntity<String> response = postarTransferencia();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
    }
}

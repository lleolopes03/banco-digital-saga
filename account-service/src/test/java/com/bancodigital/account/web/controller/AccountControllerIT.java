package com.bancodigital.account.web.controller;

import static org.assertj.core.api.Assertions.assertThat;

import com.bancodigital.account.AbstractIntegrationTest;
import com.bancodigital.account.domain.TransactionType;
import com.bancodigital.account.web.dto.AccountResponse;
import com.bancodigital.account.web.dto.BalanceResponse;
import com.bancodigital.account.web.dto.CreateAccountRequest;
import com.bancodigital.account.web.dto.DepositRequest;
import com.bancodigital.account.web.dto.PageResponse;
import com.bancodigital.account.web.dto.TransactionResponse;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

class AccountControllerIT extends AbstractIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    // POST /api/accounts não é escopado por accountId (não existe conta ainda), então basta um
    // usuário autenticado qualquer — não precisa ser dono da conta sendo criada.
    private String tokenAbertura() {
        return criarUsuarioEObterToken(restTemplate, UUID.randomUUID());
    }

    private UUID criarConta(String ownerName, String ownerDocument) {
        ResponseEntity<AccountResponse> response = restTemplate.exchange("/api/accounts", HttpMethod.POST,
                autenticado(new CreateAccountRequest(ownerName, ownerDocument), tokenAbertura()), AccountResponse.class);
        return response.getBody().id();
    }

    @Test
    void devePersistirContaERetornar201ComLocationHeader() {
        ResponseEntity<AccountResponse> response = restTemplate.exchange("/api/accounts", HttpMethod.POST,
                autenticado(new CreateAccountRequest("Maria Silva", "12345678901"), tokenAbertura()), AccountResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getHeaders().getLocation()).isNotNull();
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().balance()).isEqualByComparingTo(BigDecimal.ZERO);
    }

    @Test
    void deveRetornarSaldoCorretoDaContaCriada() {
        UUID contaId = criarConta("João Souza", "98765432100");
        String token = criarUsuarioEObterToken(restTemplate, contaId);

        ResponseEntity<BalanceResponse> response = restTemplate.exchange("/api/accounts/" + contaId + "/balance",
                HttpMethod.GET, autenticado(null, token), BalanceResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().accountId()).isEqualTo(contaId);
        assertThat(response.getBody().balance()).isEqualByComparingTo(BigDecimal.ZERO);
    }

    @Test
    void devePersistirDepositoERetornar200ComNovoSaldo() {
        UUID contaId = criarConta("Carlos Alberto", "45678912300");
        String token = criarUsuarioEObterToken(restTemplate, contaId);

        DepositRequest depositRequest = new DepositRequest(new BigDecimal("200.00"), "depósito inicial");
        ResponseEntity<AccountResponse> response = restTemplate.exchange("/api/accounts/" + contaId + "/deposits",
                HttpMethod.POST, autenticado(depositRequest, token), AccountResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().balance()).isEqualByComparingTo("200.00");
    }

    @Test
    void deveRetornar404AoDepositarEmContaInexistente() {
        UUID contaInexistente = UUID.randomUUID();
        String token = criarUsuarioEObterToken(restTemplate, contaInexistente);
        DepositRequest depositRequest = new DepositRequest(new BigDecimal("50.00"), null);

        ResponseEntity<String> response = restTemplate.exchange("/api/accounts/" + contaInexistente + "/deposits",
                HttpMethod.POST, autenticado(depositRequest, token), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void deveRetornarExtratoPaginadoCorretamente() {
        UUID contaId = criarConta("Extrato Um", "11111111111");
        String token = criarUsuarioEObterToken(restTemplate, contaId);

        restTemplate.exchange("/api/accounts/" + contaId + "/deposits", HttpMethod.POST,
                autenticado(new DepositRequest(new BigDecimal("100.00"), "primeiro"), token), AccountResponse.class);
        restTemplate.exchange("/api/accounts/" + contaId + "/deposits", HttpMethod.POST,
                autenticado(new DepositRequest(new BigDecimal("50.00"), "segundo"), token), AccountResponse.class);

        ResponseEntity<PageResponse<TransactionResponse>> response = restTemplate.exchange(
                "/api/accounts/" + contaId + "/statement", HttpMethod.GET, autenticado(null, token),
                new ParameterizedTypeReference<PageResponse<TransactionResponse>>() {
                });

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        PageResponse<TransactionResponse> body = response.getBody();
        assertThat(body).isNotNull();
        assertThat(body.conteudo()).hasSize(2);
        assertThat(body.totalElementos()).isEqualTo(2);
        assertThat(body.paginaAtual()).isZero();
        assertThat(body.tamanhoPagina()).isEqualTo(20);
        assertThat(body.primeira()).isTrue();
        assertThat(body.ultima()).isTrue();
        assertThat(body.conteudo()).allMatch(transaction -> transaction.type() == TransactionType.DEPOSIT);
    }

    @Test
    void deveFiltrarExtratoPorPeriodoInformado() {
        UUID contaId = criarConta("Extrato Periodo", "22222222222");
        String token = criarUsuarioEObterToken(restTemplate, contaId);

        restTemplate.exchange("/api/accounts/" + contaId + "/deposits", HttpMethod.POST,
                autenticado(new DepositRequest(new BigDecimal("75.00"), "depósito de hoje"), token), AccountResponse.class);

        LocalDate hoje = LocalDate.now();
        ResponseEntity<PageResponse<TransactionResponse>> dentroDoPeriodo = restTemplate.exchange(
                "/api/accounts/" + contaId + "/statement?de=" + hoje + "&ate=" + hoje, HttpMethod.GET,
                autenticado(null, token), new ParameterizedTypeReference<PageResponse<TransactionResponse>>() {
                });
        assertThat(dentroDoPeriodo.getBody()).isNotNull();
        assertThat(dentroDoPeriodo.getBody().conteudo()).hasSize(1);

        LocalDate foraDoPeriodoInicio = hoje.minusYears(1);
        LocalDate foraDoPeriodoFim = foraDoPeriodoInicio.plusDays(1);
        ResponseEntity<PageResponse<TransactionResponse>> foraDoPeriodo = restTemplate.exchange(
                "/api/accounts/" + contaId + "/statement?de=" + foraDoPeriodoInicio + "&ate=" + foraDoPeriodoFim,
                HttpMethod.GET, autenticado(null, token),
                new ParameterizedTypeReference<PageResponse<TransactionResponse>>() {
                });
        assertThat(foraDoPeriodo.getBody()).isNotNull();
        assertThat(foraDoPeriodo.getBody().conteudo()).isEmpty();
    }
}

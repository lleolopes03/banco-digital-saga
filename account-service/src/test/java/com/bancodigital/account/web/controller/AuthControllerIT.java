package com.bancodigital.account.web.controller;

import static org.assertj.core.api.Assertions.assertThat;

import com.bancodigital.account.AbstractIntegrationTest;
import com.bancodigital.account.domain.User;
import com.bancodigital.account.web.dto.BalanceResponse;
import com.bancodigital.account.web.dto.LoginRequest;
import com.bancodigital.account.web.dto.LoginResponse;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

class AuthControllerIT extends AbstractIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void deveLogarComCredenciaisValidasERetornar200ComToken() {
        UUID accountId = UUID.randomUUID();
        String email = "usuario-valido@teste.com";
        userRepository.save(User.criar(email, passwordEncoder.encode(SENHA_PADRAO_TESTE), accountId));

        ResponseEntity<LoginResponse> response = restTemplate.postForEntity("/api/auth/login",
                new LoginRequest(email, SENHA_PADRAO_TESTE), LoginResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().token()).isNotBlank();
        assertThat(response.getBody().accountId()).isEqualTo(accountId);
    }

    @Test
    void deveRetornar401ParaSenhaErrada() {
        UUID accountId = UUID.randomUUID();
        String email = "usuario-senha-errada@teste.com";
        userRepository.save(User.criar(email, passwordEncoder.encode(SENHA_PADRAO_TESTE), accountId));

        ResponseEntity<String> response = restTemplate.postForEntity("/api/auth/login",
                new LoginRequest(email, "senha-incorreta"), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void logoutDeveInvalidarTokenParaChamadasSubsequentes() {
        UUID accountId = UUID.randomUUID();
        String token = criarUsuarioEObterToken(restTemplate, accountId);

        // Confirma que o token funciona antes do logout.
        ResponseEntity<BalanceResponse> antesDoLogout = restTemplate.exchange("/api/accounts/" + accountId + "/balance",
                HttpMethod.GET, autenticado(null, token), BalanceResponse.class);
        assertThat(antesDoLogout.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);

        ResponseEntity<Void> logoutResponse = restTemplate.exchange("/api/auth/logout", HttpMethod.POST,
                autenticado(null, token), Void.class);
        assertThat(logoutResponse.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);

        ResponseEntity<String> depoisDoLogout = restTemplate.exchange("/api/accounts/" + accountId + "/balance",
                HttpMethod.GET, autenticado(null, token), String.class);
        assertThat(depoisDoLogout.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }
}

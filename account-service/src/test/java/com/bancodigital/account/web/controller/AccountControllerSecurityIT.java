package com.bancodigital.account.web.controller;

import static org.assertj.core.api.Assertions.assertThat;

import com.bancodigital.account.AbstractIntegrationTest;
import com.bancodigital.account.domain.Account;
import com.bancodigital.account.infrastructure.persistence.AccountRepository;
import com.bancodigital.account.web.dto.BalanceResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

class AccountControllerSecurityIT extends AbstractIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private AccountRepository accountRepository;

    @Test
    void deveRetornar200AoAcessarSaldoDaPropriaConta() {
        Account minhaConta = accountRepository.save(Account.abrir("Dono da Conta", "33344455501"));
        String token = criarUsuarioEObterToken(restTemplate, minhaConta.getId());

        ResponseEntity<BalanceResponse> response = restTemplate.exchange(
                "/api/accounts/" + minhaConta.getId() + "/balance", HttpMethod.GET, autenticado(null, token),
                BalanceResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().accountId()).isEqualTo(minhaConta.getId());
    }

    @Test
    void deveRetornar403AoAcessarSaldoDeOutraConta() {
        Account minhaConta = accountRepository.save(Account.abrir("Dono da Conta", "33344455502"));
        Account contaAlheia = accountRepository.save(Account.abrir("Outra Pessoa", "33344455503"));
        String token = criarUsuarioEObterToken(restTemplate, minhaConta.getId());

        ResponseEntity<String> response = restTemplate.exchange(
                "/api/accounts/" + contaAlheia.getId() + "/balance", HttpMethod.GET, autenticado(null, token),
                String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
    }

    @Test
    void deveRetornar401AoAcessarSaldoSemToken() {
        Account conta = accountRepository.save(Account.abrir("Sem Token", "33344455504"));

        ResponseEntity<String> response = restTemplate.getForEntity(
                "/api/accounts/" + conta.getId() + "/balance", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }
}

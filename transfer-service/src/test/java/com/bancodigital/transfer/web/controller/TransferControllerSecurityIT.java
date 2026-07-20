package com.bancodigital.transfer.web.controller;

import static org.assertj.core.api.Assertions.assertThat;

import com.bancodigital.transfer.AbstractIntegrationTest;
import com.bancodigital.transfer.web.dto.CreateTransferRequest;
import java.math.BigDecimal;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

class TransferControllerSecurityIT extends AbstractIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void deveRetornar403QuandoAccountOrigemIdDoBodyDivergeDoAccountIdDoToken() {
        UUID accountIdDoToken = UUID.randomUUID();
        UUID accountOrigemIdDiferente = UUID.randomUUID();
        CreateTransferRequest request = new CreateTransferRequest(accountOrigemIdDiferente, UUID.randomUUID(),
                new BigDecimal("100.00"));

        ResponseEntity<String> response = restTemplate.postForEntity("/api/transfers",
                autenticado(request, accountIdDoToken), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
    }

    @Test
    void deveAceitarTransferenciaQuandoAccountOrigemIdCoincideComAccountIdDoToken() {
        UUID accountOrigemId = UUID.randomUUID();
        CreateTransferRequest request = new CreateTransferRequest(accountOrigemId, UUID.randomUUID(),
                new BigDecimal("100.00"));

        ResponseEntity<String> response = restTemplate.postForEntity("/api/transfers",
                autenticado(request, accountOrigemId), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
    }

    @Test
    void deveRetornar401QuandoRequisicaoNaoTemToken() {
        CreateTransferRequest request = new CreateTransferRequest(UUID.randomUUID(), UUID.randomUUID(),
                new BigDecimal("100.00"));

        ResponseEntity<String> response = restTemplate.postForEntity("/api/transfers", request, String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }
}

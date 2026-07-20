package com.bancodigital.transfer.web.controller;

import static org.assertj.core.api.Assertions.assertThat;

import com.bancodigital.transfer.AbstractIntegrationTest;
import com.bancodigital.transfer.infrastructure.security.TestJwtFactory;
import com.bancodigital.transfer.web.dto.CreateTransferRequest;
import com.bancodigital.transfer.web.dto.TransferAcceptedResponse;
import java.math.BigDecimal;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;

class TransferControllerIT extends AbstractIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void devePostarDuasVezesComMesmaIdempotencyKeyERetornarOMesmoTransferId() {
        UUID accountOrigemId = UUID.randomUUID();
        CreateTransferRequest request = new CreateTransferRequest(accountOrigemId, UUID.randomUUID(),
                new BigDecimal("300.00"));
        HttpHeaders headers = new HttpHeaders();
        headers.set("Idempotency-Key", "chave-idempotencia-teste-" + UUID.randomUUID());
        headers.setBearerAuth(TestJwtFactory.tokenPara(accountOrigemId));
        HttpEntity<CreateTransferRequest> entity = new HttpEntity<>(request, headers);

        ResponseEntity<TransferAcceptedResponse> primeira = restTemplate.postForEntity("/api/transfers", entity,
                TransferAcceptedResponse.class);
        ResponseEntity<TransferAcceptedResponse> segunda = restTemplate.postForEntity("/api/transfers", entity,
                TransferAcceptedResponse.class);

        assertThat(primeira.getBody().transferId()).isEqualTo(segunda.getBody().transferId());
    }

    @Test
    void devePostarDuasTransferenciasDistintasQuandoSemIdempotencyKey() {
        UUID accountOrigemId = UUID.randomUUID();
        CreateTransferRequest request = new CreateTransferRequest(accountOrigemId, UUID.randomUUID(),
                new BigDecimal("300.00"));

        ResponseEntity<TransferAcceptedResponse> primeira = restTemplate.postForEntity("/api/transfers",
                autenticado(request, accountOrigemId), TransferAcceptedResponse.class);
        ResponseEntity<TransferAcceptedResponse> segunda = restTemplate.postForEntity("/api/transfers",
                autenticado(request, accountOrigemId), TransferAcceptedResponse.class);

        assertThat(primeira.getBody().transferId()).isNotEqualTo(segunda.getBody().transferId());
    }
}

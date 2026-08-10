package com.bancodigital.notification.web.controller;

import static org.assertj.core.api.Assertions.assertThat;

import com.bancodigital.notification.AbstractIntegrationTest;
import com.bancodigital.notification.domain.Notification;
import com.bancodigital.notification.infrastructure.persistence.NotificationRepository;
import com.bancodigital.notification.web.dto.NotificationResponse;
import com.bancodigital.notification.web.dto.PageResponse;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

class NotificationControllerIT extends AbstractIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private NotificationRepository notificationRepository;

    @Test
    void deveRetornarNotificacoesPaginadasDaPropriaConta() {
        UUID accountId = UUID.randomUUID();
        notificationRepository.save(Notification.email(accountId, UUID.randomUUID(), "Transferência recebida", "corpo 1"));
        notificationRepository.save(Notification.email(accountId, UUID.randomUUID(), "Transferência recebida", "corpo 2"));
        notificationRepository.save(Notification.email(UUID.randomUUID(), UUID.randomUUID(), "Outra conta", "corpo 3"));

        ResponseEntity<PageResponse<NotificationResponse>> response = restTemplate.exchange(
                "/api/notifications/" + accountId, HttpMethod.GET, autenticado(null, accountId),
                new ParameterizedTypeReference<PageResponse<NotificationResponse>>() {
                });

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        PageResponse<NotificationResponse> body = response.getBody();
        assertThat(body).isNotNull();
        assertThat(body.conteudo()).hasSize(2);
        assertThat(body.totalElementos()).isEqualTo(2);
        assertThat(body.conteudo()).allMatch(notification -> notification.title().equals("Transferência recebida"));
    }

    @Test
    void deveRetornar403QuandoAccountIdDaUrlDivergeDoAccountIdDoToken() {
        UUID accountIdDaUrl = UUID.randomUUID();
        UUID accountIdDoToken = UUID.randomUUID();

        ResponseEntity<String> response = restTemplate.exchange("/api/notifications/" + accountIdDaUrl,
                HttpMethod.GET, autenticado(null, accountIdDoToken), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
    }

    @Test
    void deveRetornar401QuandoRequisicaoNaoTemToken() {
        UUID accountId = UUID.randomUUID();

        ResponseEntity<String> response = restTemplate.getForEntity("/api/notifications/" + accountId, String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }
}

package com.bancodigital.notification.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.bancodigital.notification.AbstractIntegrationTest;
import com.bancodigital.notification.domain.Notification;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;

class NotificationRepositoryTest extends AbstractIntegrationTest {

    @Autowired
    private NotificationRepository notificationRepository;

    @Test
    void devePaginarNotificacoesDaContaEmOrdemDecrescentePorData() throws InterruptedException {
        UUID accountId = UUID.randomUUID();
        UUID outraConta = UUID.randomUUID();

        Notification primeira = notificationRepository.save(
                Notification.email(accountId, UUID.randomUUID(), "Transferência recebida", "corpo 1"));
        Thread.sleep(5);
        Notification segunda = notificationRepository.save(
                Notification.email(accountId, UUID.randomUUID(), "Transferência recebida", "corpo 2"));
        notificationRepository.save(Notification.email(outraConta, UUID.randomUUID(), "Outra conta", "corpo 3"));

        Page<Notification> pagina = notificationRepository.findByAccountIdOrderByCreatedAtDesc(accountId,
                PageRequest.of(0, 10));

        assertThat(pagina.getTotalElements()).isEqualTo(2);
        assertThat(pagina.getContent()).extracting(Notification::getId)
                .containsExactly(segunda.getId(), primeira.getId());
    }
}

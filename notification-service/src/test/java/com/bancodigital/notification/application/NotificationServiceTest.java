package com.bancodigital.notification.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

import com.bancodigital.notification.domain.Notification;
import com.bancodigital.notification.domain.NotificationChannel;
import com.bancodigital.notification.domain.event.TransferCompleted;
import com.bancodigital.notification.domain.event.TransferFailed;
import com.bancodigital.notification.infrastructure.persistence.NotificationRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class NotificationServiceTest {

    @Mock
    private NotificationRepository notificationRepository;

    private NotificationService notificationService;

    @BeforeEach
    void setUp() {
        notificationService = new NotificationService(notificationRepository);
    }

    @Test
    void deveRegistrarNotificacaoParaContaDestinoAoProcessarTransferConcluida() {
        UUID transferId = UUID.randomUUID();
        UUID accountDestinoId = UUID.randomUUID();
        TransferCompleted event = new TransferCompleted(transferId, UUID.randomUUID(), accountDestinoId,
                new BigDecimal("100.00"), Instant.now());

        notificationService.registrarTransferConcluida(event);

        ArgumentCaptor<Notification> captor = ArgumentCaptor.forClass(Notification.class);
        verify(notificationRepository).save(captor.capture());
        Notification notification = captor.getValue();
        assertThat(notification.getAccountId()).isEqualTo(accountDestinoId);
        assertThat(notification.getTransferId()).isEqualTo(transferId);
        assertThat(notification.getChannel()).isEqualTo(NotificationChannel.EMAIL);
        assertThat(notification.getBody()).contains("100.00");
    }

    @Test
    void deveRegistrarNotificacaoParaContaOrigemComMotivoAoProcessarTransferFalhou() {
        UUID transferId = UUID.randomUUID();
        UUID accountOrigemId = UUID.randomUUID();
        TransferFailed event = new TransferFailed(transferId, accountOrigemId, UUID.randomUUID(),
                new BigDecimal("50.00"), "LIMITE_EXCEDIDO", Instant.now());

        notificationService.registrarTransferFalhou(event);

        ArgumentCaptor<Notification> captor = ArgumentCaptor.forClass(Notification.class);
        verify(notificationRepository).save(captor.capture());
        Notification notification = captor.getValue();
        assertThat(notification.getAccountId()).isEqualTo(accountOrigemId);
        assertThat(notification.getTransferId()).isEqualTo(transferId);
        assertThat(notification.getBody()).contains("LIMITE_EXCEDIDO");
    }
}

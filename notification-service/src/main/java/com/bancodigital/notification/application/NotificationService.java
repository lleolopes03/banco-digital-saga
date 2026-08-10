package com.bancodigital.notification.application;

import com.bancodigital.notification.domain.Notification;
import com.bancodigital.notification.domain.event.TransferCompleted;
import com.bancodigital.notification.domain.event.TransferFailed;
import com.bancodigital.notification.infrastructure.persistence.NotificationRepository;
import org.springframework.stereotype.Service;

/*
 * Uma notificação por evento de saga (não uma por canal) — o objetivo desta fatia é fechar o
 * ciclo "saga terminou -> algo é registrado", não simular múltiplos canais de envio para o mesmo
 * evento. EMAIL é o canal usado como representante; PUSH/SMS (Notification.push/sms) já existem
 * no domínio para quando um canal de envio real justificar o uso deles.
 */
@Service
public class NotificationService {

    private final NotificationRepository notificationRepository;

    public NotificationService(NotificationRepository notificationRepository) {
        this.notificationRepository = notificationRepository;
    }

    public void registrarTransferConcluida(TransferCompleted event) {
        Notification notification = Notification.email(event.accountDestinoId(), event.transferId(),
                "Transferência recebida",
                "Você recebeu uma transferência de R$ " + event.valor());
        notificationRepository.save(notification);
    }

    public void registrarTransferFalhou(TransferFailed event) {
        Notification notification = Notification.email(event.accountOrigemId(), event.transferId(),
                "Transferência não concluída",
                "Sua transferência de R$ " + event.valor() + " não foi concluída. Motivo: " + event.motivo());
        notificationRepository.save(notification);
    }
}

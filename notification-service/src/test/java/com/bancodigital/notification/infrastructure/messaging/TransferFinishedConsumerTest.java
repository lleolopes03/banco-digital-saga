package com.bancodigital.notification.infrastructure.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import com.bancodigital.notification.AbstractIntegrationTest;
import com.bancodigital.notification.domain.Notification;
import com.bancodigital.notification.domain.NotificationChannel;
import com.bancodigital.notification.domain.NotificationStatus;
import com.bancodigital.notification.domain.event.TransferCompleted;
import com.bancodigital.notification.domain.event.TransferFailed;
import com.bancodigital.notification.infrastructure.persistence.NotificationRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

/*
 * Kafka real (Testcontainers) + o TransferFinishedConsumer de verdade já registrado no contexto
 * (mesmo grupo "notification-service" usado em produção) — prova que uma saga concluída/falhada
 * vira, de fato, um documento Notification no MongoDB, e que o mesmo evento reentregue duas
 * vezes (garantia at-least-once do Kafka) não duplica a notificação.
 */
class TransferFinishedConsumerTest extends AbstractIntegrationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private NotificationRepository notificationRepository;

    @Test
    void deveCriarNotificacaoParaContaDestinoAoProcessarTransferCompleted() throws Exception {
        UUID transferId = UUID.randomUUID();
        UUID accountDestinoId = UUID.randomUUID();
        TransferCompleted event = new TransferCompleted(transferId, UUID.randomUUID(), accountDestinoId,
                new BigDecimal("100.00"), Instant.now());

        publicar("TransferCompleted", event, UUID.randomUUID());

        Notification notification = aguardarNotificacao(transferId);
        assertThat(notification.getAccountId()).isEqualTo(accountDestinoId);
        assertThat(notification.getChannel()).isEqualTo(NotificationChannel.EMAIL);
        assertThat(notification.getStatus()).isEqualTo(NotificationStatus.SENT);
    }

    @Test
    void deveCriarNotificacaoParaContaOrigemComMotivoAoProcessarTransferFailed() throws Exception {
        UUID transferId = UUID.randomUUID();
        UUID accountOrigemId = UUID.randomUUID();
        TransferFailed event = new TransferFailed(transferId, accountOrigemId, UUID.randomUUID(),
                new BigDecimal("50.00"), "LIMITE_EXCEDIDO", Instant.now());

        publicar("TransferFailed", event, UUID.randomUUID());

        Notification notification = aguardarNotificacao(transferId);
        assertThat(notification.getAccountId()).isEqualTo(accountOrigemId);
        assertThat(notification.getBody()).contains("LIMITE_EXCEDIDO");
    }

    @Test
    void deveCriarApenasUmaNotificacaoMesmoComEventoReentregueDuasVezes() throws Exception {
        UUID transferId = UUID.randomUUID();
        TransferCompleted event = new TransferCompleted(transferId, UUID.randomUUID(), UUID.randomUUID(),
                new BigDecimal("75.00"), Instant.now());
        UUID eventId = UUID.randomUUID();

        publicar("TransferCompleted", event, eventId);
        publicar("TransferCompleted", event, eventId);

        aguardarNotificacao(transferId);
        // Não há um segundo tópico de saída pra confirmar "processamento concluído" aqui (só
        // persistência) — uma pequena espera extra garante que uma segunda notificação indevida,
        // se existisse, já teria sido persistida antes da asserção abaixo.
        sleep();
        List<Notification> notificacoes = notificationRepository.findByTransferId(transferId);
        assertThat(notificacoes).hasSize(1);
    }

    private void publicar(String eventType, Object payload, UUID eventId) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>("transfer.finished", null,
                MAPPER.writeValueAsString(payload));
        record.headers().add(new RecordHeader("eventType", eventType.getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("eventId", eventId.toString().getBytes(StandardCharsets.UTF_8)));
        kafkaTemplate.send(record).get();
    }

    private Notification aguardarNotificacao(UUID transferId) {
        Instant prazo = Instant.now().plus(Duration.ofSeconds(20));
        while (Instant.now().isBefore(prazo)) {
            List<Notification> encontradas = notificationRepository.findByTransferId(transferId);
            if (!encontradas.isEmpty()) {
                return encontradas.get(0);
            }
            sleep();
        }
        throw new AssertionError("Notification para transferId " + transferId + " não apareceu a tempo");
    }

    private void sleep() {
        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}

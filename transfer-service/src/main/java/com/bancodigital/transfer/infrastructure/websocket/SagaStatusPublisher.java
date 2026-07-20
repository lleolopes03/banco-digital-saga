package com.bancodigital.transfer.infrastructure.websocket;

import com.bancodigital.transfer.domain.SagaInstance;
import com.bancodigital.transfer.web.dto.SagaStatusUpdate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

/*
 * Broadcast best-effort do progresso da saga para quem estiver assinando
 * /topic/transfers/{transferId} no momento. Diferente do OutboxEvent (que garante entrega mesmo
 * se o Kafka estiver fora do ar no instante da publicação), aqui não há persistência nem retry:
 * se ninguém estiver conectado, a mensagem se perde — e não tem problema, porque o
 * TransferStatusComponent tem fallback de polling em GET /api/transfers/{id} para cobrir
 * exatamente esse caso.
 */
@Component
public class SagaStatusPublisher {

    private static final String TOPIC_PREFIX = "/topic/transfers/";

    private final SimpMessagingTemplate messagingTemplate;

    public SagaStatusPublisher(SimpMessagingTemplate messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
    }

    public void publish(SagaInstance saga) {
        messagingTemplate.convertAndSend(TOPIC_PREFIX + saga.getId(), SagaStatusUpdate.from(saga));
    }
}

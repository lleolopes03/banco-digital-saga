package com.bancodigital.account.infrastructure.messaging;

import com.bancodigital.account.domain.OutboxEvent;
import com.bancodigital.account.domain.OutboxEventStatus;
import com.bancodigital.account.infrastructure.observability.SagaObservability;
import com.bancodigital.account.infrastructure.persistence.OutboxEventRepository;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class OutboxPublisherScheduler {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisherScheduler.class);
    private static final int BATCH_SIZE = 50;
    private static final String EVENT_TYPE_HEADER = "eventType";
    private static final String EVENT_ID_HEADER = "eventId";

    // account-service publica em mais de um tópico dependendo do tipo de evento: o próprio
    // depósito (account.events) e agora também os eventos da saga de transferência. O eventType
    // também vai como header Kafka porque alguns desses tópicos (transfer.reserved,
    // transfer.credited, transfer.compensated) carregam mais de um tipo de evento e o
    // consumidor precisa distinguir sem depender do formato do payload.
    private static final Map<String, String> TOPICO_POR_EVENTO = Map.of(
            "DepositCompleted", "account.events",
            "TransferReserved", "transfer.reserved",
            "TransferReservationFailed", "transfer.reserved",
            "DebitConfirmed", "transfer.debited",
            "CreditConfirmed", "transfer.credited",
            "CreditFailed", "transfer.credited",
            "ReservationReleased", "transfer.compensated",
            "DebitReversed", "transfer.compensated"
    );

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final SagaObservability sagaObservability;

    public OutboxPublisherScheduler(OutboxEventRepository outboxEventRepository, KafkaTemplate<String, String> kafkaTemplate,
                                     SagaObservability sagaObservability) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.sagaObservability = sagaObservability;
    }

    @Scheduled(fixedDelay = 500)
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> pendingEvents = outboxEventRepository.findByStatusOrderByCreatedAtAsc(
                OutboxEventStatus.PENDING, PageRequest.of(0, BATCH_SIZE));

        for (OutboxEvent event : pendingEvents) {
            try {
                String topico = resolverTopico(event.getEventType());
                ProducerRecord<String, String> record = new ProducerRecord<>(topico, null,
                        event.getAggregateId().toString(), event.getPayload());
                record.headers().add(new RecordHeader(EVENT_TYPE_HEADER,
                        event.getEventType().getBytes(StandardCharsets.UTF_8)));
                // eventId é o id do próprio OutboxEvent (já um UUID único gerado na criação do
                // registro) — propagado como header para que os consumers possam usá-lo como
                // chave de idempotência (IdempotentEventHandler).
                record.headers().add(new RecordHeader(EVENT_ID_HEADER,
                        event.getId().toString().getBytes(StandardCharsets.UTF_8)));
                // traceparent (W3C) já foi capturado no momento em que este OutboxEvent foi
                // criado (ver SagaObservability.traceparentAtual) — aqui só copiamos o valor
                // armazenado para o header, sem tentar injetar Context.current() nesta thread de
                // scheduling, que não tem nenhuma relação com o trace original da saga.
                sagaObservability.injetarHeader(record, event.getTraceContext());

                kafkaTemplate.send(record).get();
                event.markPublished();
            } catch (Exception ex) {
                log.error("Falha ao publicar outbox event {} (tipo {}) no Kafka, mantendo PENDING para nova tentativa",
                        event.getId(), event.getEventType(), ex);
            }
        }
    }

    private String resolverTopico(String eventType) {
        String topico = TOPICO_POR_EVENTO.get(eventType);
        if (topico == null) {
            throw new IllegalStateException("Nenhum tópico configurado para o eventType: " + eventType);
        }
        return topico;
    }
}

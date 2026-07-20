package com.bancodigital.transfer.infrastructure.messaging;

import com.bancodigital.transfer.domain.OutboxEvent;
import com.bancodigital.transfer.domain.OutboxEventStatus;
import com.bancodigital.transfer.infrastructure.observability.SagaObservability;
import com.bancodigital.transfer.infrastructure.persistence.OutboxEventRepository;
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

    // transfer-service publica um comando diferente em cada etapa da saga, cada um no seu
    // próprio tópico (transfer.requested, transfer.debit, transfer.credit). Os comandos de
    // compensação (ReleaseReservedAmount, ReverseDebitRequested) dividem o tópico
    // transfer.compensate, distinguidos pelo header eventType.
    private static final Map<String, String> TOPICO_POR_EVENTO = Map.of(
            "TransferRequested", "transfer.requested",
            "DebitConfirmationRequested", "transfer.debit",
            "CreditRequested", "transfer.credit",
            "ReleaseReservedAmount", "transfer.compensate",
            "ReverseDebitRequested", "transfer.compensate"
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
                // chave de idempotência (IdempotentEventHandler), sem depender de nenhum campo
                // de negócio do payload que possa se repetir entre eventos diferentes.
                record.headers().add(new RecordHeader(EVENT_ID_HEADER,
                        event.getId().toString().getBytes(StandardCharsets.UTF_8)));
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

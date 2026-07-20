package com.bancodigital.antifraud.infrastructure.messaging;

import com.bancodigital.antifraud.domain.OutboxEvent;
import com.bancodigital.antifraud.domain.OutboxEventStatus;
import com.bancodigital.antifraud.infrastructure.observability.SagaObservability;
import com.bancodigital.antifraud.infrastructure.persistence.OutboxEventRepository;
import java.nio.charset.StandardCharsets;
import java.util.List;
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
    private static final String TOPIC = "antifraud.events";
    private static final String EVENT_TYPE_HEADER = "eventType";
    private static final String EVENT_ID_HEADER = "eventId";

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
                // AntiFraudApproved e AntiFraudRejected dividem o mesmo tópico; o header
                // eventType é o que permite o transfer-service diferenciá-los ao consumir.
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, null,
                        event.getAggregateId().toString(), event.getPayload());
                record.headers().add(new RecordHeader(EVENT_TYPE_HEADER,
                        event.getEventType().getBytes(StandardCharsets.UTF_8)));
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
}

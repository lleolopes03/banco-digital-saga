package com.bancodigital.transfer.infrastructure.messaging;

import com.bancodigital.transfer.domain.OutboxEvent;
import com.bancodigital.transfer.infrastructure.observability.SagaObservability;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.stereotype.Component;

/**
 * Transforma um {@link OutboxEvent} no registro Kafka a publicar: escolhe o tópico pelo tipo do
 * evento e anexa os headers (eventType, eventId e o traceparent já capturado). Adicionar um novo
 * tipo de evento é só acrescentar uma entrada em {@code TOPICO_POR_EVENTO}.
 */
@Component
public class OutboxRecordFactory {

    private static final String EVENT_TYPE_HEADER = "eventType";
    private static final String EVENT_ID_HEADER = "eventId";

    // transfer-service publica um comando diferente em cada etapa da saga, cada um no seu próprio
    // tópico. Os comandos de compensação (ReleaseReservedAmount, ReverseDebitRequested) dividem
    // transfer.compensate, e os eventos terminais (TransferCompleted, TransferFailed) dividem
    // transfer.finished — distinguidos pelo header eventType.
    private static final Map<String, String> TOPICO_POR_EVENTO = Map.of(
            "TransferRequested", "transfer.requested",
            "DebitConfirmationRequested", "transfer.debit",
            "CreditRequested", "transfer.credit",
            "ReleaseReservedAmount", "transfer.compensate",
            "ReverseDebitRequested", "transfer.compensate",
            "TransferCompleted", "transfer.finished",
            "TransferFailed", "transfer.finished"
    );

    private final SagaObservability sagaObservability;

    public OutboxRecordFactory(SagaObservability sagaObservability) {
        this.sagaObservability = sagaObservability;
    }

    public ProducerRecord<String, String> criar(OutboxEvent event) {
        ProducerRecord<String, String> record = new ProducerRecord<>(resolverTopico(event.getEventType()), null,
                event.getAggregateId().toString(), event.getPayload());
        record.headers().add(new RecordHeader(EVENT_TYPE_HEADER, bytes(event.getEventType())));
        // eventId é o id do próprio OutboxEvent (um UUID único gerado na criação do registro),
        // propagado como header para que os consumers o usem como chave de idempotência.
        record.headers().add(new RecordHeader(EVENT_ID_HEADER, bytes(event.getId().toString())));
        // traceparent (W3C) já foi capturado quando o OutboxEvent foi criado (ver
        // SagaObservability.traceparentAtual) — aqui só copiamos o valor armazenado para o header,
        // sem tentar injetar Context.current() nesta thread de scheduling, que não tem nenhuma
        // relação com o trace original da saga.
        sagaObservability.injetarHeader(record, event.getTraceContext());
        return record;
    }

    private static String resolverTopico(String eventType) {
        String topico = TOPICO_POR_EVENTO.get(eventType);
        if (topico == null) {
            throw new IllegalStateException("Nenhum tópico configurado para o eventType: " + eventType);
        }
        return topico;
    }

    private static byte[] bytes(String valor) {
        return valor.getBytes(StandardCharsets.UTF_8);
    }
}

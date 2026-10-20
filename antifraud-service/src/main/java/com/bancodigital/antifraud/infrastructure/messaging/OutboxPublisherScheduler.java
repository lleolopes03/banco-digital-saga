package com.bancodigital.antifraud.infrastructure.messaging;

import com.bancodigital.antifraud.domain.OutboxEvent;
import com.bancodigital.antifraud.infrastructure.observability.SagaObservability;
import com.bancodigital.antifraud.infrastructure.persistence.OutboxEventRepository;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

@Component
public class OutboxPublisherScheduler {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisherScheduler.class);
    private static final int MAX_LOTES_POR_EXECUCAO = 20;
    private static final String EVENT_TYPE_HEADER = "eventType";
    private static final String EVENT_ID_HEADER = "eventId";

    // AntiFraudApproved e AntiFraudRejected dividem o mesmo tópico; o header eventType é o que
    // permite o transfer-service diferenciá-los ao consumir. Tipo desconhecido falha (fica PENDING
    // e é registrado), em vez de ser publicado às cegas num tópico de negócio.
    private static final Map<String, String> TOPICO_POR_EVENTO = Map.of(
            "AntiFraudApproved", "antifraud.events",
            "AntiFraudRejected", "antifraud.events"
    );
    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final SagaObservability sagaObservability;
    private final TransactionTemplate transactionTemplate;
    private final int tamanhoLote;

    public OutboxPublisherScheduler(OutboxEventRepository outboxEventRepository, KafkaTemplate<String, String> kafkaTemplate,
                                     SagaObservability sagaObservability, TransactionTemplate transactionTemplate,
                                     @Value("${outbox.batch-size:100}") int tamanhoLote) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.sagaObservability = sagaObservability;
        this.transactionTemplate = transactionTemplate;
        this.tamanhoLote = tamanhoLote;
    }

    /*
     * Cada poll drena a fila: enquanto o lote vier cheio e algo tiver sido publicado, lê o próximo
     * lote na hora, sem esperar o intervalo. Cada lote é uma transação curta (em vez de uma única
     * transação para todo o poll). O intervalo é só o piso de latência quando a fila está vazia.
     */
    @Scheduled(fixedDelayString = "${outbox.poll-interval:PT0.1S}")
    public void publishPendingEvents() {
        for (int i = 0; i < MAX_LOTES_POR_EXECUCAO; i++) {
            ResultadoLote r = transactionTemplate.execute((status) -> publicarLote());
            if (r == null || r.publicados() == 0 || r.lidos() < tamanhoLote) {
                return;
            }
        }
    }

    /*
     * Envia o lote em pipeline: dispara todos os send() e só depois aguarda as confirmações, em
     * vez de um .get() por evento (que serializava um round-trip ao broker por evento).
     *
     * A seleção (reivindicarPendentes) trava as linhas com FOR UPDATE SKIP LOCKED e devolve no
     * máximo um evento por agregado, o mais antigo ainda PENDING. Assim várias instâncias podem
     * rodar este poller ao mesmo tempo sem publicar o mesmo evento nem inverter a ordem de uma
     * saga; e um evento que falha fica PENDING e bloqueia os seguintes do mesmo agregado até ser
     * reenviado. Efeito colateral: dois eventos do mesmo agregado nunca saem no mesmo lote (o
     * segundo sai no lote seguinte, que o dreno em loop já busca na hora).
     */
    private ResultadoLote publicarLote() {
        List<OutboxEvent> pendentes = outboxEventRepository.reivindicarPendentes(tamanhoLote);
        List<Envio> envios = new ArrayList<>(pendentes.size());
        for (OutboxEvent event : pendentes) {
            try {
                envios.add(new Envio(event, kafkaTemplate.send(montarRecord(event))));
            } catch (Exception ex) {
                registrarFalha(event, ex);
            }
        }
        int publicados = 0;
        for (Envio envio : envios) {
            try {
                envio.confirmacao().get();
                envio.evento().markPublished();
                publicados++;
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception ex) {
                registrarFalha(envio.evento(), ex);
            }
        }
        return new ResultadoLote(pendentes.size(), publicados);
    }

    private ProducerRecord<String, String> montarRecord(OutboxEvent event) {
        ProducerRecord<String, String> record = new ProducerRecord<>(resolverTopico(event.getEventType()), null,
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
        return record;
    }

    private void registrarFalha(OutboxEvent event, Exception ex) {
        log.error("Falha ao publicar outbox event {} (tipo {}) no Kafka, mantendo PENDING para nova tentativa",
                event.getId(), event.getEventType(), ex);
    }

    private String resolverTopico(String eventType) {
        String topico = TOPICO_POR_EVENTO.get(eventType);
        if (topico == null) {
            throw new IllegalStateException("Nenhum tópico configurado para o eventType: " + eventType);
        }
        return topico;
    }
    private record Envio(OutboxEvent evento, CompletableFuture<?> confirmacao) {
    }

    private record ResultadoLote(int lidos, int publicados) {
    }
}

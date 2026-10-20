package com.bancodigital.account.infrastructure.messaging;

import com.bancodigital.account.domain.OutboxEvent;
import com.bancodigital.account.infrastructure.persistence.OutboxEventRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Publica no Kafka os eventos pendentes do Outbox. Só orquestra o ciclo (reivindicar lote, enviar,
 * confirmar); a montagem do registro Kafka e o roteamento de tópico ficam em {@link OutboxRecordFactory}.
 */
@Component
public class OutboxPublisherScheduler {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisherScheduler.class);
    private static final int MAX_LOTES_POR_EXECUCAO = 20;

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OutboxRecordFactory recordFactory;
    private final TransactionTemplate transactionTemplate;
    private final int tamanhoLote;

    public OutboxPublisherScheduler(OutboxEventRepository outboxEventRepository,
                                     KafkaTemplate<String, String> kafkaTemplate, OutboxRecordFactory recordFactory,
                                     TransactionTemplate transactionTemplate,
                                     @Value("${outbox.batch-size:100}") int tamanhoLote) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.recordFactory = recordFactory;
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
            ResultadoLote resultado = transactionTemplate.execute((status) -> publicarLote());
            if (resultado == null || resultado.publicados() == 0 || resultado.lidos() < tamanhoLote) {
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
        List<Envio> envios = enviarTodos(pendentes);
        int publicados = confirmarEnvios(envios);
        return new ResultadoLote(pendentes.size(), publicados);
    }

    private List<Envio> enviarTodos(List<OutboxEvent> eventos) {
        List<Envio> envios = new ArrayList<>(eventos.size());
        for (OutboxEvent evento : eventos) {
            try {
                envios.add(new Envio(evento, kafkaTemplate.send(recordFactory.criar(evento))));
            } catch (Exception ex) {
                registrarFalha(evento, ex);
            }
        }
        return envios;
    }

    private int confirmarEnvios(List<Envio> envios) {
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
        return publicados;
    }

    private void registrarFalha(OutboxEvent event, Exception ex) {
        log.error("Falha ao publicar outbox event {} (tipo {}) no Kafka, mantendo PENDING para nova tentativa",
                event.getId(), event.getEventType(), ex);
    }

    private record Envio(OutboxEvent evento, CompletableFuture<?> confirmacao) {
    }

    private record ResultadoLote(int lidos, int publicados) {
    }
}

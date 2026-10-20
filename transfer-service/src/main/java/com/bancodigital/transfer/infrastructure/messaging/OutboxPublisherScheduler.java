package com.bancodigital.transfer.infrastructure.messaging;

import com.bancodigital.transfer.domain.OutboxEvent;
import com.bancodigital.transfer.infrastructure.persistence.OutboxEventRepository;
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

    /**
     * Drena a fila: enquanto o lote vier cheio e algo tiver sido publicado, lê o próximo na hora. O
     * intervalo é só o piso de latência com a fila vazia. Cada lote é uma transação curta.
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
     * Envia o lote em pipeline (todos os send() antes de aguardar as confirmações). reivindicarPendentes
     * trava as linhas (SKIP LOCKED) e traz só o evento mais antigo de cada agregado: várias instâncias
     * não duplicam eventos nem invertem a ordem de uma saga, e um envio que falha bloqueia os seguintes
     * da mesma saga até ser reenviado.
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

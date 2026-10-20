package com.bancodigital.transfer.infrastructure.maintenance;

import com.bancodigital.transfer.infrastructure.persistence.OutboxEventRepository;
import com.bancodigital.transfer.infrastructure.persistence.ProcessedEventRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.function.IntSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Apaga em lotes, em transações curtas, o que só cresce: eventos de outbox já publicados
 * ({@code retention.outbox}, padrão 7 dias; PENDING nunca é tocado) e processed_events
 * ({@code retention.processed-events}, padrão 14 dias, acima da retenção de 7 dias do Kafka para a
 * idempotência cobrir qualquer reentrega). Várias instâncias em paralelo são inofensivas: o DELETE é idempotente.
 */
@Component
public class RetentionCleanupScheduler {

    private static final Logger log = LoggerFactory.getLogger(RetentionCleanupScheduler.class);
    private static final int LOTE = 1_000;
    private static final int MAX_LOTES_POR_EXECUCAO = 100;

    private final OutboxEventRepository outboxEventRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final TransactionTemplate transactionTemplate;
    private final Duration retencaoOutbox;
    private final Duration retencaoProcessedEvents;

    public RetentionCleanupScheduler(OutboxEventRepository outboxEventRepository,
                                     ProcessedEventRepository processedEventRepository,
                                     TransactionTemplate transactionTemplate,
                                     @Value("${retention.outbox:P7D}") Duration retencaoOutbox,
                                     @Value("${retention.processed-events:P14D}") Duration retencaoProcessedEvents) {
        this.outboxEventRepository = outboxEventRepository;
        this.processedEventRepository = processedEventRepository;
        this.transactionTemplate = transactionTemplate;
        this.retencaoOutbox = retencaoOutbox;
        this.retencaoProcessedEvents = retencaoProcessedEvents;
    }

    @Scheduled(fixedDelayString = "${retention.cleanup-interval:PT1H}",
            initialDelayString = "${retention.initial-delay:PT5M}")
    public void limpar() {
        Resultado r = executar(Instant.now());
        if (r.outbox() > 0 || r.processedEvents() > 0) {
            log.info("Retenção: {} linhas de outbox_events (PUBLISHED > {}) e {} de processed_events (> {}) apagadas",
                    r.outbox(), retencaoOutbox, r.processedEvents(), retencaoProcessedEvents);
        }
    }

    /** Separado de {@link #limpar()} para poder ser exercitado com um "agora" controlado. */
    public Resultado executar(Instant agora) {
        Instant limiteOutbox = agora.minus(retencaoOutbox);
        Instant limiteProcessed = agora.minus(retencaoProcessedEvents);
        int outbox = apagarEmLotes(() -> outboxEventRepository.apagarPublicadosAntesDe(limiteOutbox, LOTE));
        int processed = apagarEmLotes(() -> processedEventRepository.apagarProcessadosAntesDe(limiteProcessed, LOTE));
        return new Resultado(outbox, processed);
    }

    private int apagarEmLotes(IntSupplier apagarUmLote) {
        int total = 0;
        for (int i = 0; i < MAX_LOTES_POR_EXECUCAO; i++) {
            Integer apagadas = transactionTemplate.execute(status -> apagarUmLote.getAsInt());
            int n = apagadas == null ? 0 : apagadas;
            total += n;
            if (n < LOTE) {
                break; // acabou o que havia para apagar; o resto fica para a próxima execução
            }
        }
        return total;
    }

    public record Resultado(int outbox, int processedEvents) {
    }
}

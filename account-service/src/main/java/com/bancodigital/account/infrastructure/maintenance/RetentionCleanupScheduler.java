package com.bancodigital.account.infrastructure.maintenance;

import com.bancodigital.account.infrastructure.persistence.OutboxEventRepository;
import com.bancodigital.account.infrastructure.persistence.ProcessedEventRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.function.IntSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/*
 * Retenção das tabelas que só crescem: outbox_events e processed_events. Sem isto, cada saga
 * deixa linhas para sempre (em ~1 tx/s sustentado são centenas de milhares de linhas por dia por
 * tabela), o que degrada índices, backup e vacuum.
 *
 * Regras de segurança:
 *  - outbox_events: só apaga as PUBLISHED mais antigas que `retention.outbox`. Eventos PENDING
 *    NUNCA são tocados, por mais antigos que sejam — apagá-los seria perder um evento ainda não
 *    publicado.
 *  - processed_events: é a fonte de verdade da idempotência quando o Redis (TTL de 24h) é
 *    limpo, então a retenção precisa cobrir a janela em que o Kafka ainda pode reentregar uma
 *    mensagem (retenção padrão do tópico: 7 dias). Por isso o padrão aqui é 14 dias, com folga.
 *
 * Apaga em lotes, cada um na sua transação curta, para não segurar lock nem inflar o WAL numa
 * única operação gigante. Várias instâncias rodando ao mesmo tempo são inofensivas (DELETE
 * idempotente). Duplicado por serviço, mesma convenção de OutboxEvent/ProcessedEvent.
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

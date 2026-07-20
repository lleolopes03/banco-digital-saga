package com.bancodigital.account.infrastructure.idempotency;

import com.bancodigital.account.domain.ProcessedEvent;
import com.bancodigital.account.infrastructure.persistence.ProcessedEventRepository;
import java.time.Duration;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/*
 * Wrapper reutilizável de idempotência para consumers Kafka: Kafka garante at-least-once, então
 * qualquer consumer pode receber o mesmo evento mais de uma vez (redelivery após rebalance,
 * timeout de commit, etc). Sem isso, reprocessar um evento significaria débito/crédito duplicado.
 *
 * Redis é a checagem rápida (SETNX com TTL de 24h — bem acima do timeout de saga de 2 minutos,
 * então nenhum evento legítimo chegaria depois disso). Postgres (processed_events) é a fonte de
 * verdade: se o Redis for limpo/reiniciado, a checagem cai para o Postgres antes de reprocessar,
 * então a garantia de idempotência não depende do Redis estar sempre disponível.
 *
 * A chave Redis é namespaced por serviço (spring.application.name), não só por eventId: alguns
 * tópicos têm fan-out para mais de um consumer group (ex.: transfer.reserved é consumido tanto
 * por account-service quanto, futuramente, por outros serviços) — o mesmo eventId pode chegar a
 * mais de um serviço como entregas legitimamente independentes (cada consumer group recebe sua
 * própria cópia). Sem o namespace por serviço, o primeiro serviço a processar "roubaria" a chave
 * global e o outro trataria sua própria primeira entrega, legítima, como se fosse duplicata. O
 * Postgres já não tem esse problema (cada serviço tem seu próprio banco), só o Redis
 * compartilhado precisa dessa distinção.
 *
 * Deliberadamente NÃO envolve processamento.run() nem a gravação final em uma única transação
 * @Transactional deste componente: os métodos de negócio que este wrapper chama (ex.:
 * AccountService.confirmarDebito) já controlam sua própria transação via TransactionTemplate e
 * são @Retryable em cima de OptimisticLockingFailureException — cada tentativa de retry precisa
 * abrir sua própria transação nova. Se este wrapper abrisse uma transação externa, todo o retry
 * aconteceria dentro da MESMA transação já potencialmente marcada rollback-only pela tentativa
 * anterior, quebrando o mecanismo de retry. O preço dessa escolha é uma janela (bem estreita, e
 * coberta pelo Redis na prática) entre o commit do efeito de negócio e o commit da linha em
 * processed_events, em caso de crash exatamente nesse intervalo.
 */
@Component
public class IdempotentEventHandler {

    private static final Logger log = LoggerFactory.getLogger(IdempotentEventHandler.class);
    private static final String REDIS_KEY_PREFIX = "idempotency:";
    private static final Duration TTL = Duration.ofHours(24);

    private final StringRedisTemplate redisTemplate;
    private final ProcessedEventRepository processedEventRepository;
    private final String nomeServico;

    public IdempotentEventHandler(StringRedisTemplate redisTemplate, ProcessedEventRepository processedEventRepository,
                                   @Value("${spring.application.name}") String nomeServico) {
        this.redisTemplate = redisTemplate;
        this.processedEventRepository = processedEventRepository;
        this.nomeServico = nomeServico;
    }

    public void processarSeNovo(UUID eventId, String eventType, Runnable processamento) {
        String chaveRedis = REDIS_KEY_PREFIX + nomeServico + ":" + eventId;
        Boolean reservado = redisTemplate.opsForValue().setIfAbsent(chaveRedis, eventType, TTL);

        if (!Boolean.TRUE.equals(reservado)) {
            log.info("Evento duplicado ignorado (Redis): eventId={}, eventType={}", eventId, eventType);
            return;
        }

        if (processedEventRepository.existsById(eventId)) {
            log.info("Evento duplicado ignorado (Postgres, cache Redis provavelmente foi limpo): eventId={}, eventType={}",
                    eventId, eventType);
            return;
        }

        try {
            processamento.run();
        } catch (RuntimeException ex) {
            redisTemplate.delete(chaveRedis);
            throw ex;
        }

        processedEventRepository.save(ProcessedEvent.agora(eventId));
    }
}

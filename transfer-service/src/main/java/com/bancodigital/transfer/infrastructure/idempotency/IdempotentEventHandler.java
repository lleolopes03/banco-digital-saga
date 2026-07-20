package com.bancodigital.transfer.infrastructure.idempotency;

import com.bancodigital.transfer.domain.ProcessedEvent;
import com.bancodigital.transfer.infrastructure.persistence.ProcessedEventRepository;
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
 * então nenhum evento legítimo chegaria depois disso, então um TTL vencido nunca causa
 * reprocessamento indevido de algo ainda "em voo"). Postgres (processed_events) é a fonte de
 * verdade: se o Redis for limpo/reiniciado, a checagem cai para o Postgres antes de reprocessar,
 * então a garantia de idempotência não depende do Redis estar sempre disponível.
 *
 * A chave Redis é namespaced por serviço (spring.application.name), não só por eventId: alguns
 * tópicos têm fan-out para mais de um consumer group — transfer.reserved, por exemplo, é
 * consumido tanto por transfer-service quanto por antifraud-service, cada um com seu próprio
 * consumer group recebendo sua PRÓPRIA cópia da mesma mensagem (mesmo eventId). Sem o namespace
 * por serviço, o primeiro dos dois a processar "roubaria" a chave global no Redis compartilhado,
 * e o outro trataria sua própria primeira entrega, legítima, como se fosse duplicata — foi
 * exatamente esse bug que travava a saga em ANTIFRAUD_PENDING (antifraud-service nunca chegava a
 * avaliar a transferência). O Postgres já não tem esse problema, pois cada serviço tem seu
 * próprio banco de processed_events.
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
            // Libera a chave para que uma redelivery legítima (o processamento pode ter
            // falhado por um motivo transitório) não seja descartada como "duplicada".
            redisTemplate.delete(chaveRedis);
            throw ex;
        }

        processedEventRepository.save(ProcessedEvent.agora(eventId));
    }
}

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

/**
 * Garante que cada evento Kafka seja processado uma única vez (a entrega é at-least-once).
 * Redis é a checagem rápida (SETNX, TTL 24h); a tabela processed_events é a fonte de verdade quando
 * o Redis é limpo.
 *
 * <ul>
 *   <li>A chave do Redis inclui o nome do serviço: num tópico com fan-out (ex.: transfer.reserved)
 *       o mesmo eventId chega a mais de um serviço, e cada entrega é legítima.</li>
 *   <li>Não há transação externa em volta do processamento: os métodos de negócio abrem a própria
 *       transação e fazem retry, e uma transação externa já marcada rollback-only quebraria o retry.
 *       Custo: uma janela mínima entre o commit do efeito e o da linha em processed_events.</li>
 * </ul>
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

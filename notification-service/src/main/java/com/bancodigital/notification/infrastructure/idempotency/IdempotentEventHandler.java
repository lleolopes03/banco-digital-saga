package com.bancodigital.notification.infrastructure.idempotency;

import com.bancodigital.notification.domain.ProcessedEvent;
import com.bancodigital.notification.infrastructure.persistence.ProcessedEventRepository;
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
 * timeout de commit, etc).
 *
 * Redis é a checagem rápida (SETNX com TTL de 24h). MongoDB (processed_events) é a fonte de
 * verdade: se o Redis for limpo/reiniciado, a checagem cai para o Mongo antes de reprocessar —
 * mesma estratégia de duas camadas já usada em account-service/transfer-service/antifraud-service
 * (lá com Postgres na camada durável; aqui com MongoDB, único banco deste serviço).
 *
 * A chave Redis é namespaced por serviço (spring.application.name), não só por eventId — mesma
 * razão já documentada nos outros serviços: nada aqui consome um tópico com fan-out para mais de
 * um consumer group hoje, mas o namespace evita a mesma armadilha se isso mudar no futuro.
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
            log.info("Evento duplicado ignorado (MongoDB, cache Redis provavelmente foi limpo): eventId={}, eventType={}",
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

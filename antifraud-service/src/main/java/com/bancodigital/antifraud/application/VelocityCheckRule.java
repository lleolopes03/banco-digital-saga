package com.bancodigital.antifraud.application;

import com.bancodigital.antifraud.domain.event.TransferReserved;
import java.time.Duration;
import java.util.Optional;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Component
public class VelocityCheckRule implements AntiFraudRule {

    private static final String KEY_PREFIX = "antifraud:velocity:";
    private static final long LIMITE_TRANSFERENCIAS = 5;
    private static final Duration JANELA = Duration.ofSeconds(60);

    private final StringRedisTemplate redisTemplate;

    public VelocityCheckRule(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public Optional<String> avaliar(TransferReserved event) {
        String key = KEY_PREFIX + event.accountOrigemId();
        Long contagem = redisTemplate.opsForValue().increment(key);

        if (contagem != null && contagem == 1L) {
            redisTemplate.expire(key, JANELA);
        }

        if (contagem != null && contagem > LIMITE_TRANSFERENCIAS) {
            return Optional.of("VELOCITY_EXCEDIDA");
        }
        return Optional.empty();
    }
}

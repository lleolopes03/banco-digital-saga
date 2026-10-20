package com.bancodigital.account.infrastructure.cache;

import com.bancodigital.account.application.BalanceCache;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Cache-aside de saldo em Redis. get() devolve null tanto para chave ausente quanto para falha de
 * conexão do StringRedisTemplate, então a leitura sempre cai de volta no Postgres e nunca quebra.
 */
@Component
class RedisBalanceCache implements BalanceCache {

    private static final String KEY_PREFIX = "balance:";
    // TTL curto de propósito: saldo é dinheiro, então prioriza consistência razoável (no máximo
    // 30s de defasagem) sobre o máximo de performance/hit-rate possível do cache.
    private static final Duration TTL = Duration.ofSeconds(30);

    private final StringRedisTemplate redisTemplate;

    RedisBalanceCache(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public Optional<BigDecimal> buscar(UUID accountId) {
        String valor = redisTemplate.opsForValue().get(chave(accountId));
        return Optional.ofNullable(valor).map(BigDecimal::new);
    }

    @Override
    public void guardar(UUID accountId, BigDecimal saldo) {
        redisTemplate.opsForValue().set(chave(accountId), saldo.toPlainString(), TTL);
    }

    @Override
    public void invalidar(UUID accountId) {
        redisTemplate.delete(chave(accountId));
    }

    private static String chave(UUID accountId) {
        return KEY_PREFIX + accountId;
    }
}

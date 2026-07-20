package com.bancodigital.account.infrastructure.security;

import java.time.Duration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

/*
 * Blacklist de logout: guarda o JTI (não o token inteiro) com TTL igual ao tempo de validade
 * restante do token, para que a entrada suma do Redis sozinha assim que o token expiraria de
 * qualquer forma — sem isso a blacklist cresceria para sempre. O valor gravado é irrelevante
 * (só a existência da chave importa), então usamos uma string vazia.
 */
@Service
public class TokenBlacklistService {

    private static final String BLACKLIST_KEY_PREFIX = "jwt:blacklist:";

    private final StringRedisTemplate redisTemplate;

    public TokenBlacklistService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void blacklist(String jti, Duration ttlRestante) {
        if (ttlRestante.isNegative() || ttlRestante.isZero()) {
            // Token já expirado: não há necessidade de blacklist, a expiração natural já cobre.
            return;
        }
        redisTemplate.opsForValue().set(BLACKLIST_KEY_PREFIX + jti, "", ttlRestante);
    }

    public boolean estaNaBlacklist(String jti) {
        return Boolean.TRUE.equals(redisTemplate.hasKey(BLACKLIST_KEY_PREFIX + jti));
    }
}

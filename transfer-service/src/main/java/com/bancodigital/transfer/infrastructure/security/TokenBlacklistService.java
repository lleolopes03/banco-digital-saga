package com.bancodigital.transfer.infrastructure.security;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

/*
 * Mesma chave/prefixo do account-service (jwt:blacklist:<jti>) — os dois serviços apontam para
 * a mesma instância Redis (ver docker-compose.yml), então um logout feito via account-service
 * também bloqueia o token aqui. transfer-service só consulta, nunca escreve nesta blacklist
 * (quem escreve é o endpoint de logout, que só existe no account-service).
 */
@Service
public class TokenBlacklistService {

    private static final String BLACKLIST_KEY_PREFIX = "jwt:blacklist:";

    private final StringRedisTemplate redisTemplate;

    public TokenBlacklistService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public boolean estaNaBlacklist(String jti) {
        return Boolean.TRUE.equals(redisTemplate.hasKey(BLACKLIST_KEY_PREFIX + jti));
    }
}

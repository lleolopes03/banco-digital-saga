package com.bancodigital.transfer.infrastructure.security;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;
import javax.crypto.SecretKey;

/*
 * transfer-service não emite tokens em produção (quem faz login é o account-service) — mas os
 * testes de autorização por conta precisam de tokens válidos para contas arbitrárias, sem
 * depender de um account-service real no ar. Assina com o mesmo segredo default de
 * application.yml (jwt.secret), então tokens gerados aqui são validados normalmente pelo
 * JwtAuthenticationFilter real.
 */
public final class TestJwtFactory {

    private static final String SECRET = "banco-digital-saga-jwt-secret-desenvolvimento-nao-usar-em-producao";
    private static final SecretKey KEY = Keys.hmacShaKeyFor(SECRET.getBytes(StandardCharsets.UTF_8));

    private TestJwtFactory() {
    }

    public static String tokenPara(UUID accountId) {
        return tokenPara(UUID.randomUUID(), accountId, Duration.ofMinutes(15));
    }

    public static String tokenExpiradoPara(UUID accountId) {
        return tokenPara(UUID.randomUUID(), accountId, Duration.ofMinutes(-1));
    }

    public static String tokenPara(UUID userId, UUID accountId, Duration validade) {
        Instant agora = Instant.now();
        return Jwts.builder()
                .subject(userId.toString())
                .claim("accountId", accountId.toString())
                .id(UUID.randomUUID().toString())
                .issuedAt(Date.from(agora))
                .expiration(Date.from(agora.plus(validade)))
                .signWith(KEY)
                .compact();
    }
}

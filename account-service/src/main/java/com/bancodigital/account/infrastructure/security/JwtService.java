package com.bancodigital.account.infrastructure.security;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;
import javax.crypto.SecretKey;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/*
 * Gera e valida os JWTs de curta duração (token não carrega refresh — ver README, "próximo
 * passo"). O claim "accountId" é o que permite às controllers autorizar por conta sem consultar
 * o banco a cada requisição: o token já carrega a conta do usuário autenticado.
 */
@Service
public class JwtService {

    private static final String CLAIM_ACCOUNT_ID = "accountId";

    private final SecretKey key;
    private final Duration expiration;

    public JwtService(@Value("${jwt.secret}") String secret,
                       @Value("${jwt.expiration-minutes:15}") long expirationMinutes) {
        this.key = Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
        this.expiration = Duration.ofMinutes(expirationMinutes);
    }

    public String gerarToken(UUID userId, UUID accountId) {
        Instant agora = Instant.now();
        Instant expiraEm = agora.plus(expiration);
        return Jwts.builder()
                .subject(userId.toString())
                .claim(CLAIM_ACCOUNT_ID, accountId.toString())
                .id(UUID.randomUUID().toString())
                .issuedAt(Date.from(agora))
                .expiration(Date.from(expiraEm))
                .signWith(key)
                .compact();
    }

    public Claims validarEExtrairClaims(String token) {
        return Jwts.parser().verifyWith(key).build().parseSignedClaims(token).getPayload();
    }

    public UUID extrairAccountId(Claims claims) {
        return UUID.fromString(claims.get(CLAIM_ACCOUNT_ID, String.class));
    }

    public Duration expiration() {
        return expiration;
    }
}

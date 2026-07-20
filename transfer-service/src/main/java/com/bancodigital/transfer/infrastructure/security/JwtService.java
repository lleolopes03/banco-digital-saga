package com.bancodigital.transfer.infrastructure.security;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import javax.crypto.SecretKey;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/*
 * transfer-service só valida tokens (quem gera é o account-service, dono do login) — por isso
 * não há gerarToken() aqui. A chave secreta é a mesma (compartilhada via env var JWT_SECRET),
 * então um token emitido pelo account-service é validado normalmente aqui.
 */
@Service
public class JwtService {

    private static final String CLAIM_ACCOUNT_ID = "accountId";

    private final SecretKey key;

    public JwtService(@Value("${jwt.secret}") String secret) {
        this.key = Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
    }

    public Claims validarEExtrairClaims(String token) {
        return Jwts.parser().verifyWith(key).build().parseSignedClaims(token).getPayload();
    }

    public UUID extrairAccountId(Claims claims) {
        return UUID.fromString(claims.get(CLAIM_ACCOUNT_ID, String.class));
    }
}

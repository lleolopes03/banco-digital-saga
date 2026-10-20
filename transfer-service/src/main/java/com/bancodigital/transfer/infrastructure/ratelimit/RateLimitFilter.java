package com.bancodigital.transfer.infrastructure.ratelimit;

import com.bancodigital.transfer.web.dto.CreateTransferRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Rate limit de infraestrutura contra volume bruto de chamadas, complementar ao velocity check de negócio
 * do antifraud-service. Janela fixa por chave (INCR + EXPIRE no Redis): simples e O(1). INCR e EXPIRE não
 * são atômicos, então um crash logo após o primeiro INCR deixaria a chave sem TTL: risco aceito para não
 * depender de Redisson.
 */
@Component
public class RateLimitFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(RateLimitFilter.class);
    private static final String RATE_LIMIT_KEY_PREFIX = "ratelimit:transfer:";
    private static final String TRANSFERS_PATH = "/api/transfers";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final StringRedisTemplate redisTemplate;
    private final int maxRequisicoes;
    private final Duration janela;

    public RateLimitFilter(StringRedisTemplate redisTemplate,
                            @Value("${rate-limit.transfer.max-requests:10}") int maxRequisicoes,
                            @Value("${rate-limit.transfer.window:60s}") Duration janela) {
        this.redisTemplate = redisTemplate;
        this.maxRequisicoes = maxRequisicoes;
        this.janela = janela;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !("POST".equalsIgnoreCase(request.getMethod()) && TRANSFERS_PATH.equals(request.getRequestURI()));
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        CachedBodyHttpServletRequest cachedRequest = new CachedBodyHttpServletRequest(request);

        UUID accountOrigemId = extrairAccountOrigemId(cachedRequest);
        if (accountOrigemId == null) {
            // Corpo ausente/malformado ou sem accountOrigemId: não há o que limitar por conta
            // aqui, a validação normal do controller (@Valid) trata isso como 400 adiante.
            filterChain.doFilter(cachedRequest, response);
            return;
        }

        if (excedeuLimite(accountOrigemId, response)) {
            return;
        }

        filterChain.doFilter(cachedRequest, response);
    }

    private boolean excedeuLimite(UUID accountOrigemId, HttpServletResponse response) throws IOException {
        String chave = RATE_LIMIT_KEY_PREFIX + accountOrigemId;
        Long contagem = redisTemplate.opsForValue().increment(chave);
        if (contagem != null && contagem == 1L) {
            redisTemplate.expire(chave, janela);
        }

        if (contagem == null || contagem <= maxRequisicoes) {
            return false;
        }

        long retryAfterSegundos = ttlRestanteEmSegundos(chave);
        log.warn("Rate limit excedido para conta {}: {} requisições na janela de {}s (limite: {})",
                accountOrigemId, contagem, janela.toSeconds(), maxRequisicoes);

        response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());
        response.setHeader(HttpHeaders.RETRY_AFTER, String.valueOf(retryAfterSegundos));
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.getWriter().write(OBJECT_MAPPER.writeValueAsString(corpoErro(retryAfterSegundos)));
        return true;
    }

    private long ttlRestanteEmSegundos(String chave) {
        Long ttl = redisTemplate.getExpire(chave, TimeUnit.SECONDS);
        return (ttl != null && ttl > 0) ? ttl : janela.toSeconds();
    }

    private Map<String, Object> corpoErro(long retryAfterSegundos) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("timestamp", Instant.now().toString());
        body.put("status", HttpStatus.TOO_MANY_REQUESTS.value());
        body.put("error", HttpStatus.TOO_MANY_REQUESTS.getReasonPhrase());
        body.put("message", "Limite de " + maxRequisicoes + " transferências por conta a cada "
                + janela.toSeconds() + "s excedido. Tente novamente em " + retryAfterSegundos + "s.");
        return body;
    }

    private UUID extrairAccountOrigemId(CachedBodyHttpServletRequest request) {
        try {
            CreateTransferRequest body = OBJECT_MAPPER.readValue(request.getInputStream(), CreateTransferRequest.class);
            return body != null ? body.accountOrigemId() : null;
        } catch (IOException ex) {
            return null;
        }
    }
}

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

/*
 * Camada de rate limiting de infraestrutura, complementar (não substituta) ao velocity check de
 * negócio que já existe no antifraud-service: este filtro protege a API contra volume bruto de
 * chamadas (ex.: um cliente com bug em loop, ou um ataque simples de força bruta), sem nenhuma
 * noção de regra de fraude — quem decide se um PADRÃO de transferências é suspeito continua
 * sendo o antifraud-service.
 *
 * Contador de janela fixa via INCR + EXPIRE no Redis (não um token bucket "de verdade" com
 * refill suave): simples, O(1), e suficiente para o objetivo aqui. Como INCR e EXPIRE não são
 * atômicos entre si, existe uma janela estreita (só no primeiro request de uma chave nova) onde
 * um crash do processo deixaria a chave sem TTL — risco aceito em troca de não introduzir
 * Redisson só para isso.
 */
@Component
public class RateLimitFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(RateLimitFilter.class);
    private static final String RATE_LIMIT_KEY_PREFIX = "ratelimit:transfer:";
    private static final String TRANSFERS_PATH = "/api/transfers";
    // Instância própria em vez de injetar um ObjectMapper do Spring, seguindo o mesmo padrão já
    // usado em SagaOrchestratorService/AccountService para serialização pontual de payloads.
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

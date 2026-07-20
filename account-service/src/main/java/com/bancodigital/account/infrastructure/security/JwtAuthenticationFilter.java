package com.bancodigital.account.infrastructure.security;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/*
 * Só autentica quando o header Authorization está presente: se ausente, deixa a requisição
 * seguir sem autenticação e é o SecurityConfig (anyRequest().authenticated() +
 * AuthenticationEntryPoint) quem devolve 401 para rotas protegidas — mesmo padrão do
 * JwtAuthorizationFilter do retail-manager. Token presente mas inválido/expirado/na blacklist é
 * tratado aqui mesmo, porque nesses casos queremos rejeitar explicitamente em vez de deixar
 * seguir como anônimo.
 */
@Component
public class JwtAuthenticationFilter extends OncePerRequestFilter {

    private static final String AUTH_HEADER = "Authorization";
    private static final String BEARER_PREFIX = "Bearer ";

    private final JwtService jwtService;
    private final TokenBlacklistService tokenBlacklistService;

    public JwtAuthenticationFilter(JwtService jwtService, TokenBlacklistService tokenBlacklistService) {
        this.jwtService = jwtService;
        this.tokenBlacklistService = tokenBlacklistService;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getRequestURI();
        return path.startsWith("/api/auth/") || path.startsWith("/actuator/health");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        String header = request.getHeader(AUTH_HEADER);
        if (header == null || !header.startsWith(BEARER_PREFIX)) {
            filterChain.doFilter(request, response);
            return;
        }

        String token = header.substring(BEARER_PREFIX.length());
        try {
            Claims claims = jwtService.validarEExtrairClaims(token);
            if (tokenBlacklistService.estaNaBlacklist(claims.getId())) {
                SecurityErrorResponseWriter.write(response, HttpStatus.UNAUTHORIZED,
                        "Token invalidado por logout.");
                return;
            }

            UUID userId = UUID.fromString(claims.getSubject());
            UUID accountId = jwtService.extrairAccountId(claims);
            AuthenticatedUser principal = new AuthenticatedUser(userId, accountId);
            SecurityContextHolder.getContext()
                    .setAuthentication(new UsernamePasswordAuthenticationToken(principal, null, List.of()));
        } catch (JwtException | IllegalArgumentException ex) {
            SecurityErrorResponseWriter.write(response, HttpStatus.UNAUTHORIZED, "Token JWT inválido ou expirado.");
            return;
        }

        filterChain.doFilter(request, response);
    }
}

package com.bancodigital.transfer.infrastructure.config;

import com.bancodigital.transfer.infrastructure.security.JsonAccessDeniedHandler;
import com.bancodigital.transfer.infrastructure.security.JsonAuthenticationEntryPoint;
import com.bancodigital.transfer.infrastructure.security.JwtAuthenticationFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

@Configuration
@EnableWebSecurity
public class SecurityConfig {

    private final JwtAuthenticationFilter jwtAuthenticationFilter;
    private final JsonAuthenticationEntryPoint authenticationEntryPoint;
    private final JsonAccessDeniedHandler accessDeniedHandler;

    public SecurityConfig(JwtAuthenticationFilter jwtAuthenticationFilter,
                           JsonAuthenticationEntryPoint authenticationEntryPoint,
                           JsonAccessDeniedHandler accessDeniedHandler) {
        this.jwtAuthenticationFilter = jwtAuthenticationFilter;
        this.authenticationEntryPoint = authenticationEntryPoint;
        this.accessDeniedHandler = accessDeniedHandler;
    }

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http
                .csrf(csrf -> csrf.disable())
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth
                        // /actuator/prometheus liberado porque o Prometheus faz scrape sem
                        // credencial nenhuma, igual a /actuator/health — restringir essa rota
                        // por rede/firewall é responsabilidade da infra, não desta aplicação.
                        // O handshake HTTP do WebSocket (GET /ws) não pode carregar um header
                        // Authorization (limitação do WebSocket nativo do browser) — a
                        // autenticação de verdade acontece no frame STOMP CONNECT, via
                        // StompAuthChannelInterceptor.
                        // Swagger UI/api-docs liberados para carregar sem JWT — as chamadas reais
                        // feitas a partir do "Try it out" continuam exigindo um token válido.
                        .requestMatchers("/actuator/health", "/actuator/prometheus", "/ws/**",
                                "/v3/api-docs/**", "/swagger-ui/**", "/swagger-ui.html").permitAll()
                        .anyRequest().authenticated())
                .exceptionHandling(ex -> ex
                        .authenticationEntryPoint(authenticationEntryPoint)
                        .accessDeniedHandler(accessDeniedHandler))
                .addFilterBefore(jwtAuthenticationFilter, UsernamePasswordAuthenticationFilter.class);
        return http.build();
    }
}

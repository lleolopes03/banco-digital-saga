package com.bancodigital.transfer.infrastructure.client;

import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.http.client.ClientHttpRequestFactoryBuilder;
import org.springframework.boot.http.client.HttpClientSettings;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.web.client.RestClient;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

@Configuration(proxyBeanMethods = false)
public class AccountServiceClientConfig {

    @Bean
    RestClient accountServiceRestClient(
            RestClient.Builder builder,
            @Value("${account-service.base-url:http://localhost:8081}") String baseUrl,
            @Value("${account-service.connect-timeout:2s}") Duration connectTimeout,
            @Value("${account-service.read-timeout:3s}") Duration readTimeout) {
        HttpClientSettings settings = HttpClientSettings.defaults()
                .withConnectTimeout(connectTimeout)
                .withReadTimeout(readTimeout);
        return builder.baseUrl(baseUrl)
                .requestFactory(ClientHttpRequestFactoryBuilder.detect().build(settings))
                .requestInterceptor(repassarAuthorizationDaRequisicaoAtual())
                .build();
    }

    /*
     * account-service exige JWT em /api/accounts/{id}/balance (desde a fatia de autenticação).
     * Sem repassar o token de quem está criando a transferência, essa chamada sempre voltava
     * 401 e o circuit breaker caía no fallback em 100% dos casos — a pré-checagem de saldo
     * ficava desligada sem ninguém perceber. A chamada acontece na própria thread da requisição
     * HTTP de POST /api/transfers (o aspecto do Resilience4j é síncrono), então o header da
     * requisição corrente está disponível. O token é do próprio dono da conta de origem — o
     * controller já validou isso antes —, então o account-service autoriza a consulta normalmente.
     * Fora de uma requisição HTTP (nenhum caso hoje) o header simplesmente não é enviado e o
     * fallback segue valendo.
     */
    private static ClientHttpRequestInterceptor repassarAuthorizationDaRequisicaoAtual() {
        return (request, body, execution) -> {
            if (RequestContextHolder.getRequestAttributes() instanceof ServletRequestAttributes atributos) {
                String authorization = atributos.getRequest().getHeader(HttpHeaders.AUTHORIZATION);
                if (authorization != null) {
                    request.getHeaders().set(HttpHeaders.AUTHORIZATION, authorization);
                }
            }
            return execution.execute(request, body);
        };
    }
}

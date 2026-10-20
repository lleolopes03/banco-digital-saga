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
     * O account-service exige JWT no saldo: repassa o Authorization da requisição corrente (a chamada é síncrona,
     * na thread do POST /api/transfers). Fora de uma requisição o header não é enviado e vale o fallback.
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

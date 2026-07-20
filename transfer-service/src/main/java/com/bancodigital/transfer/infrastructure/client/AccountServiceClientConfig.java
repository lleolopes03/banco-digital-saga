package com.bancodigital.transfer.infrastructure.client;

import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.http.client.ClientHttpRequestFactoryBuilder;
import org.springframework.boot.http.client.HttpClientSettings;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

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
                .build();
    }
}

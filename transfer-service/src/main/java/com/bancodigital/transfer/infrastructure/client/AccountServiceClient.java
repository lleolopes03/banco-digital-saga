package com.bancodigital.transfer.infrastructure.client;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * Validação síncrona e opcional de saldo, feita antes de abrir uma saga (ver
 * SagaOrchestratorService.iniciarTransferencia). Protegida por circuit breaker: se
 * account-service estiver indisponível, {@link #balanceCheckFallback} deixa a saga seguir sem
 * essa pré-checagem — a validação definitiva continua sendo a reserva assíncrona da saga, que
 * não depende deste cliente.
 */
@Component
public class AccountServiceClient {

    private static final Logger log = LoggerFactory.getLogger(AccountServiceClient.class);

    private final RestClient accountServiceRestClient;

    public AccountServiceClient(RestClient accountServiceRestClient) {
        this.accountServiceRestClient = accountServiceRestClient;
    }

    @CircuitBreaker(name = "accountService", fallbackMethod = "balanceCheckFallback")
    public Optional<BalanceResponse> consultarSaldo(UUID accountId) {
        BalanceResponse response = accountServiceRestClient.get()
                .uri("/api/accounts/{id}/balance", accountId)
                .retrieve()
                .body(BalanceResponse.class);
        return Optional.ofNullable(response);
    }

    @SuppressWarnings("unused")
    private Optional<BalanceResponse> balanceCheckFallback(UUID accountId, Throwable throwable) {
        log.warn("Pulando validação prévia de saldo da conta {} (circuito 'accountService' aberto ou "
                + "chamada falhou): {}", accountId, throwable.toString());
        return Optional.empty();
    }
}

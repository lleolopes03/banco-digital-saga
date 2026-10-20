package com.bancodigital.account.application;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

/**
 * Porto de saída para o cache de saldo: a camada de aplicação decide QUANDO ler, gravar e
 * invalidar, sem saber que o armazenamento é Redis (inversão de dependência). A implementação
 * vive em {@code infrastructure.cache}.
 */
public interface BalanceCache {

    /** Saldo em cache, ou vazio em caso de miss (ou de o cache estar indisponível). */
    Optional<BigDecimal> buscar(UUID accountId);

    void guardar(UUID accountId, BigDecimal saldo);

    void invalidar(UUID accountId);
}

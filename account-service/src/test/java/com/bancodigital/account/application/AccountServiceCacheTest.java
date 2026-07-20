package com.bancodigital.account.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.bancodigital.account.AbstractIntegrationTest;
import com.bancodigital.account.infrastructure.persistence.AccountRepository;
import com.bancodigital.account.web.dto.AccountResponse;
import com.bancodigital.account.web.dto.BalanceResponse;
import com.bancodigital.account.web.dto.CreateAccountRequest;
import com.bancodigital.account.web.dto.DepositRequest;
import java.math.BigDecimal;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

class AccountServiceCacheTest extends AbstractIntegrationTest {

    private static final String BALANCE_CACHE_KEY_PREFIX = "balance:";

    @Autowired
    private AccountService accountService;

    // Spy sobre o bean real (Postgres via Testcontainers, não um mock): deixa a query de
    // verdade acontecer e só nos permite contar quantas vezes findById() foi chamado, para
    // provar que um cache hit realmente evita a segunda ida ao banco.
    @MockitoSpyBean
    private AccountRepository accountRepository;

    @Autowired
    private StringRedisTemplate redisTemplate;

    private UUID criarConta() {
        AccountResponse created = accountService.criarConta(
                new CreateAccountRequest("Cache Teste", String.valueOf(System.nanoTime())));
        return created.id();
    }

    @Test
    void primeiraConsultaDeSaldoEhMissEGravaNoCache() {
        UUID accountId = criarConta();

        BalanceResponse response = accountService.consultarSaldo(accountId);

        assertThat(response.balance()).isEqualByComparingTo(BigDecimal.ZERO);
        String saldoEmCache = redisTemplate.opsForValue().get(BALANCE_CACHE_KEY_PREFIX + accountId);
        assertThat(saldoEmCache).isNotNull();
        assertThat(new BigDecimal(saldoEmCache)).isEqualByComparingTo(BigDecimal.ZERO);
        verify(accountRepository, times(1)).findById(accountId);
    }

    @Test
    void segundaConsultaEhHitENaoVoltaAConsultarOPostgres() {
        UUID accountId = criarConta();

        accountService.consultarSaldo(accountId);
        verify(accountRepository, times(1)).findById(accountId);

        BalanceResponse segundaConsulta = accountService.consultarSaldo(accountId);

        assertThat(segundaConsulta.balance()).isEqualByComparingTo(BigDecimal.ZERO);
        // Continua em 1: a segunda chamada veio do Redis, não gerou uma nova query.
        verify(accountRepository, times(1)).findById(accountId);
    }

    @Test
    void depositoInvalidaCacheEProximaLeituraRefleteNovoSaldo() {
        UUID accountId = criarConta();
        accountService.consultarSaldo(accountId); // popula o cache com 0.00

        accountService.depositar(accountId, new DepositRequest(new BigDecimal("150.00"), "salário"));

        String cacheAposDeposito = redisTemplate.opsForValue().get(BALANCE_CACHE_KEY_PREFIX + accountId);
        assertThat(cacheAposDeposito).isNull();

        BalanceResponse response = accountService.consultarSaldo(accountId);
        assertThat(response.balance()).isEqualByComparingTo("150.00");
    }
}

package com.bancodigital.antifraud.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import com.bancodigital.antifraud.domain.event.TransferReserved;
import com.bancodigital.antifraud.infrastructure.persistence.BlacklistedAccountRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

@ExtendWith(MockitoExtension.class)
class AntiFraudRuleEngineTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @Mock
    private BlacklistedAccountRepository blacklistedAccountRepository;

    private AntiFraudRuleEngine engine;

    @BeforeEach
    void setUp() {
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);

        LimiteValorRule limiteValorRule = new LimiteValorRule();
        VelocityCheckRule velocityCheckRule = new VelocityCheckRule(redisTemplate);
        BlacklistRule blacklistRule = new BlacklistRule(blacklistedAccountRepository);
        engine = new AntiFraudRuleEngine(limiteValorRule, velocityCheckRule, blacklistRule);
    }

    private TransferReserved transferencia(UUID accountOrigemId, UUID accountDestinoId, BigDecimal valor) {
        return new TransferReserved(UUID.randomUUID(), accountOrigemId, accountDestinoId, valor, Instant.now());
    }

    @Test
    void deveAprovarTransferenciaDentroDoLimiteESemHistorico() {
        UUID origem = UUID.randomUUID();
        UUID destino = UUID.randomUUID();
        when(valueOperations.increment(anyString())).thenReturn(1L);
        when(blacklistedAccountRepository.existsById(origem)).thenReturn(false);
        when(blacklistedAccountRepository.existsById(destino)).thenReturn(false);

        Optional<String> resultado = engine.avaliar(transferencia(origem, destino, new BigDecimal("100.00")));

        assertThat(resultado).isEmpty();
    }

    @Test
    void deveRejeitarTransferenciaAcimaDoLimiteComMotivoLimiteExcedido() {
        TransferReserved event = transferencia(UUID.randomUUID(), UUID.randomUUID(), new BigDecimal("10000.01"));

        Optional<String> resultado = engine.avaliar(event);

        assertThat(resultado).contains("LIMITE_EXCEDIDO");
    }

    @Test
    void deveRejeitarSextaTransferenciaEm60SegundosComMotivoVelocityExcedida() {
        UUID origem = UUID.randomUUID();
        UUID destino = UUID.randomUUID();
        when(valueOperations.increment(anyString())).thenReturn(6L);

        Optional<String> resultado = engine.avaliar(transferencia(origem, destino, new BigDecimal("100.00")));

        assertThat(resultado).contains("VELOCITY_EXCEDIDA");
    }

    @Test
    void deveRejeitarContaNaBlacklistComMotivoContaBloqueada() {
        UUID origem = UUID.randomUUID();
        UUID destino = UUID.randomUUID();
        when(valueOperations.increment(anyString())).thenReturn(1L);
        when(blacklistedAccountRepository.existsById(origem)).thenReturn(true);

        Optional<String> resultado = engine.avaliar(transferencia(origem, destino, new BigDecimal("100.00")));

        assertThat(resultado).contains("CONTA_BLOQUEADA");
    }
}

package com.bancodigital.antifraud.infrastructure.idempotency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bancodigital.antifraud.domain.ProcessedEvent;
import com.bancodigital.antifraud.infrastructure.persistence.ProcessedEventRepository;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

@ExtendWith(MockitoExtension.class)
class IdempotentEventHandlerTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @Mock
    private ProcessedEventRepository processedEventRepository;

    private IdempotentEventHandler handler;

    @BeforeEach
    void setUp() {
        handler = new IdempotentEventHandler(redisTemplate, processedEventRepository, "antifraud-service");
    }

    @Test
    void deveProcessarERegistrarEventoNovo() {
        UUID eventId = UUID.randomUUID();
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.setIfAbsent(eq("idempotency:antifraud-service:" + eventId), any(), any(Duration.class))).thenReturn(true);
        when(processedEventRepository.existsById(eventId)).thenReturn(false);

        AtomicInteger execucoes = new AtomicInteger();
        handler.processarSeNovo(eventId, "TesteEvento", execucoes::incrementAndGet);

        assertThat(execucoes.get()).isEqualTo(1);
        verify(processedEventRepository).save(any(ProcessedEvent.class));
    }

    @Test
    void naoDeveChamarHandlerDeNegocioNovamenteQuandoEventoJaMarcadoNoRedis() {
        UUID eventId = UUID.randomUUID();
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.setIfAbsent(eq("idempotency:antifraud-service:" + eventId), any(), any(Duration.class))).thenReturn(false);

        AtomicInteger execucoes = new AtomicInteger();
        handler.processarSeNovo(eventId, "TesteEvento", execucoes::incrementAndGet);

        assertThat(execucoes.get()).isZero();
        verify(processedEventRepository, never()).existsById(any());
        verify(processedEventRepository, never()).save(any());
    }

    @Test
    void naoDeveChamarHandlerDeNegocioNovamenteQuandoJaRegistradoNoPostgresComRedisLimpo() {
        UUID eventId = UUID.randomUUID();
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.setIfAbsent(eq("idempotency:antifraud-service:" + eventId), any(), any(Duration.class))).thenReturn(true);
        when(processedEventRepository.existsById(eventId)).thenReturn(true);

        AtomicInteger execucoes = new AtomicInteger();
        handler.processarSeNovo(eventId, "TesteEvento", execucoes::incrementAndGet);

        assertThat(execucoes.get()).isZero();
        verify(processedEventRepository, never()).save(any());
    }

    @Test
    void deveLiberarChaveDoRedisSeProcessamentoFalharParaPermitirRetryLegitimo() {
        UUID eventId = UUID.randomUUID();
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.setIfAbsent(eq("idempotency:antifraud-service:" + eventId), any(), any(Duration.class))).thenReturn(true);
        when(processedEventRepository.existsById(eventId)).thenReturn(false);

        assertThatThrownBy(() -> handler.processarSeNovo(eventId, "TesteEvento", () -> {
            throw new RuntimeException("falha simulada");
        })).isInstanceOf(RuntimeException.class);

        verify(redisTemplate).delete("idempotency:antifraud-service:" + eventId);
        verify(processedEventRepository, never()).save(any());
    }
}

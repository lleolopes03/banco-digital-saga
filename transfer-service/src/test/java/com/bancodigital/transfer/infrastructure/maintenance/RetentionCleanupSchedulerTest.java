package com.bancodigital.transfer.infrastructure.maintenance;

import static org.assertj.core.api.Assertions.assertThat;

import com.bancodigital.transfer.AbstractIntegrationTest;
import com.bancodigital.transfer.domain.OutboxEvent;
import com.bancodigital.transfer.domain.ProcessedEvent;
import com.bancodigital.transfer.infrastructure.persistence.OutboxEventRepository;
import com.bancodigital.transfer.infrastructure.persistence.ProcessedEventRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

class RetentionCleanupSchedulerTest extends AbstractIntegrationTest {

    @Autowired
    private RetentionCleanupScheduler scheduler;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private JdbcTemplate jdbc;

    private UUID outboxPublicado(Duration idade) {
        OutboxEvent e = OutboxEvent.pending(UUID.randomUUID(), "Transfer", UUID.randomUUID(), "TesteRetencao", "{}", null);
        e.markPublished();
        UUID id = outboxEventRepository.saveAndFlush(e).getId();
        jdbc.update("UPDATE outbox_events SET published_at = ? WHERE id = ?",
                java.sql.Timestamp.from(Instant.now().minus(idade)), id);
        return id;
    }

    private UUID outboxPendente(Duration idade) {
        OutboxEvent e = OutboxEvent.pending(UUID.randomUUID(), "Transfer", UUID.randomUUID(), "TesteRetencao", "{}", null);
        UUID id = outboxEventRepository.saveAndFlush(e).getId();
        jdbc.update("UPDATE outbox_events SET created_at = ? WHERE id = ?",
                java.sql.Timestamp.from(Instant.now().minus(idade)), id);
        return id;
    }

    private UUID processado(Duration idade) {
        UUID id = processedEventRepository.saveAndFlush(ProcessedEvent.agora(UUID.randomUUID())).getEventId();
        jdbc.update("UPDATE processed_events SET processed_at = ? WHERE event_id = ?",
                java.sql.Timestamp.from(Instant.now().minus(idade)), id);
        return id;
    }

    @Test
    void deveApagarSoOutboxPublicadoAntigoENuncaOPendente() {
        UUID publicadoAntigo = outboxPublicado(Duration.ofDays(30));
        UUID publicadoRecente = outboxPublicado(Duration.ofHours(1));
        UUID pendenteAntigo = outboxPendente(Duration.ofDays(30));

        RetentionCleanupScheduler.Resultado r = scheduler.executar(Instant.now());

        assertThat(r.outbox()).isGreaterThanOrEqualTo(1);
        assertThat(outboxEventRepository.existsById(publicadoAntigo)).isFalse();
        assertThat(outboxEventRepository.existsById(publicadoRecente)).isTrue();
        // invariante critico: um evento ainda nao publicado jamais pode ser descartado
        assertThat(outboxEventRepository.existsById(pendenteAntigo)).isTrue();
    }

    @Test
    void deveApagarSoProcessedEventsMaisAntigosQueAJanelaDeRetencao() {
        UUID antigo = processado(Duration.ofDays(30));
        UUID dentroDaJanela = processado(Duration.ofDays(10)); // < 14 dias: ainda protege contra reentrega

        RetentionCleanupScheduler.Resultado r = scheduler.executar(Instant.now());

        assertThat(r.processedEvents()).isGreaterThanOrEqualTo(1);
        assertThat(processedEventRepository.existsById(antigo)).isFalse();
        assertThat(processedEventRepository.existsById(dentroDaJanela)).isTrue();
    }
}

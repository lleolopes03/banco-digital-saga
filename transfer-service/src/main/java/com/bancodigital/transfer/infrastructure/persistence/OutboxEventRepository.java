package com.bancodigital.transfer.infrastructure.persistence;

import com.bancodigital.transfer.domain.OutboxEvent;
import com.bancodigital.transfer.domain.OutboxEventStatus;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    List<OutboxEvent> findByStatusOrderByCreatedAtAsc(OutboxEventStatus status, Pageable pageable);

    /**
     * Retenção: apaga em lote eventos JÁ PUBLICADOS anteriores ao limite. Nunca toca em PENDING.
     * Usa subselect com LIMIT para o lote ser pequeno e a transação curta.
     */
    @Modifying
    @Query(value = "DELETE FROM outbox_events WHERE id IN (SELECT id FROM outbox_events "
            + "WHERE status = 'PUBLISHED' AND published_at < :limite LIMIT :lote)", nativeQuery = true)
    int apagarPublicadosAntesDe(@Param("limite") Instant limite, @Param("lote") int lote);}

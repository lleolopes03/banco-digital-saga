package com.bancodigital.antifraud.infrastructure.persistence;

import com.bancodigital.antifraud.domain.OutboxEvent;
import com.bancodigital.antifraud.domain.OutboxEventStatus;
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
     * Seleciona e trava (na transação corrente) os próximos eventos a publicar, de forma segura para várias
     * instâncias: {@code FOR UPDATE SKIP LOCKED} impede que duas peguem a mesma linha, e o {@code NOT EXISTS}
     * devolve só o evento mais antigo ainda PENDING de cada agregado, preservando a ordem por saga.
     */
    @Query(value = "SELECT o.* FROM outbox_events o WHERE o.status = 'PENDING' "
            + "AND NOT EXISTS (SELECT 1 FROM outbox_events anterior "
            + "WHERE anterior.aggregate_id = o.aggregate_id AND anterior.status = 'PENDING' "
            + "AND (anterior.created_at < o.created_at "
            + "OR (anterior.created_at = o.created_at AND anterior.id < o.id))) "
            + "ORDER BY o.created_at, o.id LIMIT :limite FOR UPDATE OF o SKIP LOCKED", nativeQuery = true)
    List<OutboxEvent> reivindicarPendentes(@Param("limite") int limite);

    /**
     * Retenção: apaga em lote eventos JÁ PUBLICADOS anteriores ao limite. Nunca toca em PENDING.
     * Usa subselect com LIMIT para o lote ser pequeno e a transação curta.
     */
    @Modifying
    @Query(value = "DELETE FROM outbox_events WHERE id IN (SELECT id FROM outbox_events "
            + "WHERE status = 'PUBLISHED' AND published_at < :limite LIMIT :lote)", nativeQuery = true)
    int apagarPublicadosAntesDe(@Param("limite") Instant limite, @Param("lote") int lote);}

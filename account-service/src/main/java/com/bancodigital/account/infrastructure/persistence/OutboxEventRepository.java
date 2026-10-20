package com.bancodigital.account.infrastructure.persistence;

import com.bancodigital.account.domain.OutboxEvent;
import com.bancodigital.account.domain.OutboxEventStatus;
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
     * Seleciona e TRAVA (na transação corrente) os próximos eventos a publicar, de forma segura
     * para várias instâncias do serviço rodando o poller ao mesmo tempo:
     *  - FOR UPDATE SKIP LOCKED: cada instância pega linhas que nenhuma outra tem travadas, sem
     *    esperar nem publicar o mesmo evento duas vezes.
     *  - NOT EXISTS: só devolve o evento mais antigo ainda PENDING de cada agregado (transferId).
     *    Uma linha travada por outra instância continua PENDING até o commit dela, então os
     *    eventos seguintes do mesmo agregado ficam de fora e a ordem por saga é preservada, também
     *    quando um envio falha (o evento fica PENDING e bloqueia os posteriores até ser reenviado).
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

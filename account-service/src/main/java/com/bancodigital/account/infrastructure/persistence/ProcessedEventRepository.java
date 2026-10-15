package com.bancodigital.account.infrastructure.persistence;

import com.bancodigital.account.domain.ProcessedEvent;
import java.time.Instant;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, UUID> {

    /** Retenção: apaga em lote registros de deduplicação anteriores ao limite. */
    @Modifying
    @Query(value = "DELETE FROM processed_events WHERE event_id IN (SELECT event_id FROM processed_events "
            + "WHERE processed_at < :limite LIMIT :lote)", nativeQuery = true)
    int apagarProcessadosAntesDe(@Param("limite") Instant limite, @Param("lote") int lote);}

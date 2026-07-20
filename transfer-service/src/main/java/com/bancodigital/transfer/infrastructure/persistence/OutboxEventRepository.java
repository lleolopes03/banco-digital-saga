package com.bancodigital.transfer.infrastructure.persistence;

import com.bancodigital.transfer.domain.OutboxEvent;
import com.bancodigital.transfer.domain.OutboxEventStatus;
import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    List<OutboxEvent> findByStatusOrderByCreatedAtAsc(OutboxEventStatus status, Pageable pageable);
}

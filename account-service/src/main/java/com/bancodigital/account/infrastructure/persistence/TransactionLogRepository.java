package com.bancodigital.account.infrastructure.persistence;

import com.bancodigital.account.domain.TransactionLogEntry;
import java.time.Instant;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TransactionLogRepository extends JpaRepository<TransactionLogEntry, UUID> {

    Page<TransactionLogEntry> findByAccountIdAndCreatedAtBetweenOrderByCreatedAtDesc(UUID accountId, Instant from,
                                                                                      Instant to, Pageable pageable);
}

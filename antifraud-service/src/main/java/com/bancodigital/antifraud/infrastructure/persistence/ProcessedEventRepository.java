package com.bancodigital.antifraud.infrastructure.persistence;

import com.bancodigital.antifraud.domain.ProcessedEvent;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, UUID> {
}

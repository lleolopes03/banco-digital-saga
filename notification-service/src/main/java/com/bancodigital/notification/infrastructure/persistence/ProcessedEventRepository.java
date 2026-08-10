package com.bancodigital.notification.infrastructure.persistence;

import com.bancodigital.notification.domain.ProcessedEvent;
import java.util.UUID;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface ProcessedEventRepository extends MongoRepository<ProcessedEvent, UUID> {
}

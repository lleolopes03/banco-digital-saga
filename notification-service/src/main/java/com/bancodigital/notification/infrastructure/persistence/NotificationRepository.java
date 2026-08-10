package com.bancodigital.notification.infrastructure.persistence;

import com.bancodigital.notification.domain.Notification;
import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface NotificationRepository extends MongoRepository<Notification, String> {

    Page<Notification> findByAccountIdOrderByCreatedAtDesc(UUID accountId, Pageable pageable);

    List<Notification> findByTransferId(UUID transferId);
}

package com.bancodigital.transfer.infrastructure.persistence;

import com.bancodigital.transfer.domain.SagaInstance;
import com.bancodigital.transfer.domain.SagaStatus;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface SagaInstanceRepository extends JpaRepository<SagaInstance, UUID> {

    List<SagaInstance> findByStatusIn(List<SagaStatus> statuses);

    Optional<SagaInstance> findByIdempotencyKey(String idempotencyKey);
}

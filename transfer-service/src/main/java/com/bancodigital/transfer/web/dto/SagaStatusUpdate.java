package com.bancodigital.transfer.web.dto;

import com.bancodigital.transfer.domain.SagaInstance;
import com.bancodigital.transfer.domain.SagaStatus;
import java.time.Instant;
import java.util.UUID;

public record SagaStatusUpdate(
        UUID transferId,
        SagaStatus status,
        String failureReason,
        Instant updatedAt
) {

    public static SagaStatusUpdate from(SagaInstance saga) {
        return new SagaStatusUpdate(saga.getId(), saga.getStatus(), saga.getFailureReason(), saga.getUpdatedAt());
    }
}

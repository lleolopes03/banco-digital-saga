package com.bancodigital.transfer.web.dto;

import com.bancodigital.transfer.domain.SagaInstance;
import com.bancodigital.transfer.domain.SagaStatus;
import java.math.BigDecimal;
import java.util.UUID;

public record TransferResponse(
        UUID transferId,
        UUID accountOrigemId,
        UUID accountDestinoId,
        BigDecimal valor,
        SagaStatus status,
        String failureReason
) {

    public static TransferResponse from(SagaInstance saga) {
        return new TransferResponse(saga.getId(), saga.getAccountOrigemId(), saga.getAccountDestinoId(),
                saga.getValor(), saga.getStatus(), saga.getFailureReason());
    }
}

package com.bancodigital.transfer.web.dto;

import com.bancodigital.transfer.domain.SagaInstance;
import com.bancodigital.transfer.domain.SagaStatus;
import io.swagger.v3.oas.annotations.media.Schema;
import java.math.BigDecimal;
import java.util.UUID;

@Schema(description = "Estado atual de uma transferência (a saga).")
public record TransferResponse(
        @Schema(description = "Identificador da transferência.", example = "5b1c2d3e-4f50-4a61-8b72-93a4b5c6d7e8") UUID transferId,
        @Schema(description = "Conta de origem.") UUID accountOrigemId,
        @Schema(description = "Conta de destino.") UUID accountDestinoId,
        @Schema(description = "Valor, em reais.", example = "100.00") BigDecimal valor,
        @Schema(description = "Etapa atual da saga; COMPLETED e FAILED são finais.", example = "COMPLETED") SagaStatus status,
        @Schema(description = "Motivo da falha quando status = FAILED (ex.: SALDO_INSUFICIENTE, SAGA_TIMEOUT).", example = "SALDO_INSUFICIENTE", nullable = true)
        String failureReason
) {

    public static TransferResponse from(SagaInstance saga) {
        return new TransferResponse(saga.getId(), saga.getAccountOrigemId(), saga.getAccountDestinoId(),
                saga.getValor(), saga.getStatus(), saga.getFailureReason());
    }
}

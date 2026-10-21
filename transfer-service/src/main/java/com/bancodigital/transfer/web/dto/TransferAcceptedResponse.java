package com.bancodigital.transfer.web.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.UUID;

@Schema(description = "Confirmação de que o pedido foi aceito.")
public record TransferAcceptedResponse(
        @Schema(description = "Identificador da transferência, para acompanhar a saga.", example = "5b1c2d3e-4f50-4a61-8b72-93a4b5c6d7e8")
        UUID transferId) {
}

package com.bancodigital.transfer.web.controller;

import com.bancodigital.transfer.application.TransferInitiationService;
import com.bancodigital.transfer.application.TransferQueryService;
import com.bancodigital.transfer.infrastructure.security.AuthenticatedUser;
import com.bancodigital.transfer.web.dto.CreateTransferRequest;
import com.bancodigital.transfer.web.dto.ErrorResponse;
import com.bancodigital.transfer.web.dto.TransferAcceptedResponse;
import com.bancodigital.transfer.web.dto.TransferResponse;
import com.bancodigital.transfer.web.openapi.RespostaAcessoNegado;
import com.bancodigital.transfer.web.openapi.RespostaDadosInvalidos;
import com.bancodigital.transfer.web.openapi.RespostaNaoAutenticado;
import com.bancodigital.transfer.web.openapi.RespostaTransferenciaNaoEncontrada;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.headers.Header;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/transfers")
@Tag(name = "Transferências", description = "Abertura e acompanhamento de transferências entre contas. O processamento é"
        + " assíncrono (saga orquestrada via Kafka): a requisição só aceita o pedido.")
public class TransferController {

    private final TransferInitiationService transferInitiationService;
    private final TransferQueryService transferQueryService;

    public TransferController(TransferInitiationService transferInitiationService,
            TransferQueryService transferQueryService) {
        this.transferInitiationService = transferInitiationService;
        this.transferQueryService = transferQueryService;
    }

    @PostMapping
    @Operation(summary = "Solicita uma transferência",
            description = "Aceita o pedido (202) e devolve o identificador da transferência; reserva de saldo, análise"
                    + " antifraude, débito e crédito acontecem depois, de forma assíncrona. Acompanhe o resultado em"
                    + " GET /api/transfers/{id} ou pelo WebSocket. A conta de origem deve ser a do usuário"
                    + " autenticado. Há uma pré-checagem síncrona de saldo (best-effort).")
    @ApiResponse(responseCode = "202", description = "Pedido aceito; a saga continua em segundo plano.",
            content = @Content(schema = @Schema(implementation = TransferAcceptedResponse.class)))
    @ApiResponse(responseCode = "422", description = "Saldo insuficiente na pré-checagem síncrona.",
            content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    @ApiResponse(responseCode = "429", description = "Limite de transferências por conta excedido na janela de tempo."
            + " O header Retry-After informa em quantos segundos tentar de novo.",
            headers = @Header(name = "Retry-After", description = "Segundos até a próxima tentativa permitida."),
            content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    @RespostaDadosInvalidos
    @RespostaNaoAutenticado
    @RespostaAcessoNegado
    public ResponseEntity<TransferAcceptedResponse> iniciarTransferencia(
            @Valid @RequestBody CreateTransferRequest request,
            @Parameter(description = "Chave de idempotência (opcional): repetir o pedido com a mesma chave devolve a"
                    + " mesma transferência, sem criar outra.", example = "2f6c1c0a-7b1e-4d0c-9a52-3c5e6f7a8b9c")
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @AuthenticationPrincipal AuthenticatedUser principal) {
        if (!principal.accountId().equals(request.accountOrigemId())) {
            throw new AccessDeniedException(
                    "A conta de origem " + request.accountOrigemId() + " não pertence ao usuário autenticado.");
        }
        UUID transferId = transferInitiationService.iniciarTransferencia(request.accountOrigemId(),
                request.accountDestinoId(), request.valor(), idempotencyKey);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(new TransferAcceptedResponse(transferId));
    }

    @GetMapping("/{id}")
    @Operation(summary = "Consulta uma transferência",
            description = "Devolve o estado atual da saga (STARTED, RESERVED, ANTIFRAUD_PENDING, ..., COMPLETED ou"
                    + " FAILED) e, se falhou, o motivo.")
    @ApiResponse(responseCode = "200", description = "Estado atual da transferência.")
    @RespostaNaoAutenticado
    @RespostaTransferenciaNaoEncontrada
    public ResponseEntity<TransferResponse> consultarTransferencia(
            @Parameter(description = "Identificador devolvido ao solicitar a transferência.") @PathVariable UUID id) {
        return ResponseEntity.ok(TransferResponse.from(transferQueryService.consultarSaga(id)));
    }
}

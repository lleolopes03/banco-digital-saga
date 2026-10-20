package com.bancodigital.transfer.web.controller;

import com.bancodigital.transfer.application.TransferInitiationService;
import com.bancodigital.transfer.application.TransferQueryService;
import com.bancodigital.transfer.infrastructure.security.AuthenticatedUser;
import com.bancodigital.transfer.web.dto.CreateTransferRequest;
import com.bancodigital.transfer.web.dto.TransferAcceptedResponse;
import com.bancodigital.transfer.web.dto.TransferResponse;
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
public class TransferController {

    private final TransferInitiationService transferInitiationService;
    private final TransferQueryService transferQueryService;

    public TransferController(TransferInitiationService transferInitiationService,
                              TransferQueryService transferQueryService) {
        this.transferInitiationService = transferInitiationService;
        this.transferQueryService = transferQueryService;
    }

    @PostMapping
    public ResponseEntity<TransferAcceptedResponse> iniciarTransferencia(
            @Valid @RequestBody CreateTransferRequest request,
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
    public ResponseEntity<TransferResponse> consultarTransferencia(@PathVariable UUID id) {
        return ResponseEntity.ok(TransferResponse.from(transferQueryService.consultarSaga(id)));
    }
}

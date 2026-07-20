package com.bancodigital.account.web.controller;

import com.bancodigital.account.application.AccountService;
import com.bancodigital.account.infrastructure.security.AuthenticatedUser;
import com.bancodigital.account.web.dto.AccountResponse;
import com.bancodigital.account.web.dto.BalanceResponse;
import com.bancodigital.account.web.dto.CreateAccountRequest;
import com.bancodigital.account.web.dto.DepositRequest;
import com.bancodigital.account.web.dto.PageResponse;
import com.bancodigital.account.web.dto.TransactionResponse;
import jakarta.validation.Valid;
import java.net.URI;
import java.time.LocalDate;
import java.util.UUID;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/accounts")
public class AccountController {

    private final AccountService accountService;

    public AccountController(AccountService accountService) {
        this.accountService = accountService;
    }

    @PostMapping
    public ResponseEntity<AccountResponse> criarConta(@Valid @RequestBody CreateAccountRequest request) {
        AccountResponse response = accountService.criarConta(request);
        return ResponseEntity.created(URI.create("/api/accounts/" + response.id())).body(response);
    }

    @GetMapping("/{id}")
    public ResponseEntity<AccountResponse> buscarPorId(@PathVariable UUID id, @AuthenticationPrincipal AuthenticatedUser principal) {
        validarAcessoConta(id, principal);
        return ResponseEntity.ok(AccountResponse.from(accountService.buscarPorId(id)));
    }

    @GetMapping("/{id}/balance")
    public ResponseEntity<BalanceResponse> consultarSaldo(@PathVariable UUID id, @AuthenticationPrincipal AuthenticatedUser principal) {
        validarAcessoConta(id, principal);
        return ResponseEntity.ok(accountService.consultarSaldo(id));
    }

    @PostMapping("/{id}/deposits")
    public ResponseEntity<AccountResponse> depositar(@PathVariable UUID id, @Valid @RequestBody DepositRequest request,
                                                       @AuthenticationPrincipal AuthenticatedUser principal) {
        validarAcessoConta(id, principal);
        return ResponseEntity.ok(accountService.depositar(id, request));
    }

    @GetMapping("/{id}/statement")
    public ResponseEntity<PageResponse<TransactionResponse>> consultarExtrato(
            @PathVariable UUID id,
            @RequestParam(required = false) LocalDate de,
            @RequestParam(required = false) LocalDate ate,
            @RequestParam(name = "pagina", defaultValue = "0") int pagina,
            @RequestParam(name = "tamanhoPagina", defaultValue = "20") int tamanhoPagina,
            @AuthenticationPrincipal AuthenticatedUser principal) {
        validarAcessoConta(id, principal);
        Pageable pageable = PageRequest.of(pagina, tamanhoPagina);
        return ResponseEntity.ok(accountService.consultarExtrato(id, de, ate, pageable));
    }

    private void validarAcessoConta(UUID id, AuthenticatedUser principal) {
        if (!principal.accountId().equals(id)) {
            throw new AccessDeniedException("A conta " + id + " não pertence ao usuário autenticado.");
        }
    }
}

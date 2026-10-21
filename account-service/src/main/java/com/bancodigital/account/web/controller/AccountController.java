package com.bancodigital.account.web.controller;

import com.bancodigital.account.application.AccountService;
import com.bancodigital.account.infrastructure.security.AuthenticatedUser;
import com.bancodigital.account.web.dto.AccountResponse;
import com.bancodigital.account.web.dto.BalanceResponse;
import com.bancodigital.account.web.dto.CreateAccountRequest;
import com.bancodigital.account.web.dto.DepositRequest;
import com.bancodigital.account.web.dto.ErrorResponse;
import com.bancodigital.account.web.dto.PageResponse;
import com.bancodigital.account.web.dto.TransactionResponse;
import com.bancodigital.account.web.openapi.RespostaAcessoNegado;
import com.bancodigital.account.web.openapi.RespostaContaNaoEncontrada;
import com.bancodigital.account.web.openapi.RespostaDadosInvalidos;
import com.bancodigital.account.web.openapi.RespostaNaoAutenticado;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
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
@Tag(name = "Contas", description = "Abertura de conta, saldo, depósitos e extrato. Cada usuário só acessa a própria conta.")
public class AccountController {

    private final AccountService accountService;

    public AccountController(AccountService accountService) {
        this.accountService = accountService;
    }

    @PostMapping
    @Operation(summary = "Abre uma conta",
            description = "Cria uma conta com saldo zero para o titular. O CPF (11 dígitos) deve ser único.")
    @ApiResponse(responseCode = "201", description = "Conta criada. O header Location aponta para a conta.",
            content = @Content(schema = @Schema(implementation = AccountResponse.class)))
    @ApiResponse(responseCode = "409", description = "Já existe uma conta com este CPF.",
            content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    @RespostaDadosInvalidos
    @RespostaNaoAutenticado
    public ResponseEntity<AccountResponse> criarConta(@Valid @RequestBody CreateAccountRequest request) {
        AccountResponse response = accountService.criarConta(request);
        return ResponseEntity.created(URI.create("/api/accounts/" + response.id())).body(response);
    }

    @GetMapping("/{id}")
    @Operation(summary = "Consulta uma conta", description = "Devolve os dados cadastrais e o saldo da conta.")
    @ApiResponse(responseCode = "200", description = "Dados da conta.")
    @RespostaNaoAutenticado
    @RespostaAcessoNegado
    @RespostaContaNaoEncontrada
    public ResponseEntity<AccountResponse> buscarPorId(
            @Parameter(description = "Identificador da conta.") @PathVariable UUID id,
            @AuthenticationPrincipal AuthenticatedUser principal) {
        validarAcessoConta(id, principal);
        return ResponseEntity.ok(AccountResponse.from(accountService.buscarPorId(id)));
    }

    @GetMapping("/{id}/balance")
    @Operation(summary = "Consulta o saldo",
            description = "Devolve o saldo disponível. É a leitura mais frequente do sistema, por isso vem de um cache"
                    + " de até 30 segundos, invalidado a cada operação que altera o saldo.")
    @ApiResponse(responseCode = "200", description = "Saldo disponível.")
    @RespostaNaoAutenticado
    @RespostaAcessoNegado
    @RespostaContaNaoEncontrada
    public ResponseEntity<BalanceResponse> consultarSaldo(
            @Parameter(description = "Identificador da conta.") @PathVariable UUID id,
            @AuthenticationPrincipal AuthenticatedUser principal) {
        validarAcessoConta(id, principal);
        return ResponseEntity.ok(accountService.consultarSaldo(id));
    }

    @PostMapping("/{id}/deposits")
    @Operation(summary = "Deposita na conta",
            description = "Credita o valor informado (maior que zero) e registra o lançamento no extrato.")
    @ApiResponse(responseCode = "409", description = "Conflito de concorrência na conta após as novas tentativas;"
            + " a requisição pode ser repetida.",
            content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    @ApiResponse(responseCode = "200", description = "Depósito registrado; devolve a conta com o saldo atualizado.")
    @RespostaDadosInvalidos
    @RespostaNaoAutenticado
    @RespostaAcessoNegado
    @RespostaContaNaoEncontrada
    public ResponseEntity<AccountResponse> depositar(
            @Parameter(description = "Identificador da conta.") @PathVariable UUID id,
            @Valid @RequestBody DepositRequest request,
            @AuthenticationPrincipal AuthenticatedUser principal) {
        validarAcessoConta(id, principal);
        return ResponseEntity.ok(accountService.depositar(id, request));
    }

    @GetMapping("/{id}/statement")
    @Operation(summary = "Consulta o extrato",
            description = "Lista os lançamentos da conta, do mais recente para o mais antigo, com paginação e filtro"
                    + " opcional por período (datas inclusivas, em UTC).")
    @ApiResponse(responseCode = "200", description = "Página do extrato.")
    @RespostaNaoAutenticado
    @RespostaAcessoNegado
    @RespostaContaNaoEncontrada
    public ResponseEntity<PageResponse<TransactionResponse>> consultarExtrato(
            @Parameter(description = "Identificador da conta.") @PathVariable UUID id,
            @Parameter(description = "Data inicial do período (AAAA-MM-DD).", example = "2026-10-01")
            @RequestParam(required = false) LocalDate de,
            @Parameter(description = "Data final do período (AAAA-MM-DD).", example = "2026-10-31")
            @RequestParam(required = false) LocalDate ate,
            @Parameter(description = "Número da página, começando em 0.")
            @RequestParam(name = "pagina", defaultValue = "0") int pagina,
            @Parameter(description = "Itens por página.")
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

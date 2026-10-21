package com.bancodigital.account.web.controller;

import com.bancodigital.account.application.AuthService;
import com.bancodigital.account.web.dto.ErrorResponse;
import com.bancodigital.account.web.dto.LoginRequest;
import com.bancodigital.account.web.dto.LoginResponse;
import com.bancodigital.account.web.openapi.RespostaDadosInvalidos;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirements;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/auth")
@Tag(name = "Autenticação", description = "Login e logout com JWT (HS256). O token revogado no logout vai para uma blacklist no Redis.")
public class AuthController {

    private static final String BEARER_PREFIX = "Bearer ";

    private final AuthService authService;

    public AuthController(AuthService authService) {
        this.authService = authService;
    }

    @PostMapping("/login")
    @SecurityRequirements
    @Operation(summary = "Autentica o usuário",
            description = "Valida e-mail e senha e devolve um token JWT. Use-o no botão Authorize (ou no header"
                    + " Authorization: Bearer) para chamar os demais endpoints. Endpoint público.")
    @ApiResponse(responseCode = "200", description = "Autenticado; devolve o token JWT e sua validade.")
    @ApiResponse(responseCode = "401", description = "E-mail ou senha incorretos.",
            content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    @RespostaDadosInvalidos
    public ResponseEntity<LoginResponse> login(@Valid @RequestBody LoginRequest request) {
        return ResponseEntity.ok(authService.login(request));
    }

    @PostMapping("/logout")
    @Operation(summary = "Encerra a sessão",
            description = "Revoga o token informado: ele passa a ser recusado em todos os endpoints até expirar.")
    @ApiResponse(responseCode = "204", description = "Token revogado.")
    @ApiResponse(responseCode = "401", description = "Token ausente, inválido ou já expirado.",
            content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    public ResponseEntity<Void> logout(@Parameter(hidden = true) @RequestHeader("Authorization") String authorizationHeader) {
        String token = authorizationHeader.startsWith(BEARER_PREFIX)
                ? authorizationHeader.substring(BEARER_PREFIX.length())
                : authorizationHeader;
        authService.logout(token);
        return ResponseEntity.noContent().build();
    }
}

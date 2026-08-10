package com.bancodigital.notification.web.controller;

import com.bancodigital.notification.domain.Notification;
import com.bancodigital.notification.infrastructure.persistence.NotificationRepository;
import com.bancodigital.notification.infrastructure.security.AuthenticatedUser;
import com.bancodigital.notification.web.dto.NotificationResponse;
import com.bancodigital.notification.web.dto.PageResponse;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/notifications")
public class NotificationController {

    private final NotificationRepository notificationRepository;

    public NotificationController(NotificationRepository notificationRepository) {
        this.notificationRepository = notificationRepository;
    }

    @GetMapping("/{accountId}")
    public ResponseEntity<PageResponse<NotificationResponse>> listarPorConta(
            @PathVariable UUID accountId,
            @RequestParam(name = "pagina", defaultValue = "0") int pagina,
            @RequestParam(name = "tamanhoPagina", defaultValue = "20") int tamanhoPagina,
            @AuthenticationPrincipal AuthenticatedUser principal) {
        validarAcessoConta(accountId, principal);
        Page<Notification> resultado = notificationRepository
                .findByAccountIdOrderByCreatedAtDesc(accountId, PageRequest.of(pagina, tamanhoPagina));
        return ResponseEntity.ok(PageResponse.from(resultado, NotificationResponse::from));
    }

    private void validarAcessoConta(UUID accountId, AuthenticatedUser principal) {
        if (!principal.accountId().equals(accountId)) {
            throw new AccessDeniedException("A conta " + accountId + " não pertence ao usuário autenticado.");
        }
    }
}

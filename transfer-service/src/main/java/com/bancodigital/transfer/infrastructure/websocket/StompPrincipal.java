package com.bancodigital.transfer.infrastructure.websocket;

import java.security.Principal;
import java.util.UUID;

public record StompPrincipal(UUID accountId) implements Principal {

    @Override
    public String getName() {
        return accountId.toString();
    }
}

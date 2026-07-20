package com.bancodigital.transfer.infrastructure.security;

import java.util.UUID;

public record AuthenticatedUser(UUID userId, UUID accountId) {
}

package com.bancodigital.account.infrastructure.security;

import java.util.UUID;

public record AuthenticatedUser(UUID userId, UUID accountId) {
}

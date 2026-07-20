package com.bancodigital.account.web.dto;

import java.util.UUID;

public record LoginResponse(
        String token,
        UUID accountId,
        long expiresInSeconds
) {
}

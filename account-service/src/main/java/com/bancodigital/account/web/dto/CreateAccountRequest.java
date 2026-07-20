package com.bancodigital.account.web.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

public record CreateAccountRequest(
        @NotBlank String ownerName,
        @NotBlank @Pattern(regexp = "\\d{11}", message = "ownerDocument deve conter 11 dígitos numéricos (CPF)")
        String ownerDocument
) {
}

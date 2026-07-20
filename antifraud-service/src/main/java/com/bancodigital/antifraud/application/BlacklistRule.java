package com.bancodigital.antifraud.application;

import com.bancodigital.antifraud.domain.event.TransferReserved;
import com.bancodigital.antifraud.infrastructure.persistence.BlacklistedAccountRepository;
import java.util.Optional;
import org.springframework.stereotype.Component;

@Component
public class BlacklistRule implements AntiFraudRule {

    private final BlacklistedAccountRepository blacklistedAccountRepository;

    public BlacklistRule(BlacklistedAccountRepository blacklistedAccountRepository) {
        this.blacklistedAccountRepository = blacklistedAccountRepository;
    }

    @Override
    public Optional<String> avaliar(TransferReserved event) {
        if (blacklistedAccountRepository.existsById(event.accountOrigemId())
                || blacklistedAccountRepository.existsById(event.accountDestinoId())) {
            return Optional.of("CONTA_BLOQUEADA");
        }
        return Optional.empty();
    }
}

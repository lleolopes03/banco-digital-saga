package com.bancodigital.antifraud.application;

import com.bancodigital.antifraud.domain.event.TransferReserved;
import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Component;

@Component
public class AntiFraudRuleEngine {

    // Ordem deliberada, da regra mais barata para a mais cara: LimiteValorRule não faz I/O
    // nenhum, VelocityCheckRule só bate no Redis, e BlacklistRule é a única que consulta o
    // Postgres. Qualquer reprovação já corta o fluxo, então colocar as regras baratas primeiro
    // evita pagar o custo das mais caras quando a transferência já seria rejeitada de qualquer
    // forma.
    private final List<AntiFraudRule> rules;

    public AntiFraudRuleEngine(LimiteValorRule limiteValorRule, VelocityCheckRule velocityCheckRule,
                               BlacklistRule blacklistRule) {
        this.rules = List.of(limiteValorRule, velocityCheckRule, blacklistRule);
    }

    public Optional<String> avaliar(TransferReserved event) {
        for (AntiFraudRule rule : rules) {
            Optional<String> motivo = rule.avaliar(event);
            if (motivo.isPresent()) {
                return motivo;
            }
        }
        return Optional.empty();
    }
}

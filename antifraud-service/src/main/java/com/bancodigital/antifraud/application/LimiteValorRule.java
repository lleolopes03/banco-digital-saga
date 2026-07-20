package com.bancodigital.antifraud.application;

import com.bancodigital.antifraud.domain.event.TransferReserved;
import java.math.BigDecimal;
import java.util.Optional;
import org.springframework.stereotype.Component;

@Component
public class LimiteValorRule implements AntiFraudRule {

    private static final BigDecimal LIMITE = new BigDecimal("10000");

    @Override
    public Optional<String> avaliar(TransferReserved event) {
        if (event.valor().compareTo(LIMITE) > 0) {
            return Optional.of("LIMITE_EXCEDIDO");
        }
        return Optional.empty();
    }
}

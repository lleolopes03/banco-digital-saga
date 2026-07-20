package com.bancodigital.antifraud.application;

import com.bancodigital.antifraud.domain.event.TransferReserved;
import java.util.Optional;

public interface AntiFraudRule {

    /**
     * @return vazio se a transferência passa nesta regra, ou o motivo da reprovação.
     */
    Optional<String> avaliar(TransferReserved event);
}

package com.bancodigital.notification.infrastructure.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import com.bancodigital.notification.domain.event.TransferCompleted;
import com.bancodigital.notification.domain.event.TransferFailed;
import org.junit.jupiter.api.Test;

/*
 * O nome simples da classe de cada evento é o valor do header eventType publicado pelo transfer-service: é o
 * contrato entre os dois serviços, e o roteador do consumer usa exatamente esse nome.
 */
class EventTypeContractTest {

    @Test
    void nomeDaClasseDeCadaEventoEOTipoPublicadoNoKafka() {
        assertThat(TransferCompleted.class.getSimpleName()).isEqualTo("TransferCompleted");
        assertThat(TransferFailed.class.getSimpleName()).isEqualTo("TransferFailed");
    }
}

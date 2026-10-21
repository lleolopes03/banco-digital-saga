package com.bancodigital.antifraud.infrastructure.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.bancodigital.antifraud.domain.OutboxEvent;
import com.bancodigital.antifraud.domain.event.AntiFraudApproved;
import com.bancodigital.antifraud.domain.event.AntiFraudRejected;
import com.bancodigital.antifraud.domain.event.TransferReserved;
import com.bancodigital.antifraud.infrastructure.observability.SagaObservability;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/*
 * O nome simples da classe de cada evento é o valor do header eventType no Kafka: é o contrato com os outros
 * serviços. Renomear um desses records sem renomear o dos demais quebraria a saga em silêncio.
 */
class EventTypeContractTest {

    private static final Map<Class<?>, String> NOME_NO_CONTRATO = Map.ofEntries(
            Map.entry(AntiFraudApproved.class, "AntiFraudApproved"),
            Map.entry(AntiFraudRejected.class, "AntiFraudRejected"),
            Map.entry(TransferReserved.class, "TransferReserved")
    );

    private static final List<Class<?>> PRODUZIDOS = List.of(
            AntiFraudApproved.class,
            AntiFraudRejected.class
    );

    @Test
    void nomeDaClasseDeCadaEventoEOTipoPublicadoNoKafka() {
        NOME_NO_CONTRATO.forEach((classe, nome) -> assertThat(classe.getSimpleName()).isEqualTo(nome));
    }

    @Test
    void todoEventoProduzidoTemTopicoConfigurado() {
        OutboxRecordFactory factory = new OutboxRecordFactory(mock(SagaObservability.class));

        for (Class<?> evento : PRODUZIDOS) {
            OutboxEvent outbox = OutboxEvent.pending(UUID.randomUUID(), "Transfer", UUID.randomUUID(),
                    evento.getSimpleName(), "{}", null);

            assertThat(factory.criar(outbox).topic()).as(evento.getSimpleName()).isNotBlank();
        }
    }
}

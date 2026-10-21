package com.bancodigital.transfer.infrastructure.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.bancodigital.transfer.domain.OutboxEvent;
import com.bancodigital.transfer.domain.event.AntiFraudApproved;
import com.bancodigital.transfer.domain.event.AntiFraudRejected;
import com.bancodigital.transfer.domain.event.CreditConfirmed;
import com.bancodigital.transfer.domain.event.CreditFailed;
import com.bancodigital.transfer.domain.event.CreditRequested;
import com.bancodigital.transfer.domain.event.DebitConfirmationRequested;
import com.bancodigital.transfer.domain.event.DebitConfirmed;
import com.bancodigital.transfer.domain.event.DebitReversed;
import com.bancodigital.transfer.domain.event.ReleaseReservedAmount;
import com.bancodigital.transfer.domain.event.ReservationReleased;
import com.bancodigital.transfer.domain.event.ReverseDebitRequested;
import com.bancodigital.transfer.domain.event.TransferCompleted;
import com.bancodigital.transfer.domain.event.TransferFailed;
import com.bancodigital.transfer.domain.event.TransferRequested;
import com.bancodigital.transfer.domain.event.TransferReservationFailed;
import com.bancodigital.transfer.domain.event.TransferReserved;
import com.bancodigital.transfer.infrastructure.observability.SagaObservability;
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
            Map.entry(CreditConfirmed.class, "CreditConfirmed"),
            Map.entry(CreditFailed.class, "CreditFailed"),
            Map.entry(CreditRequested.class, "CreditRequested"),
            Map.entry(DebitConfirmationRequested.class, "DebitConfirmationRequested"),
            Map.entry(DebitConfirmed.class, "DebitConfirmed"),
            Map.entry(DebitReversed.class, "DebitReversed"),
            Map.entry(ReleaseReservedAmount.class, "ReleaseReservedAmount"),
            Map.entry(ReservationReleased.class, "ReservationReleased"),
            Map.entry(ReverseDebitRequested.class, "ReverseDebitRequested"),
            Map.entry(TransferCompleted.class, "TransferCompleted"),
            Map.entry(TransferFailed.class, "TransferFailed"),
            Map.entry(TransferRequested.class, "TransferRequested"),
            Map.entry(TransferReservationFailed.class, "TransferReservationFailed"),
            Map.entry(TransferReserved.class, "TransferReserved")
    );

    private static final List<Class<?>> PRODUZIDOS = List.of(
            TransferRequested.class,
            DebitConfirmationRequested.class,
            CreditRequested.class,
            ReleaseReservedAmount.class,
            ReverseDebitRequested.class,
            TransferCompleted.class,
            TransferFailed.class
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

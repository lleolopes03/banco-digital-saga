package com.bancodigital.antifraud;

import static org.assertj.core.api.Assertions.assertThat;

import com.bancodigital.antifraud.domain.event.AntiFraudApproved;
import com.bancodigital.antifraud.domain.event.AntiFraudRejected;
import com.bancodigital.antifraud.domain.event.TransferReserved;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.kafka.KafkaContainer;

class AntiFraudServiceIT extends AbstractIntegrationTest {

    private static final String TOPIC_ENTRADA = "transfer.reserved";
    private static final String TOPIC_SAIDA = "antifraud.events";
    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaContainer kafkaContainer;

    private void publicarTransferReserved(TransferReserved event) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_ENTRADA, null,
                event.accountOrigemId().toString(), MAPPER.writeValueAsString(event));
        record.headers().add(new RecordHeader("eventType", "TransferReserved".getBytes(StandardCharsets.UTF_8)));
        // eventId é obrigatório desde a Fatia 10 (idempotência): em produção quem adiciona esse
        // header é o OutboxPublisherScheduler do account-service; aqui simulamos com um UUID
        // novo a cada chamada, já que este teste publica direto no tópico sem passar pelo outbox.
        record.headers().add(new RecordHeader("eventId", UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)));
        kafkaTemplate.send(record).get();
    }

    @Test
    void deveAprovarTransferenciaDentroDasRegrasEPublicarAntiFraudApproved() throws Exception {
        UUID transferId = UUID.randomUUID();
        TransferReserved event = new TransferReserved(transferId, UUID.randomUUID(), UUID.randomUUID(),
                new BigDecimal("100.00"), Instant.now());

        publicarTransferReserved(event);

        Optional<ConsumerRecord<String, String>> record = consumirMensagem(TOPIC_SAIDA, transferId.toString());
        assertThat(record).isPresent();

        AntiFraudApproved payload = MAPPER.readValue(record.get().value(), AntiFraudApproved.class);
        assertThat(payload.transferId()).isEqualTo(transferId);
    }

    @Test
    void deveRejeitarTransferenciaAcimaDoLimiteEPublicarAntiFraudRejectedComMotivo() throws Exception {
        UUID transferId = UUID.randomUUID();
        TransferReserved event = new TransferReserved(transferId, UUID.randomUUID(), UUID.randomUUID(),
                new BigDecimal("15000.00"), Instant.now());

        publicarTransferReserved(event);

        Optional<ConsumerRecord<String, String>> record = consumirMensagem(TOPIC_SAIDA, transferId.toString());
        assertThat(record).isPresent();

        AntiFraudRejected payload = MAPPER.readValue(record.get().value(), AntiFraudRejected.class);
        assertThat(payload.transferId()).isEqualTo(transferId);
        assertThat(payload.motivo()).isEqualTo("LIMITE_EXCEDIDO");
    }

    private Optional<ConsumerRecord<String, String>> consumirMensagem(String topico, String chaveEsperada) {
        Map<String, Object> consumerProps = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "antifraud-service-test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        );
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(List.of(topico));
            long deadline = System.currentTimeMillis() + 20000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    if (chaveEsperada.equals(record.key())) {
                        return Optional.of(record);
                    }
                }
            }
            return Optional.empty();
        }
    }
}

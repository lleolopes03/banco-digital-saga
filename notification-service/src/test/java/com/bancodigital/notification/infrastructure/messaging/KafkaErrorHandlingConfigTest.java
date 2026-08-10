package com.bancodigital.notification.infrastructure.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import com.bancodigital.notification.AbstractIntegrationTest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.kafka.KafkaContainer;

/*
 * Prova, com Kafka real (Testcontainers) e o TransferFinishedConsumer de verdade já registrado
 * no contexto da aplicação (mesmo grupo "notification-service" usado em produção), que uma
 * mensagem com payload que falha na deserialização em toda tentativa acaba desviada para
 * "transfer.finished-dlt" em vez de travar a partição para sempre — a prova de que o
 * DefaultErrorHandler configurado em KafkaErrorHandlingConfig está de fato ligado ao container
 * dos @KafkaListener deste serviço, e não só declarado.
 */
class KafkaErrorHandlingConfigTest extends AbstractIntegrationTest {

    private static final String TOPIC = "transfer.finished";
    private static final String DLT_TOPIC = TOPIC + "-dlt";
    private static final String PAYLOAD_INVALIDO = "payload-invalido-para-teste-de-dlq";

    @Autowired
    private KafkaContainer kafkaContainer;

    @Test
    void mensagemComPayloadInvalidoDeveSerEnviadaParaDLQAposEsgotarTentativas() {
        publicarPayloadInvalido();

        Optional<ConsumerRecord<String, String>> record = consumirDaDLT();

        assertThat(record).isPresent();
        assertThat(record.get().value()).isEqualTo(PAYLOAD_INVALIDO);
    }

    private void publicarPayloadInvalido() {
        Map<String, Object> producerProps = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "chave-teste", PAYLOAD_INVALIDO);
            record.headers().add(new RecordHeader("eventType", "TransferCompleted".getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("eventId", UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)));
            kafkaTemplate.send(record);
            kafkaTemplate.flush();
        } finally {
            kafkaTemplate.destroy();
        }
    }

    private Optional<ConsumerRecord<String, String>> consumirDaDLT() {
        Map<String, Object> consumerProps = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "dlq-test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(List.of(DLT_TOPIC));
            long deadline = System.currentTimeMillis() + 20_000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    return Optional.of(record);
                }
            }
            return Optional.empty();
        }
    }
}

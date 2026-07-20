package com.bancodigital.account.infrastructure.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.bancodigital.account.AbstractIntegrationTest;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.micrometer.tracing.test.autoconfigure.AutoConfigureTracing;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.kafka.KafkaContainer;

/*
 * Prova, com Kafka real (Testcontainers), que o header traceparent (W3C) sai do produtor e
 * chega intacto no consumidor — a mesma peça de infraestrutura usada por
 * OutboxPublisherScheduler (que injeta o valor já capturado) e pelos @KafkaListener consumers
 * (que extraem via SagaObservability.executarEtapa) em todos os três serviços da saga.
 *
 * @AutoConfigureTracing é necessário porque o Spring Boot desativa a exportação (e, por tabela,
 * a criação dos beans de propagação W3C) em testes por padrão, para nenhuma suíte tentar falar
 * com um coletor OTLP de verdade sem pedir explicitamente — sem essa anotação,
 * getTextMapPropagator() devolve um propagator no-op e traceparentAtual() sempre retorna null.
 */
@AutoConfigureTracing
class TracingContextPropagationTest extends AbstractIntegrationTest {

    private static final String TOPIC = "tracing-propagation-test";

    @Autowired
    private SagaObservability sagaObservability;

    @Autowired
    private Tracer tracer;

    @Autowired
    private KafkaContainer kafkaContainer;

    @Test
    void traceparentDeveChegarIntactoDoProdutorAoConsumidor() {
        String traceparentEsperado = capturarTraceparentDeUmSpanAtivo();
        assertThat(traceparentEsperado).isNotBlank();

        publicar(traceparentEsperado);

        Optional<ConsumerRecord<String, String>> record = consumir();
        assertThat(record).isPresent();
        assertThat(headerValue(record.get(), "traceparent")).isEqualTo(traceparentEsperado);
    }

    private String capturarTraceparentDeUmSpanAtivo() {
        Span span = tracer.spanBuilder("teste.trace_propagation").startSpan();
        try (Scope scope = span.makeCurrent()) {
            return sagaObservability.traceparentAtual();
        } finally {
            span.end();
        }
    }

    private void publicar(String traceparent) {
        Map<String, Object> producerProps = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "chave-teste", "{}");
            sagaObservability.injetarHeader(record, traceparent);
            kafkaTemplate.send(record);
            kafkaTemplate.flush();
        } finally {
            kafkaTemplate.destroy();
        }
    }

    private Optional<ConsumerRecord<String, String>> consumir() {
        Map<String, Object> consumerProps = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "tracing-propagation-test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(List.of(TOPIC));
            long deadline = System.currentTimeMillis() + 15000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    return Optional.of(record);
                }
            }
            return Optional.empty();
        }
    }

    private String headerValue(ConsumerRecord<String, String> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}

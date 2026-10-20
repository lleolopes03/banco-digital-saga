package com.bancodigital.account.infrastructure.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import com.bancodigital.account.AbstractIntegrationTest;
import com.bancodigital.account.domain.OutboxEvent;
import com.bancodigital.account.domain.OutboxEventStatus;
import com.bancodigital.account.infrastructure.observability.SagaObservability;
import com.bancodigital.account.infrastructure.persistence.OutboxEventRepository;
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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.kafka.KafkaContainer;

/*
 * @Transactional aqui não é só conveniência de limpeza: o AccountApplication tem
 * @EnableScheduling, então o OutboxPublisherScheduler real (com o KafkaTemplate real) já está
 * rodando em background a cada 500ms durante toda a suíte. Sem isolar os dados desta classe em
 * uma transação de teste (revertida ao final, nunca commitada), esse scheduler de background
 * competiria pelas mesmas linhas PENDING e publicaria o evento do teste de falha antes da
 * asserção rodar.
 */
@Transactional
class OutboxPublisherSchedulerTest extends AbstractIntegrationTest {

    private static final String TOPIC = "account.events";

    @Autowired
    private OutboxPublisherScheduler scheduler;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private KafkaContainer kafkaContainer;

    @Autowired
    private SagaObservability sagaObservability;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Test
    void devePublicarEventoPendenteEMarcarComoPublished() {
        UUID accountId = UUID.randomUUID();
        String payload = "{\"accountId\":\"" + accountId + "\",\"amount\":100.00}";
        OutboxEvent event = OutboxEvent.pending(UUID.randomUUID(), "Account", accountId, "DepositCompleted", payload, null);
        outboxEventRepository.saveAndFlush(event);

        scheduler.publishPendingEvents();

        OutboxEvent reloaded = outboxEventRepository.findById(event.getId()).orElseThrow();
        assertThat(reloaded.getStatus()).isEqualTo(OutboxEventStatus.PUBLISHED);
        assertThat(reloaded.getPublishedAt()).isNotNull();

        // Comparamos com reloaded.getPayload() (o texto já normalizado pelo Postgres ao
        // gravar como jsonb), não com a string original: jsonb reformata espaços/ordem de
        // chaves ao persistir, então o valor publicado no Kafka é o que o scheduler releu do
        // banco, não o literal que inserimos.
        Optional<ConsumerRecord<String, String>> record = consumirMensagem(accountId.toString());
        assertThat(record).isPresent();
        assertThat(record.get().value()).isEqualTo(reloaded.getPayload());
    }

    @Test
    void falhaSimuladaNaPublicacaoMantemEventoPendenteParaRetry() {
        UUID accountId = UUID.randomUUID();
        String payload = "{\"accountId\":\"" + accountId + "\",\"amount\":50.00}";
        OutboxEvent event = OutboxEvent.pending(UUID.randomUUID(), "Account", accountId, "DepositCompleted", payload, null);
        outboxEventRepository.saveAndFlush(event);

        KafkaTemplate<String, String> kafkaTemplateQuebrado = criarKafkaTemplateApontandoParaBrokerInexistente();
        OutboxPublisherScheduler schedulerComFalha = new OutboxPublisherScheduler(outboxEventRepository, kafkaTemplateQuebrado,
                new OutboxRecordFactory(sagaObservability), transactionTemplate, 100);

        schedulerComFalha.publishPendingEvents();

        OutboxEvent reloaded = outboxEventRepository.findById(event.getId()).orElseThrow();
        assertThat(reloaded.getStatus()).isEqualTo(OutboxEventStatus.PENDING);
        assertThat(reloaded.getPublishedAt()).isNull();
    }

    @Test
    void deveDrenarVariosLotesNaMesmaExecucaoEmVezDeUmLotePorIntervalo() {
        // lote de 2 e 5 eventos: sem o dreno em loop seriam necessárias 3 execuções do scheduler
        OutboxPublisherScheduler schedulerLoteMinimo = new OutboxPublisherScheduler(outboxEventRepository, kafkaTemplate,
                new OutboxRecordFactory(sagaObservability), transactionTemplate, 2);
        List<UUID> ids = new java.util.ArrayList<>();
        for (int i = 0; i < 5; i++) {
            UUID accountId = UUID.randomUUID();
            String payload = "{\"accountId\":\"" + accountId + "\",\"amount\":" + (10 + i) + ".00}";
            OutboxEvent event = OutboxEvent.pending(UUID.randomUUID(), "Account", accountId, "DepositCompleted", payload, null);
            ids.add(outboxEventRepository.saveAndFlush(event).getId());
        }

        schedulerLoteMinimo.publishPendingEvents();

        for (UUID id : ids) {
            assertThat(outboxEventRepository.findById(id).orElseThrow().getStatus())
                    .isEqualTo(OutboxEventStatus.PUBLISHED);
        }
    }

    private KafkaTemplate<String, String> criarKafkaTemplateApontandoParaBrokerInexistente() {
        Map<String, Object> configs = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1",
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 2000,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 2000,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 3000,
                ProducerConfig.RETRIES_CONFIG, 0
        );
        DefaultKafkaProducerFactory<String, String> producerFactory =
                new DefaultKafkaProducerFactory<>(configs, new StringSerializer(), new StringSerializer());
        return new KafkaTemplate<>(producerFactory);
    }

    private Optional<ConsumerRecord<String, String>> consumirMensagem(String chaveEsperada) {
        Map<String, Object> consumerProps = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "outbox-scheduler-test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        );
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(List.of(TOPIC));
            long deadline = System.currentTimeMillis() + 15000;
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

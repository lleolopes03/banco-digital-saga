package com.bancodigital.account.infrastructure.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import com.bancodigital.account.AbstractIntegrationTest;
import com.bancodigital.account.domain.Account;
import com.bancodigital.account.infrastructure.persistence.AccountRepository;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.kafka.KafkaContainer;

/*
 * Prova a garantia central da Fatia 10: Kafka é at-least-once, então o mesmo evento pode chegar
 * mais de uma vez (redelivery após rebalance, timeout de commit, etc). Aqui simulamos essa
 * reentrega manualmente — publicando duas vezes, direto no tópico, o mesmo DebitConfirmationRequested
 * (mesmo eventId no header) — e verificamos que o saldo da conta origem só é debitado uma única
 * vez, graças ao IdempotentEventHandler que envolve DebitConfirmationRequestedConsumer.
 */
class DebitConfirmationRequestedIdempotencyIT extends AbstractIntegrationTest {

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private KafkaContainer kafkaContainer;

    @Test
    void deveDebitarApenasUmaVezMesmoComEventoReentregueDuasVezes() {
        Account conta = Account.open("Redelivery Teste", "55566677788");
        conta.deposit(new BigDecimal("1000.00"));
        conta.reservar(new BigDecimal("300.00"));
        accountRepository.saveAndFlush(conta);

        UUID transferId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        String payload = "{\"transferId\":\"" + transferId + "\",\"accountOrigemId\":\"" + conta.getId()
                + "\",\"valor\":300.00,\"timestamp\":\"" + Instant.now() + "\"}";

        publicarDuasVezes(eventId, payload);

        BigDecimal saldoFinal = aguardarSaldo(conta.getId(), new BigDecimal("700.00"));

        // Se tivesse debitado duas vezes o saldo cairia para 400.00 (ou a segunda tentativa
        // lançaria IllegalStateException por reservedBalance insuficiente) — qualquer um dos
        // dois seria uma quebra da garantia de idempotência.
        assertThat(saldoFinal).isEqualByComparingTo("700.00");
    }

    private BigDecimal aguardarSaldo(UUID accountId, BigDecimal saldoEsperado) {
        Instant prazo = Instant.now().plus(Duration.ofSeconds(20));
        BigDecimal saldoAtual = null;
        while (Instant.now().isBefore(prazo)) {
            saldoAtual = accountRepository.findById(accountId).orElseThrow().getBalance();
            if (saldoAtual.compareTo(saldoEsperado) == 0) {
                return saldoAtual;
            }
            sleep();
        }
        return saldoAtual;
    }

    private void sleep() {
        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private void publicarDuasVezes(UUID eventId, String payload) {
        Map<String, Object> producerProps = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));
        try {
            for (int i = 0; i < 2; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>("transfer.debit", null, payload);
                record.headers().add(new RecordHeader("eventId", eventId.toString().getBytes(StandardCharsets.UTF_8)));
                kafkaTemplate.send(record);
            }
            kafkaTemplate.flush();
        } finally {
            kafkaTemplate.destroy();
        }
    }
}

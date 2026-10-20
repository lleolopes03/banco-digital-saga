package com.bancodigital.transfer.infrastructure.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Impede que uma mensagem com falha permanente trave a partição: depois de 1 tentativa + 2 retries
 * (1 s entre elas, bem abaixo do timeout de 2 min da saga) ela vai para o tópico "{@code <original>-dlt}".
 * O Spring Boot aplica este handler a todos os {@code @KafkaListener} do serviço.
 */
@Configuration(proxyBeanMethods = false)
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        // Partição -1: o producer escolhe; a de origem pode não existir no -dlt.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> new TopicPartition(record.topic() + "-dlt", -1));
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(1_000L, 2));
        errorHandler.setRetryListeners(new LoggingRetryListener());
        return errorHandler;
    }

    private static class LoggingRetryListener implements RetryListener {

        @Override
        public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
            log.warn("Falha ao processar mensagem do tópico {} (tentativa {}): {}", record.topic(), deliveryAttempt,
                    ex.getMessage());
        }

        @Override
        public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
            log.error("Mensagem do tópico {} enviada para a DLQ após esgotar as tentativas: {}", record.topic(),
                    ex.getMessage());
        }

        @Override
        public void recoveryFailed(ConsumerRecord<?, ?> record, Exception original, Exception failure) {
            log.error("Falha ao publicar mensagem do tópico {} na DLQ", record.topic(), failure);
        }
    }
}

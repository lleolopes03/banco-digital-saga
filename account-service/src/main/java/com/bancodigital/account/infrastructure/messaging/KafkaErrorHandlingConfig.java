package com.bancodigital.account.infrastructure.messaging;

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

/*
 * Sem isso, uma mensagem que falha de forma permanente (payload corrompido, ou uma regra de
 * negócio que nunca vai passar) ficaria retentando pra sempre na mesma posição da partição —
 * Kafka processa em ordem dentro de uma partição, então nenhuma mensagem publicada depois dela
 * no mesmo tópico seria processada enquanto isso não se resolvesse. Isso é especialmente
 * sensível aqui: os consumers deste serviço (TransferRequestedConsumer, CreditRequestedConsumer,
 * etc.) são quem efetivamente move dinheiro entre contas — uma partição travada bloqueia o
 * núcleo da saga inteira, não só uma notificação. O DeadLetterPublishingRecoverer desvia a
 * mensagem, após esgotar as tentativas, para o tópico "<topico-original>-dlt" (sufixo padrão
 * desta versão do Spring Kafka), preservando o motivo da falha nos headers, e o consumer segue
 * processando as próximas normalmente.
 *
 * Mesma configuração usada em transfer-service e antifraud-service — duplicada aqui porque cada
 * serviço é um deploy independente sem biblioteca compartilhada (mesma convenção já usada para
 * OutboxEvent/ProcessedEvent/SagaObservability).
 *
 * O Spring Boot injeta automaticamente qualquer bean CommonErrorHandler único no
 * ConcurrentKafkaListenerContainerFactory autoconfigurado — aplica-se a todos os @KafkaListener
 * deste serviço, sem precisar configurar cada consumer individualmente.
 */
@Configuration(proxyBeanMethods = false)
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        // Partição -1: quem escolhe é o producer (pela chave). O padrão do recoverer reusa o número da
        // partição de origem, que não existe no -dlt (criado sob demanda com 1 partição) quando o
        // tópico original tem várias.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> new TopicPartition(record.topic() + "-dlt", -1));
        // 1 tentativa inicial + 2 retries, 1s entre elas: rápido o suficiente para não competir
        // com o timeout de 2 minutos do SagaTimeoutScheduler (transfer-service), que é a rede de
        // segurança maior para sagas travadas.
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

package com.bancodigital.account.infrastructure.messaging;

import java.util.Arrays;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * Declara os tópicos que o serviço produz ou consome, com várias partições: a chave é o transferId, então os
 * eventos de uma saga ficam em ordem na mesma partição e sagas diferentes rodam em paralelo.
 * Declarar também os consumidos garante que quem sobe primeiro crie o tópico certo antes de iniciar seus
 * listeners; um tópico criado sob demanda com 1 partição e ampliado depois deixaria mensagens sem
 * consumidor até o refresh de metadata.
 */
@Configuration(proxyBeanMethods = false)
public class KafkaTopicsConfig {

    @Bean
    KafkaAdmin.NewTopics topicosDaSaga(@Value("${app.kafka.partitions:3}") int particoes,
                                       @Value("${app.kafka.replicas:1}") int replicas) {
        NewTopic[] topicos = Arrays.stream(new String[] {"account.events", "transfer.reserved", "transfer.debited", "transfer.credited", "transfer.compensated",
                "transfer.requested", "transfer.debit", "transfer.credit", "transfer.compensate"})
                .map((nome) -> TopicBuilder.name(nome).partitions(particoes).replicas(replicas).build())
                .toArray(NewTopic[]::new);
        return new KafkaAdmin.NewTopics(topicos);
    }
}

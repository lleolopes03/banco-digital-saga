package com.bancodigital.antifraud.infrastructure.messaging;

import java.util.Arrays;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/*
 * Declara os tópicos que este serviço PRODUZ ou CONSOME, com mais de uma partição. Sem isto o
 * broker cria cada tópico sob demanda com 1 partição, e um consumer com 1 partição processa uma
 * mensagem de cada vez — foi o que formou fila no primeiro salto da saga sob carga (ver
 * load-tests/README.md).
 *
 * Por que declarar também os tópicos consumidos: o KafkaAdmin roda antes de os listeners deste
 * serviço iniciarem, então quem sobe primeiro cria o tópico já com o número certo de partições.
 * Se só o produtor declarasse, um consumer que assina antes disso faria o broker criar o tópico
 * com 1 partição; o aumento posterior só seria percebido pelos consumers no refresh de metadata,
 * e as mensagens das partições novas ficariam sem consumidor (as sagas parariam em STARTED).
 *
 * A chave de cada mensagem é o transferId (aggregateId do Outbox), então todos os eventos de uma
 * saga caem na mesma partição e continuam em ordem; sagas diferentes passam a ser processadas em
 * paralelo (spring.kafka.listener.concurrency). O KafkaAdmin cria o tópico se não existe e aumenta
 * as partições se já existe com menos (partições nunca diminuem). Contagem e réplicas são
 * configuráveis; réplicas = 1 só serve para broker único (dev).
 */
@Configuration(proxyBeanMethods = false)
public class KafkaTopicsConfig {

    @Bean
    KafkaAdmin.NewTopics topicosDaSaga(@Value("${app.kafka.partitions:3}") int particoes,
                                       @Value("${app.kafka.replicas:1}") int replicas) {
        NewTopic[] topicos = Arrays.stream(new String[] {"antifraud.events", "transfer.reserved"})
                .map((nome) -> TopicBuilder.name(nome).partitions(particoes).replicas(replicas).build())
                .toArray(NewTopic[]::new);
        return new KafkaAdmin.NewTopics(topicos);
    }
}

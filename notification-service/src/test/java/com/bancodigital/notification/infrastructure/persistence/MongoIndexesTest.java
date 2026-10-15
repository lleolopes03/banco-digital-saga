package com.bancodigital.notification.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.bancodigital.notification.AbstractIntegrationTest;
import com.bancodigital.notification.domain.Notification;
import com.bancodigital.notification.domain.ProcessedEvent;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.IndexInfo;

/*
 * Garante que os indices declarados por anotacao EXISTEM de fato no MongoDB. Desde o Spring Data
 * MongoDB 3.0 a criacao automatica vem desligada (spring.data.mongodb.auto-index-creation): sem
 * ela, o @CompoundIndex ficava so decorativo e a consulta do historico fazia varredura da collection.
 * Um teste de repositorio comum nao pega isso — a query funciona, so fica lenta.
 */
class MongoIndexesTest extends AbstractIntegrationTest {

    @Autowired
    private MongoTemplate mongoTemplate;

    @Test
    void indiceCompostoDeNotificacoesPorContaEDataDeveExistir() {
        List<IndexInfo> indices = mongoTemplate.indexOps(Notification.class).getIndexInfo();

        assertThat(indices).extracting(IndexInfo::getName).contains("accountId_createdAt");
    }

    @Test
    void processedEventsDeveTerIndiceTtlDe14Dias() {
        List<IndexInfo> indices = mongoTemplate.indexOps(ProcessedEvent.class).getIndexInfo();

        IndexInfo ttl = indices.stream().filter((i) -> i.getExpireAfter().isPresent()).findFirst().orElse(null);

        assertThat(ttl).as("indice TTL em processed_events").isNotNull();
        assertThat(ttl.getExpireAfter()).contains(Duration.ofDays(14));
    }
}

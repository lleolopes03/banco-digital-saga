package com.bancodigital.account.infrastructure.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import com.bancodigital.account.AbstractIntegrationTest;
import com.bancodigital.account.domain.OutboxEvent;
import com.bancodigital.account.infrastructure.persistence.OutboxEventRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/*
 * Prova a seleção do poller do Outbox quando há mais de uma instância do serviço: exclusão mútua
 * (FOR UPDATE SKIP LOCKED) e ordem por agregado (um evento por agregado, o mais antigo).
 *
 * Não é @Transactional: precisa de transações reais e concorrentes, com linhas commitadas.
 *
 * Roda no contexto compartilhado da suíte (um contexto novo subiria mais containers do Testcontainers).
 * Por isso o poller de background deste e de outros contextos em cache também reivindica linhas
 * PENDING (e as trava por alguns ms; o eventType "TesteClaim" não tem tópico, então ele nunca
 * publica nada). As sondagens são com rollback, as propriedades de segurança (nunca devolver o evento
 * errado) são checadas em TODA sondagem, e o resultado esperado é aguardado "eventualmente".
 */
class OutboxClaimConcurrencyIT extends AbstractIntegrationTest {

    @Autowired
    private OutboxEventRepository repository;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private JdbcTemplate jdbc;

    private final List<UUID> agregadosCriados = new ArrayList<>();

    @AfterEach
    void limpar() {
        for (UUID agregado : agregadosCriados) {
            jdbc.update("DELETE FROM outbox_events WHERE aggregate_id = ?", agregado);
        }
    }

    private UUID criar(UUID agregado, Duration idade) {
        agregadosCriados.add(agregado);
        OutboxEvent e = OutboxEvent.pending(UUID.randomUUID(), "Transfer", agregado, "TesteClaim", "{}", null);
        UUID id = repository.saveAndFlush(e).getId();
        jdbc.update("UPDATE outbox_events SET created_at = ? WHERE id = ?",
                java.sql.Timestamp.from(Instant.now().minus(idade)), id);
        return id;
    }

    @Test
    void duasInstanciasNaoReivindicamOMesmoEvento() throws Exception {
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ids.add(criar(UUID.randomUUID(), Duration.ofSeconds(100 - i)));
        }

        // O poller de background do próprio contexto também disputa essas linhas e pode estar com
        // elas travadas no instante da tentativa; por isso repete até as duas "instâncias"
        // conseguirem linhas. A disjunção, que é a propriedade em teste, vale em TODA tentativa.
        boolean umaTentativaComAmbas = false;
        for (int tentativa = 0; tentativa < 40 && !umaTentativaComAmbas; tentativa++) {
            CountDownLatch instanciaAReivindicou = new CountDownLatch(1);
            CountDownLatch instanciaBTerminou = new CountDownLatch(1);

            // instância A reivindica e segura a transação aberta (locks mantidos) até B terminar
            CompletableFuture<List<UUID>> a = CompletableFuture.supplyAsync(() -> transactionTemplate.execute((status) -> {
                List<UUID> reivindicados = idsDe(repository.reivindicarPendentes(6), ids);
                instanciaAReivindicou.countDown();
                await(instanciaBTerminou);
                status.setRollbackOnly();
                return reivindicados;
            }));
            assertThat(instanciaAReivindicou.await(10, TimeUnit.SECONDS)).isTrue();

            // instância B roda enquanto A segura os locks: com SKIP LOCKED não bloqueia nem repete
            List<UUID> deB = transactionTemplate.execute((status) -> {
                status.setRollbackOnly();
                return idsDe(repository.reivindicarPendentes(10), ids);
            });
            instanciaBTerminou.countDown();
            List<UUID> deA = a.get(10, TimeUnit.SECONDS);

            assertThat(deA).hasSizeLessThanOrEqualTo(6);
            assertThat(java.util.Collections.disjoint(deA, deB)).as("A=" + deA + " B=" + deB).isTrue();
            umaTentativaComAmbas = !deA.isEmpty() && !deB.isEmpty();
            if (!umaTentativaComAmbas) {
                Thread.sleep(50);
            }
        }
        assertThat(umaTentativaComAmbas).as("alguma tentativa com as duas instâncias obtendo linhas").isTrue();
    }
    @Test
    void soReivindicaOEventoMaisAntigoDeCadaAgregadoEMantemOrdemAteSerPublicado() throws Exception {
        UUID agregadoX = UUID.randomUUID();
        UUID x1 = criar(agregadoX, Duration.ofSeconds(30));
        UUID x2 = criar(agregadoX, Duration.ofSeconds(20));
        UUID x3 = criar(agregadoX, Duration.ofSeconds(10));
        UUID y1 = criar(UUID.randomUUID(), Duration.ofSeconds(25));
        List<UUID> meus = List.of(x1, x2, x3, y1);

        // x2 e x3 jamais podem sair enquanto x1 está PENDING; y1 (outro agregado) não é bloqueado
        aguardarSondagem(meus, (r) -> r.contains(x1) && r.contains(y1), Set.of(x2, x3));
        assertThat(sondar(meus)).doesNotContain(x2, x3);

        // x1 publicado e commitado -> agora x2 é o mais antigo PENDING do agregado
        transactionTemplate.executeWithoutResult((status) -> repository.findById(x1).orElseThrow().markPublished());
        aguardarSondagem(meus, (r) -> r.contains(x2) && r.contains(y1), Set.of(x3));
    }

    @Test
    void eventoQueNaoFoiPublicadoContinuaBloqueandoOsPosterioresDoMesmoAgregado() throws Exception {
        UUID agregado = UUID.randomUUID();
        UUID e1 = criar(agregado, Duration.ofSeconds(20));
        UUID e2 = criar(agregado, Duration.ofSeconds(10));
        List<UUID> meus = List.of(e1, e2);

        // simula falha de envio: reivindica e NÃO marca como publicado (rollback); e2 nunca pode passar e1
        aguardarSondagem(meus, (r) -> r.contains(e1), Set.of(e2));
    }

    /** Repete a sondagem até o predicado valer; em TODA sondagem, nenhum id proibido pode aparecer. */
    private void aguardarSondagem(List<UUID> meus, Predicate<List<UUID>> esperado, Set<UUID> proibidos) throws Exception {
        long limite = System.currentTimeMillis() + 15_000;
        List<UUID> ultimo = List.of();
        while (System.currentTimeMillis() < limite) {
            ultimo = sondar(meus);
            assertThat(ultimo).doesNotContainAnyElementsOf(proibidos);
            if (esperado.test(ultimo)) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("Resultado esperado não apareceu; última sondagem: " + ultimo);
    }

    private List<UUID> sondar(List<UUID> meus) {
        return transactionTemplate.execute((status) -> {
            status.setRollbackOnly();
            return idsDe(repository.reivindicarPendentes(50), meus);
        });
    }

    private static List<UUID> idsDe(List<OutboxEvent> eventos, List<UUID> apenasEstes) {
        return eventos.stream().map(OutboxEvent::getId).filter(apenasEstes::contains).toList();
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
}

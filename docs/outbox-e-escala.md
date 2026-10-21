# Outbox, partições e retenção de dados

[← Voltar ao README](../README.md)

## Publicação do Outbox, partições e escala horizontal

O poller de cada serviço (`OutboxPublisherScheduler`) foi o primeiro gargalo medido em carga:
com intervalo fixo de 500 ms e um `send().get()` por evento, ~95% do tempo de uma saga era
espera de publicação (ver [`load-tests/`](../load-tests/README.md)). O desenho atual:

| Decisão | Por quê |
|---|---|
| Intervalo de 100 ms (`outbox.poll-interval`) e lote de 100 (`outbox.batch-size`) | Corta a espera média por salto de ~250 ms para ~50 ms; a query é indexada e barata. |
| Envio em pipeline: dispara todos os `send()` do lote e depois aguarda as confirmações | Um round-trip ao broker por lote, não por evento. Quem falha continua `PENDING`. |
| Dreno em loop: se o lote vier cheio, lê o próximo na hora (até 20 lotes por execução) | O intervalo vira só o piso de latência com a fila vazia, não o teto de vazão. |
| Cada lote numa transação curta (`TransactionTemplate`) | Não segura locks nem uma conexão durante o poll inteiro. |
| `FOR UPDATE SKIP LOCKED` + só o evento mais antigo de cada agregado (`reivindicarPendentes`) | **Várias instâncias** do serviço podem rodar o poller sem publicar o mesmo evento nem inverter a ordem de uma saga; se um envio falha, ele continua `PENDING` e bloqueia os seguintes **da mesma saga** (as outras seguem). |

**Resultado medido** (rampa de 1 a 12 tx/s, 2.550 sagas, uma máquina de desenvolvimento): a
latência mediana da saga a 12 tx/s caiu de **19,7 s para 0,9 s** e o p95 de 26 s para 3,4 s, sem
nenhum erro nem saga pendente. A tabela completa, a decomposição do tempo e uma cauda de ~3% cuja
causa ainda não foi identificada estão em [`load-tests/README.md`](../load-tests/README.md).

O comportamento é *at-least-once* (um crash entre o envio e o commit reenvia o evento); os
consumers já são idempotentes (Redis + `processed_events`). `OutboxClaimConcurrencyIT`
(nos três serviços) prova a exclusão mútua entre duas transações concorrentes e a ordem por
agregado, inclusive depois de uma falha simulada.

**Partições e consumers em paralelo.** Os tópicos são declarados por quem os produz
(`KafkaTopicsConfig`) com 3 partições (`app.kafka.partitions`, `app.kafka.replicas`), e cada
`@KafkaListener` roda com 3 threads (`spring.kafka.listener.concurrency`, variável
`KAFKA_LISTENER_CONCURRENCY`). A chave de cada mensagem é o `transferId`, então todos os eventos
de uma saga caem na mesma partição e seguem em ordem, enquanto sagas diferentes são processadas em
paralelo. A concorrência sobre a mesma conta continua protegida por lock otimista (`@Version`) com
retry. O pool de conexões subiu para 20 (`DB_POOL_SIZE`) para acomodar as threads extras. A DLQ
deixa o *producer* escolher a partição do `-dlt` (o padrão reusaria o número da partição de origem,
que não existe no tópico de DLQ criado sob demanda com 1 partição).

Limites conhecidos: o poller é seguro para várias instâncias, mas isso foi provado em teste de
integração, não em carga com várias JVMs; e o poller continua sendo polling — a próxima etapa
natural, se a latência por salto importar mais que a simplicidade, é CDC (Debezium) ou acordar o
poller logo após o commit.

## Retenção de dados (Outbox e idempotência)

`outbox_events` e `processed_events` só crescem: cada saga deixa linhas para sempre. Cada
serviço com Outbox (`account`, `transfer`, `antifraud`) tem um `RetentionCleanupScheduler` que
apaga em lotes de 1.000, cada lote numa transação curta:

| Tabela | O que apaga | Padrão | Propriedade |
|---|---|---|---|
| `outbox_events` | só `PUBLISHED` mais antigos que a janela; `PENDING` nunca é tocado | 7 dias | `retention.outbox` |
| `processed_events` | registros mais antigos que a janela | 14 dias | `retention.processed-events` |

A janela de `processed_events` precisa cobrir o tempo em que o Kafka ainda pode reentregar uma
mensagem (retenção padrão do tópico: 7 dias), porque é a fonte de verdade da idempotência quando
a chave do Redis (TTL de 24h) expira. Daí os 14 dias, com folga. Execução a cada hora
(`retention.cleanup-interval`), com atraso inicial de 5 minutos (`retention.initial-delay`).
Índices parciais em `published_at` e `processed_at` (migrations Flyway) sustentam os `DELETE`s.
No `notification-service` (MongoDB) o mesmo papel é feito por um índice TTL de 14 dias em
`processed_events`.

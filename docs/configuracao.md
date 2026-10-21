# Configuração

[← Voltar ao README](../README.md)

Toda configuração tem um padrão que funciona em desenvolvimento local; em outros ambientes, sobrescreva por
variável de ambiente (ou pela propriedade equivalente do Spring). O `.env.example` na raiz lista as variáveis de
conexão.

## Conexões (variáveis de ambiente)

| Variável | Padrão | Usada por | Descrição |
|---|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | todos | Endereço do Kafka. |
| `ACCOUNT_DB_HOST` / `_PORT` / `_NAME` / `_USER` / `_PASSWORD` | `localhost` / `5442` / `account_db` / `account` / `account` | account | PostgreSQL do `account-service`. |
| `TRANSFER_DB_*` | `localhost` / `5443` / `transfer_db` / `transfer` / `transfer` | transfer | PostgreSQL do `transfer-service`. |
| `ANTIFRAUD_DB_*` | `localhost` / `5434` / `antifraud_db` / `antifraud` / `antifraud` | antifraud | PostgreSQL do `antifraud-service`. |
| `MONGO_NOTIFICATION_HOST` / `_PORT` / `_DB` | `localhost` / `27018` / `notification_db` | notification | MongoDB do `notification-service`. |
| `REDIS_HOST` / `REDIS_PORT` | `localhost` / `6379` | todos | Cache, idempotência, rate limit e blacklist do JWT. |
| `JWT_SECRET` | segredo de desenvolvimento | account, transfer, notification | Chave HS256 do JWT. **Troque em qualquer ambiente real.** O padrão existe só para o desenvolvimento local. |
| `JWT_EXPIRATION_MINUTES` | `15` | account | Validade do token emitido no login. |
| `ACCOUNT_SERVICE_BASE_URL` | `http://localhost:8081` | transfer | Endereço do `account-service` para a pré-checagem síncrona de saldo. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318/v1/traces` | todos | Destino dos traces (otel-collector). |

## Desempenho e escala

| Variável / propriedade | Padrão | Descrição |
|---|---|---|
| `DB_POOL_SIZE` | `20` | Tamanho do pool de conexões (Hikari) dos serviços com PostgreSQL. |
| `KAFKA_LISTENER_CONCURRENCY` | `3` | Threads por `@KafkaListener`. Só ajuda se o tópico tiver partições suficientes. |
| `app.kafka.partitions` | `3` | Partições dos tópicos da saga, criados na subida. |
| `app.kafka.replicas` | `1` | Fator de replicação dos tópicos. `1` só serve para um broker único (desenvolvimento). |
| `outbox.poll-interval` | `PT0.1S` | Intervalo do poller do Outbox quando a fila está vazia. |
| `outbox.batch-size` | `100` | Eventos por lote do poller. |
| `TRACING_SAMPLING_PROBABILITY` | `1.0` | Fração dos traces amostrados (`0.1` = 10%). No perfil `dev`; em produção o padrão do Spring é 10%. |

## Retenção de dados

| Propriedade | Padrão | Descrição |
|---|---|---|
| `retention.outbox` | `P7D` | Eventos do Outbox **já publicados** mais antigos que isto são apagados. Eventos pendentes nunca são. |
| `retention.processed-events` | `P14D` | Registros de idempotência mais antigos que isto são apagados. Deve ficar acima da retenção do Kafka. |
| `retention.cleanup-interval` | `PT1H` | Intervalo entre execuções da limpeza. |
| `retention.initial-delay` | `PT5M` | Espera antes da primeira execução. |

## Limites e resiliência (transfer-service)

| Propriedade | Padrão | Descrição |
|---|---|---|
| `rate-limit.transfer.max-requests` | `10` | Transferências por conta de origem na janela. Acima disso, `429` com `Retry-After`. |
| `rate-limit.transfer.window` | `60s` | Janela do rate limit. |
| `account-service.connect-timeout` / `read-timeout` | `2s` / `3s` | Timeouts da chamada síncrona de saldo. |
| `resilience4j.circuitbreaker.instances.accountService.*` | janela 10, mínimo 10 chamadas, 50% de falha, 10 s aberto, 3 chamadas em meia-abertura | Circuit breaker da pré-checagem de saldo. |

A saga tem um timeout de 2 minutos para sagas travadas (constante no `SagaTimeoutScheduler`).

## Variáveis reservadas

`ANTHROPIC_API_KEY`, `ZAPI_INSTANCE_ID` e `ZAPI_TOKEN` estão no `.env.example`, mas **nenhum código as usa hoje**: são
reservadas para um envio real de notificações (e-mail/WhatsApp) que ainda não existe.

## Portas locais

| Serviço | Porta |
|---|---|
| account / transfer / antifraud / notification | `8081` / `8082` / `8083` / `8084` |
| Frontend (Angular) | `4200` |
| Kafka | `9092` |
| PostgreSQL (account / transfer / antifraud) | `5442` / `5443` / `5434` |
| MongoDB | `27018` |
| Redis | `6379` |
| Jaeger / Prometheus / Grafana | `16686` / `9090` / `3000` |

As portas dos bancos fogem do padrão (5432, 5433, 27017) para não colidir com instalações nativas na máquina de
desenvolvimento. Ajuste em `docker-compose.yml`, `.env.example` e nos `application-dev.yml` se preferir.

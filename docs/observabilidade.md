# Tracing distribuído da saga (OpenTelemetry)

[← Voltar ao README](../README.md)

Uma transferência atravessa `account-service` → Kafka → `transfer-service` → Kafka →
`antifraud-service` → Kafka → `transfer-service` → Kafka → `account-service` → Kafka →
`transfer-service`, seis vezes cruzando a fronteira assíncrona do Kafka. Sem propagação
explícita de contexto, cada hop viraria um trace novo e desconectado no Jaeger — o objetivo
é que a saga inteira apareça como **um único trace**, do `POST /api/transfers` até a
etapa final (conclusão ou compensação).

## Arquitetura

```
account-service ────┐
transfer-service ────┼── OTLP (HTTP :4318) ──► otel-collector ──► Jaeger (:16686)
antifraud-service ───┤                              │
notification-service ┘                              ▼
                                        (só traces — métricas não passam
                                         pelo collector, ver abaixo)

account-service ────┐
transfer-service ────┼── scrape /actuator/prometheus ──► Prometheus (:9090) ──► Grafana (:3000)
antifraud-service ───┤
notification-service ┘
```

O `otel-collector` (`otel/opentelemetry-collector-contrib`, config em
`observability/otel-collector-config.yaml`) existe como uma camada extra entre os serviços e o
Jaeger — recebe OTLP de todos os quatro e repassa em lote, em vez de cada serviço falar direto
com o Jaeger. Métricas usam um caminho totalmente separado (scrape do Prometheus em
`/actuator/prometheus`, não push OTLP) — mais simples de depurar e é o modelo padrão do
ecossistema Prometheus/Grafana.

**Armazenamento e amostragem**: o Jaeger sobe com armazenamento em disco (Badger, volume
`jaeger_data`) e retenção de 72h, não mais em memória sem limite — medido em carga, cada trace
ocupava ~120 KB de RAM e o container passava de 2 GB depois de ~15 mil sagas. O container tem
`mem_limit: 1g` e o `otel-collector` tem `memory_limiter`, então um backend lento faz o coletor
recusar spans em vez de crescer até cair. Em desenvolvimento 100% dos traces são amostrados;
`TRACING_SAMPLING_PROBABILITY` (ex.: `0.1`) reduz isso sem mexer no código. Em produção o ideal é
um backend gerenciado (Elasticsearch/OpenSearch) e amostragem na borda.

> **Pegadinha do Spring Boot 4.1**: a propriedade `management.otlp.tracing.endpoint` (válida até
> a linha 3.x) foi renomeada para `management.opentelemetry.tracing.export.otlp.endpoint`. Usar o
> nome antigo não dá erro nenhum — o app sobe normalmente e simplesmente não exporta trace
> nenhum, silenciosamente. Ver `application-dev.yml` dos quatro serviços.

## Propagação através do Kafka (padrão Outbox)

Cada evento gravado no Outbox (`OutboxEvent`) carrega uma coluna
`trace_context` com o `traceparent` (W3C) capturado no momento da criação do evento — **nunca**
no `OutboxPublisherScheduler`, que roda numa thread de scheduling sem nenhuma relação com a
transação original. A classe `SagaObservability` (duplicada por serviço, mesma convenção já
usada para `OutboxEvent`/`ProcessedEvent`) concentra essa lógica:

- `traceparentAtual()` — injeta `Context.current()` num carrier `Map<String,String>` via
  `OpenTelemetry.getPropagators().getTextMapPropagator()` e devolve a string `traceparent`.
  Chamada dentro do método de negócio que cria o `OutboxEvent` (ex.: `AccountService.criarOutboxEvent`).
- `injetarHeader(record, traceContext)` — copia essa string (já capturada, sem reinjetar) como
  header Kafka literal, chamada pelo `OutboxPublisherScheduler` na hora de publicar.
- `executarEtapa(etapa, traceparentRecebido, atributos, ação)` — do lado do consumer: extrai o
  `Context` do header `traceparent` recebido (`@Header(value = "traceparent", required = false)`
  em todo `@KafkaListener`), abre um span `saga.<etapa>` como filho desse contexto, seta os
  atributos, roda a ação dentro do escopo do span, e registra a duração num Timer Micrometer —
  tudo num só lugar, então tracing e métricas de latência nunca ficam dessincronizados.

Se o header vier ausente/nulo (evento antigo, ou publicado fora desse fluxo), o span simplesmente
começa um trace novo em vez de falhar — a propagação é best-effort, nunca bloqueia a saga.

`notification-service` só consome eventos (nunca publica via Outbox), então sua versão de
`SagaObservability` não tem `traceparentAtual()`/`injetarHeader()` — só `executarEtapa()`, do
lado consumer.

## Spans customizados

| Span | Serviço | Onde |
|---|---|---|
| `saga.reserve` | account-service | `TransferRequestedConsumer` → `AccountService.reservarSaldo` |
| `saga.antifraud_check` | antifraud-service | `TransferReservedConsumer` → `AntiFraudService.avaliar` |
| `saga.debit` | account-service | `DebitConfirmationRequestedConsumer` → `AccountService.confirmarDebito` |
| `saga.credit` | account-service | `CreditRequestedConsumer` → `AccountService.creditarPorTransferencia` |
| `saga.compensate` | account-service | `TransferCompensateConsumer` → `liberarReserva`/`reverterDebito` (os dois cenários de compensação) |
| `saga.notify` | notification-service | `TransferFinishedConsumer` → `NotificationService.registrarTransferConcluida`/`registrarTransferFalhou` |

Cada um carrega os atributos `transferId` e `valor` (nunca CPF/nome — ver "Sanitização" abaixo).
Os cinco consumers do `transfer-service` (que só orquestram, não executam a etapa de negócio em
si) também propagam o trace e abrem spans próprios (`saga.orchestrate.reserve_ack`,
`.antifraud_result`, `.debit_ack`, `.credit_result`, `.compensation_ack`) para que **nenhum** hop
da saga quebre o trace — não só os cinco nomeados acima.

## Sanitização

Nenhum span/atributo customizado carrega `ownerName`/`ownerDocument` (CPF) ou dado de login
(`email`/`senha`) — só IDs (`transferId`) e valores agregados (`valor`). Os spans HTTP
auto-instrumentados (Spring MVC/RestClient, via `spring-boot-starter-opentelemetry`) seguem as
convenções semânticas padrão do OTel (método, rota, status code) e não serializam corpo de
requisição/resposta.

## Métricas Prometheus

Cada serviço expõe `/actuator/prometheus` (liberado no `SecurityConfig` de account-service,
transfer-service e notification-service, igual a `/actuator/health` — o Prometheus não carrega
JWT nenhum ao fazer scrape):

- `saga_transfers_total{status="COMPLETED"|"FAILED"}` — contador, incrementado no
  `SagaOrchestratorService` exatamente nos pontos onde a state machine transiciona pra um estado
  terminal (`processarCreditoConfirmado`, `processarReservaLiberada`, `processarDebitoRevertido`,
  e o ramo `STARTED` de `processarTimeout`).
- `saga_compensations_total{reason="LIMITE_EXCEDIDO"|"VELOCITY_EXCEDIDA"|"CONTA_BLOQUEADA"|"CONTA_DESTINO_BLOCKED"|"SAGA_TIMEOUT"|...}` —
  contador, incrementado onde a compensação é decidida (`processarAntifraudeRejeitada`,
  `processarCreditoFalhou`, `processarTimeout`).
- `saga_step_duration_seconds{step="reserve"|"debit"|"credit"|"antifraud_check"|"compensate"|"orchestrate.*"}` —
  histograma (`publishPercentileHistogram()`, necessário pro `histogram_quantile` do dashboard
  funcionar), registrado dentro do mesmo `executarEtapa(...)` que abre o span.

## Dashboard Grafana

`observability/grafana-dashboard.json` — versionado no repo, provisionado automaticamente no
Grafana via `observability/grafana/provisioning/` (datasource Prometheus + o dashboard em si,
sem nenhum passo manual de import). Painéis:

1. **Taxa de sucesso de sagas** — `COMPLETED / (COMPLETED + FAILED)` nos últimos 5 minutos.
2. **Sagas concluídas vs falhas** (última hora).
3. **Sagas compensadas por motivo** (pizza, `saga_compensations_total` agrupado por `reason`).
4. **Latência p95 por etapa da saga** — `histogram_quantile(0.95, ...)` sobre
   `saga_step_duration_seconds`, uma linha por `step`, agregando dados de todos os serviços que
   emitem essa métrica (os três da saga, mais `notify` do notification-service) no mesmo gráfico.
5. **Sagas por status ao longo do tempo** (barras empilhadas).

## Teste automatizado: `TracingContextPropagationTest`

`account-service/src/test/java/.../infrastructure/observability/TracingContextPropagationTest.java`
— Kafka real via Testcontainers. Abre um span de teste, captura o `traceparent` via
`SagaObservability.traceparentAtual()`, publica uma mensagem com esse header (usando
`injetarHeader`, o mesmo código de produção), consome com um `KafkaConsumer` cru, e verifica que
o header `traceparent` chegou **byte a byte idêntico** ao produzido — a prova de que a
propagação sobrevive a um round-trip real pelo Kafka, não só em memória.

Requer `@AutoConfigureTracing` (`org.springframework.boot.micrometer.tracing.test.autoconfigure`):
por padrão o Spring Boot desativa a exportação de tracing em testes (`@SpringBootTest`), pra
nenhuma suíte tentar falar com um coletor OTLP de verdade sem pedir explicitamente — sem essa
anotação, `getTextMapPropagator()` devolve um propagator no-op e o teste falha com
`traceparent` nulo.

## Teste manual: uma transferência, um trace, três serviços

<details>
<summary>Passo a passo (clique para expandir)</summary>

1. Suba a infraestrutura completa (`docker compose up -d` — agora inclui `otel-collector`,
   `prometheus` e `grafana`) e os três serviços da saga em profile `dev`.
2. Obtenha um token (`POST /api/auth/login` no Swagger UI do account-service; veja [Segurança](seguranca.md)) e
   dispare uma transferência — `POST http://localhost:8082/api/transfers`.
3. Abra o Jaeger UI (http://localhost:16686), Service = `transfer-service`, Operation =
   `http post /api/transfers`, **Find Traces**.
4. Clique no trace. Confirme: **um único trace**, `Services 3`, com o waterfall
   `saga.reserve` → `saga.orchestrate.reserve_ack` → `saga.antifraud_check` →
   `saga.orchestrate.antifraud_result` → `saga.debit` → `saga.orchestrate.debit_ack` →
   `saga.credit` → `saga.orchestrate.credit_result`, todos sob o mesmo `http post /api/transfers`.
5. Para ver o caminho de compensação, dispare uma transferência acima de R$ 10.000 (rejeitada
   pela `LimiteValorRule`) — o trace mostra `saga.antifraud_check` seguido de `saga.compensate`
   em vez de `saga.debit`/`saga.credit`.
6. Abra o Grafana (http://localhost:3000) → dashboard "Banco Digital Saga — Transferências" e
   confirme que os painéis têm dados reais (não "No data").

</details>

Os traces reais desta verificação (capturados via API do Jaeger) estão documentados em
`observability/screenshots/README.md`.

> **Notificações**: `TransferCompleted`/`TransferFailed` são publicados
> pelo mesmo `publicarEvento(...)` que já captura o `traceparent` atual, então o mesmo trace
> se estende até `saga.notify` em `notification-service`. Isso reaproveita o mecanismo provado
> por `TracingContextPropagationTest` (account-service) e **foi verificado manualmente no
> Jaeger** subindo os quatro serviços: cada transferência que percorre a saga inteira aparece
> como um único trace com `Services 4` (`account-service`, `antifraud-service`,
> `notification-service`, `transfer-service`) e entre ~16 e ~23 spans, incluindo `saga.notify`;
> nos caminhos de falha o mesmo trace também traz `saga.compensate`. Ainda não há um teste
> automatizado dedicado de propagação para o notification-service —
> `TransferFinishedConsumerTest` confirma que a `Notification` é criada, não que o trace chega
> intacto.

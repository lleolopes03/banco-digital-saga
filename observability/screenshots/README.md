# Evidências de verificação — tracing distribuído da saga

O navegador embutido desta sessão teve uma falha na captura de screenshot (timeout em
`computer{action:"screenshot"}`, reproduzido em várias páginas e abas diferentes, incluindo
páginas simples como a home do Grafana — indicando uma falha do próprio mecanismo de captura, não
das páginas). Em vez de prints PNG, esta pasta guarda os traces reais exportados durante uma
demonstração completa (stack subida via `docker compose up`, os três serviços em profile `dev`,
transferências disparadas via API), capturados como JSON via a API REST do Jaeger
(`GET /api/traces/{traceID}`) — dado bruto e verificável, não uma reconstrução manual.

## Como reproduzir e tirar os prints reais

Com a stack no ar (`docker compose up -d` + os três serviços em profile `dev`, ver seção
"Observabilidade" do README raiz), abra:

- Jaeger UI: http://localhost:16686 — Service = `transfer-service`, Operation = `http post /api/transfers`
- Grafana: http://localhost:3000 (usuário `admin`/`admin`, ou acesso anônimo como Viewer já
  habilitado) — dashboard "Banco Digital Saga — Transferências"

## O que foi verificado nesta sessão

### 1. Caminho feliz — `trace-happy-path-raw.json`

Trace ID `3a49e30d8672c185ff123fbae120276e`, disparado por `POST /api/transfers` com
`accountOrigemId`/`accountDestinoId` reais. **Um único trace, 21 spans, 3 serviços**
(account-service: 7, antifraud-service: 1, transfer-service: 13), waterfall completo:

```
transfer-service   http post /api/transfers          (raiz)
account-service      saga.reserve
transfer-service      saga.orchestrate.reserve_ack
antifraud-service       saga.antifraud_check
transfer-service          saga.orchestrate.antifraud_result
account-service              saga.debit
transfer-service               saga.orchestrate.debit_ack
account-service                  saga.credit
transfer-service                   saga.orchestrate.credit_result
```

Confirma a propagação de trace através das 4 fronteiras assíncronas do Kafka
(`transfer.requested` → `transfer.reserved` → `antifraud.events` → `transfer.debit` →
`transfer.credit` → `transfer.credited`) como um trace único, não 6 traces desconexos.

### 2. Caminho de compensação — `trace-compensation-path-raw.json`

Trace ID `c3b3d40be9d6baec9a9728a21da7593e`, transferência de R$ 15.500,00 (acima do limite de
R$ 10.000,00 da `LimiteValorRule`), rejeitada pelo antifraud-service com motivo
`LIMITE_EXCEDIDO`. Waterfall:

```
account-service      saga.reserve
transfer-service       saga.orchestrate.reserve_ack
antifraud-service        saga.antifraud_check       (rejeitado: LIMITE_EXCEDIDO)
transfer-service           saga.orchestrate.antifraud_result
account-service               saga.compensate         (libera a reserva)
transfer-service                saga.orchestrate.compensation_ack
```

Confirma que o caminho de compensação também é rastreado como parte do mesmo trace da saga
original — não é um trace novo desconectado.

### 3. Métricas Prometheus (consultadas via API em `http://localhost:9090`)

```
saga_transfers_total{status="COMPLETED", job="transfer-service"} = 3
saga_compensations_total{reason="LIMITE_EXCEDIDO", job="transfer-service"} = 1
saga_step_duration_seconds_count{step="reserve", job="account-service"} = 3
saga_step_duration_seconds_count{step="debit", job="account-service"} = 3
saga_step_duration_seconds_count{step="credit", job="account-service"} = 3
saga_step_duration_seconds_count{step="antifraud_check", job="antifraud-service"} = 5
saga_step_duration_seconds_count{step="orchestrate.reserve_ack", job="transfer-service"} = 3
saga_step_duration_seconds_count{step="orchestrate.antifraud_result", job="transfer-service"} = 3
saga_step_duration_seconds_count{step="orchestrate.debit_ack", job="transfer-service"} = 3
saga_step_duration_seconds_count{step="orchestrate.credit_result", job="transfer-service"} = 3
```

Todos os 3 targets do Prometheus (`account-service`, `transfer-service`, `antifraud-service`)
reportaram `health: up` em `GET /api/v1/targets`.

### Bug real encontrado e corrigido durante esta verificação

O `SecurityConfig` do account-service/transfer-service (da Fatia 13) só liberava
`/api/auth/**` e `/actuator/health` sem JWT — isso derrubava o scrape do Prometheus em
`/actuator/prometheus` com 401 (o Prometheus não carrega token nenhum). Adicionado
`/actuator/prometheus` à lista de rotas públicas nos dois `SecurityConfig`.

Também corrigido: a propriedade `management.otlp.tracing.endpoint` (válida na linha 3.x do
Spring Boot) não existe mais no Spring Boot 4.1 — o nome correto é
`management.opentelemetry.tracing.export.otlp.endpoint`. Sem essa correção, nenhum span saía
para o otel-collector/Jaeger, silenciosamente (sem erro nos logs). Ver `application-dev.yml`
dos três serviços.

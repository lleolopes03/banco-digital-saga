# Changelog

Mudanças relevantes do projeto, agrupadas por data (ainda não há versões numeradas). Formato inspirado em
[Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/).

## 2026-10-21

### Adicionado
- Documentação OpenAPI completa nos três serviços com API HTTP: resumo e descrição de cada endpoint, respostas com os
  códigos reais (`201`, `202`, `204` e os de erro `400`, `401`, `403`, `404`, `409`, `422`, `429`), schema de erro
  único, exemplos nos DTOs e login marcado como público. `OpenApiContractTest` compara a spec com o comportamento real.
- `EventTypeContractTest` nos quatro serviços: fixa os nomes dos eventos publicados no Kafka.
- Documentação reorganizada em `docs/` (arquitetura de cada tema, configuração, solução de problemas), `CONTRIBUTING.md`
  e este `CHANGELOG.md`; o README passou a ser um documento de entrada.
- CI: o job que falha publica o nome e o stack trace do teste como anotação, e os downloads do Maven têm retry.

### Alterado
- Os consumers roteiam eventos por classe (`EventRouter`) em vez de `switch` com texto. Não há mais texto de tipo de
  evento no código de produção.
- Nomes de comportamento do domínio de conta em português: `Account.abrir`, `depositar` e as fábricas de
  `TransactionLogEntry` (`deposito`, `transferenciaEnviada`, `transferenciaRecebida`, `estorno`).
- **Comportamento da API:** erros de validação (`400`) passam a devolver o formato padrão de erro
  (`timestamp`, `status`, `error`, `message`) com o campo e o motivo, em vez do corpo padrão do Spring.

### Corrigido
- Laço de reconexão do `KafkaAdmin` nos testes `contextLoads`, que consumia CPU e derrubou um job do CI.

## 2026-10-20

### Adicionado
- Poller do Outbox mais rápido (intervalo de 100 ms, envio em pipeline, dreno em loop) e seguro para várias instâncias
  (`FOR UPDATE SKIP LOCKED`, um evento por agregado), com índice parcial e teste de concorrência.
- Tópicos da saga declarados com 3 partições e consumers com 3 threads; dead-letter com partição escolhida pelo producer.
- Jaeger com armazenamento em disco (72 h) e limite de memória; `memory_limiter` no collector; amostragem de traces
  configurável.
- `anatomia-saga.mjs`, que decompõe onde o tempo da saga é gasto.

### Alterado
- Refatoração para responsabilidade única e sem repetição nos quatro serviços (`AccountSagaService`,
  `TransferInitiationService`, `SagaOutboxPublisher`, `OutboxRecordFactory`, `EventPayloadReader`, `BalanceCache`,
  `@RetryOnOptimisticLock`).
- Comentários do backend enxugados para o porquê não óbvio (de 1.092 para 689 linhas).
- Medido na rampa de carga: latência mediana da saga a 12 tx/s de 19,7 s para 0,9 s.

## 2026-10-15

### Adicionado
- Retenção de `outbox_events` e `processed_events` (lotes, transações curtas) e índice TTL no MongoDB.
- Testes de carga (rampa e longa duração) com sementes de contas.
- Falha rápida da saga quando a reserva de saldo é rejeitada (`TransferReservationFailed`), com o motivo real.

### Corrigido
- O `transfer-service` repassa o JWT na pré-checagem síncrona de saldo (antes caía no fallback em 100% dos casos).
- Scrape de métricas do `notification-service` e índices do MongoDB que nunca eram criados.
- README: a saga é orquestrada, não coreografada.

## 2026-08-10

### Adicionado
- `notification-service` de verdade, com MongoDB, consumindo o evento terminal da saga
  (`TransferCompleted`/`TransferFailed`).
- Swagger/OpenAPI (springdoc) em account, transfer e notification.
- `notification-service` no pipeline de CI/CD e no gate ponta a ponta da saga.

## 2026-07-24

### Adicionado
- Dead-letter queue nos consumers Kafka de account-service e antifraud-service.

## 2026-07-23

### Adicionado
- Dead-letter queue nos consumers Kafka do transfer-service.

## 2026-07-20

### Adicionado
- Pipeline de CI/CD: lint, testes com Testcontainers, build de imagens Docker e deploy no Railway.

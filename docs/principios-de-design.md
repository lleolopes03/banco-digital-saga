# Princípios de design (Clean Code e SOLID)

[← Voltar ao README](../README.md)

| Princípio | Onde aparece |
|---|---|
| **Responsabilidade única** | `AccountService` (casos de uso HTTP) × `AccountSagaService` (passos da saga); `TransferInitiationService` (abre a transferência) × `SagaOrchestratorService` (conduz a saga) × `TransferQueryService` (consulta); `OutboxPublisherScheduler` (ciclo reivindicar-enviar-confirmar) × `OutboxRecordFactory` (registro Kafka e roteamento de tópico). |
| **Aberto/fechado** | Um novo tipo de evento é um novo `record` em `domain.event`, uma linha em `TOPICO_POR_EVENTO` (produtor) e um `.quando(...)` no `EventRouter` do consumer, sem editar `switch`. O `eventType` é o nome da classe do evento, então não há texto de tipo no código de produção; `EventTypeContractTest` é o único lugar que o fixa e falha se um `record` for renomeado. O ciclo de vida da saga é dado (`SagaStateMachine`), não `if`s espalhados. |
| **Inversão de dependência** | `BalanceCache` é um porto da camada de aplicação; `RedisBalanceCache` (infraestrutura) é o adaptador. O caso de uso decide *quando* ler, gravar e invalidar sem saber que o armazenamento é Redis, e os testes unitários usam um mock simples. |
| **Segregação de interfaces** | Portos pequenos e focados (`BalanceCache`: buscar, guardar, invalidar) em vez de depender de um cliente Redis inteiro. |
| **Não se repetir** | `OutboxEventWriter` / `SagaOutboxPublisher` (serialização, traceparent, tipo do evento), `@RetryOnOptimisticLock` (a política de retry em um lugar só), `EventPayloadReader` (desserialização dos consumers) e `executarPasso` (transação + invalidação de cache depois do commit). |
| **Clean Code** | Nomes que dizem a intenção (`liberacaoDeReservaSolicitada`, `falharSaga`, `compensar`); métodos curtos; handlers da saga sem blocos copiados; comentários explicam o *porquê* (concorrência, ordem de commit e cache, entrega *at-least-once*), não o *quê*. |

**Convenção de nomes.** Regras de negócio e casos de uso em português (`reservar`, `liberarReserva`,
`Account.abrir`, `depositar`); vocabulário técnico de padrões em inglês (Outbox, Scheduler, Factory,
Writer, Router). Valores persistidos ou publicados (`TransactionType`, nomes de evento) não mudam
com a convenção, para não quebrar dados e contratos existentes.

`RetryOnOptimisticLockTest` prova que a meta-anotação realmente aciona o retry do spring-retry
(sem ele, um conflito de lock deixaria de ser reexecutado e nenhum outro teste perceberia).

**Duplicação deliberada entre serviços.** `OutboxEvent`, `ProcessedEvent`, `IdempotentEventHandler`,
`SagaObservability`, `KafkaErrorHandlingConfig` e `EventPayloadReader` existem uma vez em cada
serviço. É uma troca consciente: cada serviço é um deploy independente, com build e imagem Docker
próprios, e compartilhar uma biblioteca acoplaria versões e o pipeline de todos. O custo é replicar
correções à mão. Com mais serviços, o passo natural é extrair um módulo `bank-commons` publicado
num repositório de artefatos.

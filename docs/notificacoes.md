# Notificações (MongoDB)

[← Voltar ao README](../README.md)

O `notification-service` fecha o ciclo da saga: escuta a conclusão ou falha da transferência via Kafka e
registra uma notificação — histórico consultável pela conta dona da transferência.

## Por que MongoDB, e não mais um Postgres

Cada canal de notificação tem um formato completamente diferente: um e-mail carrega
assunto+corpo HTML, um push carrega título+ícone+deep link, um SMS é só texto curto. Modelar
isso numa tabela relacional viraria colunas majoritariamente nulas por linha (uma tabela por
canal seria o outro extremo, mais rígido ainda para adicionar um canal novo). Um documento
resolve isso naturalmente — cada `Notification` carrega os campos comuns (`title`, `body`,
`createdAt`) mais um `metadata` (`Map<String, Object>`) cujo formato varia por `channel`
(`EMAIL`, `PUSH`, `SMS`). É também o único serviço do projeto que não faz JOIN nenhum: a única
consulta é "notificações de uma conta, mais recentes primeiro" — um índice composto
(`accountId`, `createdAt`) resolve isso sem nenhuma modelagem relacional.
`notification-service` é, por isso, o único banco não-Postgres do projeto (ver
[Notification.java](../notification-service/src/main/java/com/bancodigital/notification/domain/Notification.java)).

## Da saga ao Kafka: o evento terminal que faltava

Só o WebSocket (`SagaStatusPublisher`, best-effort: se ninguém estiver assinando naquele instante, a
mensagem se perde) e as métricas marcariam o fim de uma saga; sem um evento Kafka durável dizendo "esta saga
terminou", nenhum outro serviço conseguiria reagir de forma confiável. O `SagaOrchestratorService`
(`transfer-service/.../application/SagaOrchestratorService.java`) publica, via Outbox, um
`TransferCompleted` ou `TransferFailed` (motivo incluído) exatamente nos mesmos quatro pontos
onde já registrava a métrica `saga_transfers_total` — os dois eventos dividem o tópico
`transfer.finished`, distinguidos pelo header `eventType` (mesmo padrão já usado em
`transfer.compensate`).

## Consumer, idempotência e DLQ

`TransferFinishedConsumer` escuta `transfer.finished` e, para cada evento, cria uma única
`Notification` (canal `EMAIL`, escolhido como representante — o domínio já tem
`Notification.push`/`Notification.sms` prontos para quando um canal de envio real existir):
destinatário da transferência recebe "transferência recebida"; remetente recebe "não concluída"
com o motivo. Segue a mesma proteção de idempotência (Redis + fonte de verdade durável) e DLQ
(`KafkaErrorHandlingConfig`, 1 tentativa + 2 retries → `transfer.finished-dlt`) já documentadas
nas seções de Resiliência acima — só que a fonte de verdade da idempotência
(`ProcessedEventRepository`) é uma collection MongoDB aqui, não uma tabela Postgres, já que este
é o único banco do serviço. O span `saga.notify` e a métrica `saga_step_duration_seconds{step="notify"}`
seguem exatamente o mesmo `SagaObservability.executarEtapa(...)` dos outros serviços, então a
saga inteira — incluindo a notificação — continua aparecendo como um único trace no Jaeger.

## Consulta: `GET /api/notifications/{accountId}`

Histórico paginado, protegido pelo mesmo `JwtAuthenticationFilter`/`SecurityConfig` duplicados
dos outros serviços autenticados — o `{accountId}` da URL precisa bater com o `accountId` do
token, senão `403` (mesma convenção de [Segurança](seguranca.md)).

## Testes

`NotificationRepositoryTest` (paginação/ordenação, Mongo real via Testcontainers),
`TransferFinishedConsumerTest` (Kafka real: publica `TransferCompleted`/`TransferFailed` e
confirma a `Notification` persistida, incluindo o cenário de evento reentregue duas vezes),
`KafkaErrorHandlingConfigTest` (mesmo padrão de prova de DLQ dos outros serviços) e
`NotificationControllerIT` (200 para o dono da conta, 403 para outra conta, 401 sem token) e
`MongoIndexesTest`, que garante que os índices declarados existem de fato no banco (o índice
composto `accountId_createdAt` e o TTL de `processed_events`). O Spring Data MongoDB vem com a
criação automática de índices desligada; por isso `spring.data.mongodb.auto-index-creation` está
`true` — um teste de repositório comum não pega a ausência, a query só fica lenta.

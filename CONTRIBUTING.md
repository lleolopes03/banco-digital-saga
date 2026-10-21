# Contribuindo

Este é um projeto de aprendizado, mas segue o fluxo de um projeto de equipe. Este guia resume o que é preciso
para propor uma mudança.

## Pré-requisitos

- JDK 21 e Docker (os testes sobem Postgres, MongoDB, Kafka e Redis efêmeros via Testcontainers; dê ao Docker pelo
  menos 6 a 8 GB de memória)
- Node 22 para o frontend (`bank-frontend`)

O passo a passo para subir tudo localmente está no [README](README.md#subindo-o-ambiente).

## Fluxo

1. Crie uma branch a partir da `main`.
2. Faça a mudança com testes (veja abaixo).
3. Rode a verificação local do serviço alterado.
4. Abra um pull request. O CI roda lint, testes de cada serviço, o gate ponta a ponta da saga e o build das imagens.
   O deploy só acontece em push na `main`, depois de tudo verde.

## Verificação local

Em cada serviço alterado (`account-service`, `transfer-service`, `antifraud-service`, `notification-service`):

```bash
./mvnw spotless:apply   # formata o código; o CI falha se estiver fora do formato
./mvnw verify           # testes unitários e de integração (precisa do Docker)
```

Se a mudança mexer em eventos ou no fluxo da saga, rode também o gate ponta a ponta. Ele usa os serviços como
bibliotecas do repositório Maven local, então instale-os antes:

```bash
for s in account-service antifraud-service transfer-service notification-service; do
  (cd $s && ./mvnw install -DskipTests)
done
cd e2e-tests && mvn verify
```

Frontend: `npm test` e `npm run lint` em `bank-frontend`.

Algo falhando de um jeito estranho? Veja [Solução de problemas](docs/solucao-de-problemas.md).

## Convenções de código

- **Nomes:** regras de negócio e casos de uso em português; vocabulário técnico de padrões (Outbox, Scheduler, Factory,
  Router) em inglês. Valores persistidos ou publicados não mudam de nome por causa da convenção.
- **Princípios:** responsabilidade única, sem repetição de código e comentários só para o porquê que não é óbvio. Os
  detalhes e as decisões estão em [Princípios de design](docs/principios-de-design.md).
- **Duplicação entre serviços é deliberada** (cada um é um deploy independente, sem biblioteca compartilhada): ao
  corrigir uma classe duplicada (`OutboxEvent`, `IdempotentEventHandler`, `KafkaErrorHandlingConfig`...), replique a
  correção nos outros serviços.

## Testes

Toda mudança de comportamento vem com teste. Alguns testes funcionam como contrato e falham de propósito quando algo
diverge:

- `OpenApiContractTest` (account, transfer, notification): a spec OpenAPI deve bater com o comportamento real. Ao criar
  ou alterar um endpoint, documente-o (`@Operation`, `@ApiResponse` com os códigos reais, `@Schema` nos DTOs) e
  atualize a tabela de contrato do teste.
- `EventTypeContractTest` (todos os serviços): o nome da classe de cada evento é o tipo publicado no Kafka. Renomear um
  evento exige renomear nos serviços que o produzem e consomem.

## Mensagens de commit

Em português, no imperativo, com um resumo curto na primeira linha e o motivo no corpo quando não for óbvio:

```
Corrige laco de reconexao do KafkaAdmin nos testes contextLoads

O KafkaTopicsConfig faz o KafkaAdmin conectar na subida; testes sem Kafka ...
```

## Documentação

Se a mudança altera configuração, endpoints ou comportamento visível, atualize o que for afetado: o
[README](README.md), a [tabela de configuração](docs/configuracao.md) e o [CHANGELOG](CHANGELOG.md).

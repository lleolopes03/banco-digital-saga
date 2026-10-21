# Resiliência

[← Voltar ao README](../README.md)

## Resiliência: circuit breaker na validação síncrona de saldo

A comunicação entre serviços é majoritariamente assíncrona via Kafka (resiliente por natureza:
uma partição sem consumidor não derruba quem publica). A única chamada síncrona do sistema é uma
otimização em `transfer-service`: antes de abrir uma saga inteira (`POST /api/transfers`), ele
consulta `GET /api/accounts/{id}/balance` no `account-service` via `RestClient`, para recusar de
cara (`422 Unprocessable Entity`) um caso óbvio de saldo insuficiente sem gastar uma saga completa
com isso. A chamada repassa o JWT de quem está criando a transferência (header `Authorization`
da requisição corrente), porque o `account-service` exige autenticação nessa rota — sem isso a
pré-checagem voltaria sempre `401` e cairia no fallback sem ninguém notar.

Essa chamada é protegida por um circuit breaker Resilience4j (`AccountServiceClient`, em
`transfer-service/src/main/java/com/bancodigital/transfer/infrastructure/client/`):

- **Fechado (padrão)**: chama o account-service normalmente.
- **Aberto** (`transfer-service/src/main/resources/application.yml`, instância `accountService`:
  janela de 10 chamadas, abre com 50% de falha, fica aberto por 10s): pula direto para o
  fallback, **sem** tentar a chamada de rede.
- **Fallback (`balanceCheckFallback`)**: nunca bloqueia o usuário. Ele simplesmente devolve
  "saldo desconhecido" e deixa a saga ser criada normalmente — a validação definitiva de saldo é
  a reserva assíncrona (etapa 1 da saga), que independe desta chamada síncrona.

Métricas em tempo real:

```
GET http://localhost:8082/actuator/health           # mostra o CircuitBreakerHealthIndicator
GET http://localhost:8082/actuator/circuitbreakers   # estado, contagem de chamadas, taxa de falha
```

### Teste manual: derrubando o account-service e observando o circuito abrir

O mesmo comportamento (chamada normal → falhas abrindo o circuito → fallback sem bloquear a saga
→ half-open tentando a chamada real de novo) é coberto automaticamente por
`TransferServiceCircuitBreakerTest` (WireMock simulando o account-service), sem precisar derrubar
nada manualmente — o roteiro abaixo é só para ver o mesmo efeito ao vivo.

<details>
<summary>Passo a passo (clique para expandir)</summary>

Neste repositório o `account-service` roda como processo local (`./mvnw spring-boot:run`), não
como container — ajuste o passo 2 caso você tenha containerizado os serviços.

1. Suba a infraestrutura (`docker compose up -d`) e os quatro serviços em profile `dev`
   (`account-service` na porta `8081`, `transfer-service` na `8082`).
2. Confirme o circuito fechado:
   ```bash
   curl http://localhost:8082/actuator/circuitbreakers
   # "accountService": { "state": "CLOSED", ... }
   ```
3. Derrube o `account-service` (Ctrl+C no terminal onde ele roda, ou `docker stop
   <container-do-account-service>` se estiver containerizado).
4. Dispare pelo menos 10 transferências para forçar a avaliação da janela deslizante:
   ```bash
   for i in $(seq 1 10); do
     curl -s -o /dev/null -w "%{http_code}\n" -X POST http://localhost:8082/api/transfers \
       -H "Content-Type: application/json" \
       -d '{"accountOrigemId":"'$(uuidgen)'","accountDestinoId":"'$(uuidgen)'","valor":100.00}'
   done
   ```
   Todas devem responder `202 Accepted` — o fallback deixa a saga seguir mesmo sem o
   account-service no ar.
5. Confirme o circuito aberto:
   ```bash
   curl http://localhost:8082/actuator/circuitbreakers
   # "accountService": { "state": "OPEN", "failureRate": "100.0%", ... }
   ```
6. Suba o `account-service` de novo, espere ~10s (`waitDurationInOpenState`) e repita o passo 2:
   o estado deve ter voltado para `HALF_OPEN` e, após chamadas bem-sucedidas, para `CLOSED`.

</details>

## Resiliência: dead-letter queue (DLQ) nos consumers Kafka

Kafka processa mensagens em ordem dentro de uma partição. Se um `@KafkaListener` recebe uma
mensagem que falha de forma **permanente** (payload corrompido, uma transição rejeitada pela
`SagaStateMachine`) e simplesmente entra em loop de retry, nenhuma mensagem publicada depois dela
naquela partição é processada — uma "poison pill" trava a fila inteira, não só o item problemático.

Os quatro serviços com `@KafkaListener` de verdade (`account-service`, `transfer-service`,
`antifraud-service` e `notification-service`) registram um
`DefaultErrorHandler` com `DeadLetterPublishingRecoverer` (`KafkaErrorHandlingConfig`, em
`infrastructure/messaging` de cada um): até 3 tentativas (1 inicial + 2 retries, 1s de backoff
entre elas — deliberadamente curto, para não competir com o timeout de 2 minutos do
`SagaTimeoutScheduler`, que é a rede de segurança maior para sagas travadas). Esgotadas as
tentativas, a mensagem é desviada para o tópico `<topico-original>-dlt` (sufixo padrão desta
versão do Spring Kafka — **não** `.DLT`, como a maioria da documentação mais antiga descreve) e o
consumer segue processando as próximas normalmente. O Spring Boot injeta esse error handler
automaticamente em todos os `@KafkaListener` do serviço assim que o bean existe, sem precisar
configurar cada consumer individualmente.

| Serviço | Consumers cobertos | Por que importa |
|---|---|---|
| `account-service` | `TransferRequestedConsumer`, `DebitConfirmationRequestedConsumer`, `CreditRequestedConsumer`, `TransferCompensateConsumer` | É quem move dinheiro de fato (reserva/débito/crédito) — uma partição travada aqui trava o núcleo da saga |
| `transfer-service` | os cinco consumers do orquestrador (`TransferReservedConsumer`, `AntiFraudEventConsumer`, `DebitConfirmedConsumer`, `CreditConfirmedConsumer`, `TransferCompensatedConsumer`) | Sem isso, uma mensagem travada aqui trava a orquestração inteira da saga |
| `antifraud-service` | `TransferReservedConsumer` | Menor superfície, mas sem DLQ uma falha permanente trava toda avaliação de fraude subsequente |
| `notification-service` | `TransferFinishedConsumer` | Único consumer do serviço, mas sem DLQ uma falha permanente pararia todo o histórico de notificações |

Cada um dos quatro serviços tem um `KafkaErrorHandlingConfigTest` (Kafka real via Testcontainers,
mesmo grupo de consumer usado em produção): publica um payload que falha na deserialização em
toda tentativa e confirma que a mensagem aparece no tópico `-dlt` correspondente — prova de que o
error handler está de fato ligado ao container dos listeners, não só declarado.

**Evolução possível**: hoje as mensagens na DLQ ficam só armazenadas, visíveis e não
perdidas, mas sem reprocessamento automático nem alerta. Um consumer dedicado lendo da DLT — para
reprocessamento manual após corrigir a causa raiz, ou alimentando um contador Prometheus
(`kafka_dlt_messages_total`) que dispara alerta no Grafana — é a evolução natural, aproveitando a
stack de observabilidade que já existe.

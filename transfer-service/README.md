# transfer-service

Orquestrador da saga de transferência. Recebe o pedido de transferência, coordena reserva de
saldo, checagem antifraude, débito e crédito entre contas — e, em caso de falha em qualquer
etapa, aciona a compensação para devolver o saldo reservado/debitado à conta de origem.

## Estado desta fatia

Esta fatia contém **só a fundação da saga**: a entidade `SagaInstance` e a máquina de estados
(`SagaStateMachine`) que valida quais transições são permitidas. Ainda não há nenhum consumer ou
producer Kafka conectado — isso é proposital, para permitir revisar a modelagem isoladamente
antes de conectar a saga aos outros serviços (account-service e antifraud-service) nas próximas
fatias.

## Máquina de estados da saga

A `SagaStateMachine` é uma classe de domínio pura (sem Spring, sem qualquer framework): ela
mantém um mapa `estado atual → conjunto de estados válidos seguintes` e só aplica uma transição
em uma `SagaInstance` se o estado de destino do evento estiver nesse conjunto. Qualquer tentativa
fora do mapa lança `InvalidSagaTransitionException` **sem alterar o estado da saga** — é assim
que se garante que a saga nunca fica em um estado intermediário/inconsistente: uma transição ou
é aplicada por completo (status + currentStep + updatedAt), ou não é aplicada de forma alguma.

```
STARTED
   │  RESERVAR_SALDO
   ▼
RESERVED ────────────────────┐
   │  SOLICITAR_ANTIFRAUDE    │ FALHAR (compensação, fatia futura)
   ▼                          │
ANTIFRAUD_PENDING             │
   │              │           │
   │ APROVAR_     │ REJEITAR_ │
   │ ANTIFRAUDE   │ ANTIFRAUDE│
   ▼              ▼           │
ANTIFRAUD_      ANTIFRAUD_    │
APPROVED        REJECTED      │
   │              │           │
   │ DEBITAR      │ FALHAR    │
   ▼              ▼           │
DEBITED ──────► FAILED ◄──────┘
   │  CREDITAR      ▲
   ▼                │ FALHAR
CREDITED ───────────┘
   │  COMPLETAR
   ▼
COMPLETED
```

Resumo textual das transições válidas (estado atual → estados seguintes possíveis):

| Estado atual        | Estados seguintes válidos            |
|----------------------|---------------------------------------|
| `STARTED`            | `RESERVED`                            |
| `RESERVED`           | `ANTIFRAUD_PENDING`, `FAILED`         |
| `ANTIFRAUD_PENDING`  | `ANTIFRAUD_APPROVED`, `ANTIFRAUD_REJECTED` |
| `ANTIFRAUD_APPROVED` | `DEBITED`                              |
| `ANTIFRAUD_REJECTED` | `FAILED`                               |
| `DEBITED`            | `CREDITED`, `FAILED`                  |
| `CREDITED`           | `COMPLETED`, `FAILED`                 |
| `COMPLETED`          | *(estado terminal)*                   |
| `FAILED`             | *(estado terminal)*                   |

`RESERVED`, `DEBITED` e `CREDITED` podem ir para `FAILED` via compensação — o disparo real dessa
compensação (estornar saldo reservado/debitado) é implementado em uma fatia futura; aqui só a
transição de estado já está modelada e testada.

## Estrutura de pacotes

```
src/main/java/com/bancodigital/transfer/
├── domain/            SagaInstance, SagaStatus, SagaEvent, SagaStateMachine, exceções
├── application/        (vazio nesta fatia)
├── infrastructure/
│   ├── persistence/     SagaInstanceRepository
│   └── messaging/        (vazio nesta fatia — sem Kafka ainda)
└── web/                 (vazio nesta fatia)
```

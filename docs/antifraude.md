# Antifraude

[← Voltar ao README](../README.md)

O `antifraud-service` não expõe nenhum endpoint HTTP: ele só participa da saga via Kafka. Ele consome
`TransferReserved` (publicado pelo `account-service` depois de reservar o saldo), avalia a transferência e
publica `AntiFraudApproved` ou `AntiFraudRejected` pelo Outbox. A reserva que falhou
(`TransferReservationFailed`) não chega a ser avaliada.

## Regras

As regras rodam em ordem, da mais barata para a mais cara, e a primeira que reprova encerra a avaliação
(`AntiFraudRuleEngine`):

| Ordem | Regra | O que verifica | Motivo da rejeição | Custo |
|---|---|---|---|---|
| 1 | `LimiteValorRule` | Valor da transferência acima de R$ 10.000 | `LIMITE_EXCEDIDO` | Nenhum I/O |
| 2 | `VelocityCheckRule` | Mais de 5 transferências da mesma conta de origem em 60 segundos (contador no Redis, `INCR` + `EXPIRE`) | `VELOCITY_EXCEDIDA` | Redis |
| 3 | `BlacklistRule` | Conta de origem ou de destino na lista de contas bloqueadas | `CONTA_BLOQUEADA` | PostgreSQL |

Os limites são constantes no código das regras, não propriedades de configuração.

## O que acontece com a saga

- **Aprovada:** o `transfer-service` segue para a confirmação do débito.
- **Rejeitada:** a saga vai para `ANTIFRAUD_REJECTED`, registra o motivo e dispara a compensação (liberar a
  reserva). Só vira `FAILED` quando o `account-service` confirma a liberação. Ver o fluxo completo em
  [Arquitetura](../README.md#arquitetura).

O resultado fica registrado na tabela `antifraud_checks` do banco do próprio serviço.

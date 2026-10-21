# Transferência em tempo real (WebSocket/STOMP)

[← Voltar ao README](../README.md)

A tela de transferência (`/transferencia`) acompanha o progresso da saga ao vivo, sem polling —
é o maior diferencial visual do projeto (ver o GIF na [Demonstração](../README.md#demonstração) do README).

## Backend

`transfer-service` expõe um endpoint STOMP em `/ws` (`WebSocketConfig`, broker simples em
`/topic`). Depois de cada transição de estado da `SagaInstance` (nos mesmos pontos onde os
consumers Kafka já persistiam o novo status), o `SagaOrchestratorService` publica
um `SagaStatusUpdate` (status atual + `failureReason`, se houver) em
`/topic/transfers/{transferId}` via `SagaStatusPublisher`. A publicação é best-effort — se
ninguém estiver assinando naquele instante, a mensagem se perde e o fallback de polling do
frontend (abaixo) cobre o caso.

**Autenticação e autorização da conexão**: um WebSocket nativo do browser não permite anexar um
header `Authorization` no handshake HTTP inicial — por isso `/ws/**` é liberado no
`SecurityConfig`, e a autenticação de verdade acontece no frame STOMP `CONNECT`
(`StompAuthChannelInterceptor`), lendo o JWT de um header STOMP nativo (não HTTP). O mesmo
interceptor autoriza cada `SUBSCRIBE`: só quem é dono da conta de origem ou destino daquela
transferência pode assinar seu tópico.

## Frontend

`TransferStatusService` encapsula a conexão STOMP (`@stomp/stompjs`) atrás de um
`Observable<SagaStatusUpdate>`. `TransferStatusComponent` monta um stepper com 5 passos
(reservando → antifraude → débito → crédito → concluída); cada mensagem recebida atualiza o
estado visual do passo correspondente, e uma falha/compensação pinta o passo relevante de
vermelho com o `failureReason` recebido. A conexão fecha sozinha (`client.deactivate()`) quando a
saga chega a um estado terminal (`COMPLETED`/`FAILED`) ou quando o componente é destruído — nunca
fica aberta além do necessário.

**Fallback de polling**: se a conexão WebSocket cair ou nunca se estabelecer, o componente
detecta (erro ou `complete()` sem status terminal) e passa a consultar
`GET /api/transfers/{id}` a cada 2 segundos até atingir um status terminal — a UI mostra
claramente qual dos dois modos está ativo ("Acompanhando em tempo real" vs. "Atualizando a cada
2s").

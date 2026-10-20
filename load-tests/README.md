# Teste de carga com rampa

Mede **onde o sistema degrada** conforme a taxa de transferências sobe, em vez de olhar um
número isolado. Usa só Node 18+ (nenhuma dependência), contra os serviços rodando localmente.

## Como rodar

```bash
# 1. infraestrutura + os 4 serviços no ar (ver README principal, "Subindo o ambiente")

# 2. contas sintéticas de origem/destino (uma vez)
docker exec -i bank-postgres-account psql -U account -d account_db < load-tests/seed-contas-carga.sql

# 3. a rampa (padrão: 1 → 2 → 4 → 6 → 8 → 10 → 12 tx/s, ~7 min, ~2.550 transferências)
node load-tests/ramp-test.mjs

# rodada curta de fumaça
STAGES="1:5,2:10" ACCOUNTS=50 DRAIN_SECONDS=10 node load-tests/ramp-test.mjs
```

Variáveis: `STAGES` (`taxa:segundos,...`; o primeiro estágio é o aquecimento da JVM), `ACCOUNTS`
(≤ 200 semeadas), `DRAIN_SECONDS`, `ABORT_ERROR_RATE`, `ABORT_P95_MS`, `TRANSFER_URL`,
`ACCOUNT_URL`, `PROMETHEUS_URL`, `JWT_SECRET`. O relatório sai no console e em
`load-tests/results/` (ignorado pelo git).

## Decisões de desenho

- **Modelo aberto.** As requisições saem no ritmo planejado mesmo que o sistema fique lento, como
  em produção. Um modelo "fechado" (N usuários esperando resposta) desaceleraria junto com o
  sistema e esconderia a saturação.
- **200 contas de origem em rodízio.** O sistema tem limites de negócio *por conta de origem*:
  rate limit de 10 req/min e a regra de velocidade do antifraude (>5/min vira
  `VELOCITY_EXCEDIDA`). Concentrar a carga em poucas contas mediria essas regras, não a
  capacidade. Com 200 contas e 12 tx/s, cada uma envia ~3,6/min.
- **Tempo da saga lido do banco, depois do teste.** `updated_at − created_at` das sagas
  `COMPLETED`. Fazer polling de `GET /api/transfers/{id}` durante a carga adicionaria tráfego
  ao que está sendo medido.
- **Tokens gerados localmente** com o segredo de dev, em vez de 200 logins: o objetivo é medir a
  saga, não o BCrypt do login.
- **Contagem por coorte.** A vazão de um estágio conta as sagas *criadas* nele que terminaram,
  não as concluídas dentro da janela de tempo — esta última subestimaria nas bordas, já que cada
  saga leva alguns segundos.

## O que o relatório mostra

1. **Cliente** — latência do `POST /api/transfers` (o `202`), `429`/`5xx`/timeouts por estágio.
2. **Saga ponta a ponta** — p50/p95/p99/máx, sagas pendentes após o dreno, concluídas/s.
3. **Servidor** — p95 por etapa da saga (Prometheus), ao fim de cada estágio.
4. **Diagnóstico** — primeiro estágio em que algum critério de saturação aparece. É uma
   heurística com critérios impressos junto (não um oráculo): >5% de respostas ≠ 202, >5% de
   sagas pendentes, >5% `FAILED`, ou p95 da saga >2× o do primeiro estágio medido.

A rampa **para sozinha** se um estágio passar de 25% de erro ou de 5 s de p95 no `POST`.

## Teste de longa duração (`soak-test.mjs`)

A rampa responde "onde quebra"; o soak responde "o que aparece com o tempo": vazamento de
memória, deriva de latência, fila crescendo, sagas presas, DLQ enchendo e quanto o sistema leva
para se recuperar de um pico.

```bash
# até as 14:00 locais (DEADLINE), ciclos de 15 min: 8 min a 1,5 tx/s + 30 s a 12 tx/s + recuperação
SUSTAIN_RATE=1.5 DEADLINE=14:00 node load-tests/soak-test.mjs
# rodada curta para validar
DURATION_MIN=3 SUSTAIN_SECONDS=30 PEAK_SECONDS=10 CYCLE_SECONDS=75 ACCOUNTS=50 node load-tests/soak-test.mjs
```

Amostra a cada 30 s (heap, CPU, threads, pool, erros, 5xx, saúde, sagas pendentes/presas, Outbox
pendente, memória do Jaeger e do Kafka) e imprime linhas `CICLO` e `ALERTA`. Ao final confere a
**integridade**: sagas terminais × notificações no MongoDB e `transferId` duplicado.

**Válvula de segurança:** a memória do Jaeger era ilimitada quando ele guardava os traces em
memória (~120 KB por trace medido aqui). O `docker-compose.yml` agora o roda com armazenamento em
disco (Badger, retenção de 72h) e `mem_limit: 1g`, mas a válvula continua: acima de
`MAX_JAEGER_MB` (2600) a carga para e o monitoramento continua.

### Resultado de referência *antes* das otimizações (7 ciclos, 1h40, 7.560 sagas — ambiente de dev)

Poller do Outbox a cada 500 ms, um `send().get()` por evento, tópicos de 1 partição.

Sustentado (1,5 tx/s): p50 ≈ 2,2 s, p95 ≈ 2,9–3,1 s, sem deriva. Pico (12 tx/s × 30 s): p95
11–14 s, voltando a zero pendente em 12–14 s. Piso de heap plano nos 4 serviços. 7.560 sagas →
7.560 notificações, 0 duplicadas, 0 dead-letter, 0 erros 5xx. A latência da saga era dominada por
esperas (poller do Outbox a cada 500 ms × ~8 saltos), não por processamento: as JVMs ficaram em
1–6% de CPU. O joelho de capacidade, estimado pelo crescimento da fila, ficava em ~9 tx/s.

## Efeito das otimizações no Outbox e no Kafka

A mesma rampa (2.550 sagas, 200 contas, 7 estágios, 0 erros, 0 sagas pendentes, 0 `FAILED` nas
três rodadas), em três momentos. Latência da saga ponta a ponta, p50 / p95 em segundos:

| tx/s | Antes (poller 500 ms, 1 partição) | Poller novo (100 ms, pipeline, dreno) | + 3 partições e 3 threads por listener |
|---|---|---|---|
| 2 | 2,4 / 3,0 | 0,5 / 0,8 | 0,6 / 1,3 |
| 4 | 2,8 / 4,2 | 0,6 / 0,9 | 0,6 / 0,8 |
| 6 | 3,6 / 6,6 | 0,7 / 1,0 | 0,7 / 2,1 |
| 8 | 4,9 / 13,4 | 0,8 / 3,0 | 0,8 / 3,4 |
| 10 | 8,2 / 12,0 | 0,8 / 2,8 | 0,8 / 3,5 |
| 12 | 19,7 / 26,2 | 0,9 / 5,3 | 1,0 / 3,4 |

Como ler isto:

- **A mudança que importou foi o poller.** A mediana a 12 tx/s caiu de 19,7 s para 0,9 s, e a
  mediana deixou de crescer com a carga em toda a faixa testada (a cauda p95 ainda sobe a partir
  de 8 tx/s). `anatomia-saga.mjs` mostra por quê: antes, ~95% do tempo de uma saga era *espera de
  publicação* no Outbox (nas sagas medianas da rampa, 9,2 s de 9,7 s); depois, a saga mediana
  leva 0,9 s, dos quais 0,78 s ainda são espera de publicação (~8 eventos × ~100 ms de poll).
- **Partições + paralelismo no consumer tiveram efeito pequeno e inconclusivo.** Só o p95/p99 a
  12 tx/s melhorou de forma visível (5,3 → 3,4 s e 6,3 → 4,2 s); a 6–10 tx/s ficou igual ou um
  pouco pior, dentro do que esperamos de variação entre rodadas em uma máquina compartilhada (uma
  rodada por coluna, sem repetição). Elas ficam porque são pré-requisito de escala horizontal
  (partições ≥ instâncias), não por um ganho de latência medido aqui.
- **Resta uma cauda de ~3%.** No salto `TransferRequested → TransferReserved`, p50 = 12 ms,
  p90 = 68 ms, mas 3% das sagas esperam >800 ms (até 2,8 s), concentradas em poucas janelas de 2 s
  em que ~70% das mensagens atrasam juntas — ou seja, uma parada, não fila. **Causa não
  identificada.** Descartados com dados: GC (maior pausa 28 ms), *checkpoint* do Postgres (escrita
  diluída em minutos, `sync` de 9 ms), rebalance de consumer (nenhum após a subida), falha e retry
  do consumer (nenhuma) e tempo de processamento (p95 por etapa < 100 ms). Suspeita restante: a
  CPU da máquina chega a 80–85% com 4 JVMs, o gerador de carga e a infraestrutura Docker
  competindo no mesmo hardware. Só dá para confirmar rodando o gerador e a infraestrutura em
  máquinas separadas.

### Anatomia da latência (`anatomia-saga.mjs`)

Decompõe o tempo de cada saga usando só `created_at`/`published_at` do Outbox (sem
instrumentação): espera de publicação × espera de consumo, mediana × cauda, e o salto que mais
contribui. Foi com ela que o poller foi identificado como gargalo e que a cauda restante foi
localizada no primeiro salto.

```bash
node load-tests/anatomia-saga.mjs 2026-10-20T18:43:00Z 2026-10-20T18:52:00Z   # janela em UTC
```

### Lição de medição

O primeiro desenho do soak amostrava `docker stats`/`psql` de forma **síncrona** no mesmo
processo do gerador de carga. Isso travava o laço de eventos por segundos: a carga engasgava e
depois despejava o atrasado de uma vez (66 requisições no mesmo segundo), distorcendo o pico.
Tudo que roda durante a carga agora é assíncrono (`lib.mjs: psqlAsync/dockerAsync`), e cada ciclo
reporta `atrasoGerador`, marcando "MEDIÇÃO NÃO CONFIÁVEL" se passar de 1 s.

## Limites (leia antes de tirar conclusões)

- Mede **este** ambiente: tudo numa máquina só (4 JVMs + infraestrutura), Docker Desktop,
  tracing a 100%. Os números absolutos não se transportam para produção; o que se transporta é
  a *forma* da curva e onde ela quebra.
- O gerador de carga roda na mesma máquina e compete por CPU com o que mede.
- Uma instância de cada serviço. Não testa escala horizontal em carga: a exclusão mútua entre
  pollers de Outbox de várias instâncias é provada em teste de integração
  (`OutboxClaimConcurrencyIT`), não aqui.
- As sagas ficam no banco depois do teste (e aparecem no Grafana). Para um ambiente limpo,
  recrie os volumes do `docker compose`.

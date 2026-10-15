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

**Válvula de segurança:** o Jaeger all-in-one guarda traces em memória sem limite (~120 KB por
trace medido aqui). Acima de `MAX_JAEGER_MB` (2600) a carga para e o monitoramento continua.

### Resultado de referência (7 ciclos, 1h40, 7.560 sagas — ambiente de dev)

Sustentado (1,5 tx/s): p50 ≈ 2,2 s, p95 ≈ 2,9–3,1 s, sem deriva. Pico (12 tx/s × 30 s): p95
11–14 s, voltando a zero pendente em 12–14 s. Piso de heap plano nos 4 serviços. 7.560 sagas →
7.560 notificações, 0 duplicadas, 0 dead-letter, 0 erros 5xx. A latência da saga é dominada por
esperas (poller do Outbox a cada 500 ms × ~8 saltos), não por processamento: as JVMs ficaram em
1–6% de CPU. O joelho de capacidade, estimado pelo crescimento da fila, fica em ~9 tx/s.

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
- Uma instância de cada serviço. Não testa escala horizontal nem concorrência entre pollers de
  Outbox de várias instâncias.
- As sagas ficam no banco depois do teste (e aparecem no Grafana). Para um ambiente limpo,
  recrie os volumes do `docker compose`.

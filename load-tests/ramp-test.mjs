// Teste de carga com rampa para o banco-digital-saga (Node 18+, sem dependencias).
//
// Modelo "aberto" (taxa de chegada constante por estagio): as requisicoes sao disparadas no
// ritmo planejado MESMO que o sistema fique lento — e assim que a latencia degrada e a fila
// cresce, como em producao. Um modelo "fechado" (N usuarios esperando resposta) mascararia a
// saturacao, porque o gerador desaceleraria junto com o sistema.
//
// O que mede, por estagio de taxa:
//   1. Cliente: latencia do POST /api/transfers (o 202), codigos de erro, taxa efetivamente enviada.
//   2. Saga ponta a ponta: lida de saga_instances (updated_at - created_at das COMPLETED) DEPOIS
//      do teste — sem polling, para a medicao nao adicionar carga ao que esta sendo medido.
//   3. Fila: sagas ainda nao terminais ao fim do dreno (backlog).
//   4. Servidor: p95 por etapa da saga (Prometheus), avaliado ao fim de cada estagio.
//
// Uso (ver load-tests/README.md):
//   node load-tests/ramp-test.mjs
//   STAGES="1:30,2:60,4:60" ACCOUNTS=50 node load-tests/ramp-test.mjs     # rodada curta

import { mkdirSync, writeFileSync } from 'node:fs';
import {
  criarGerador, env, fmt, gerarToken, idDaConta, percentil, prom, psql, rodarAberto, sleep,
} from './lib.mjs';

const cfg = {
  transferUrl: env('TRANSFER_URL', 'http://localhost:8082'),
  accountUrl: env('ACCOUNT_URL', 'http://localhost:8081'),
  prometheusUrl: env('PROMETHEUS_URL', 'http://localhost:9090'),
  // Mesmo default de dev dos application.yml — nao e segredo de producao.
  jwtSecret: env('JWT_SECRET', 'banco-digital-saga-jwt-secret-desenvolvimento-nao-usar-em-producao'),
  accounts: Number(env('ACCOUNTS', '200')),
  // "taxa(tx/s):duracao(s)" separados por virgula; o primeiro estagio e o aquecimento da JVM.
  stages: env('STAGES', '1:30,2:60,4:60,6:60,8:60,10:60,12:60')
    .split(',')
    .map((s, i) => {
      const [rate, seconds] = s.split(':').map(Number);
      return { index: i, rate, seconds, warmup: i === 0 };
    }),
  drainSeconds: Number(env('DRAIN_SECONDS', '30')),
  abortErrorRate: Number(env('ABORT_ERROR_RATE', '0.25')),
  abortP95Ms: Number(env('ABORT_P95_MS', '5000')),
  pgContainer: env('PG_TRANSFER_CONTAINER', 'bank-postgres-transfer'),
  pgUser: env('PG_TRANSFER_USER', 'transfer'),
  pgDb: env('PG_TRANSFER_DB', 'transfer_db'),
};

const sqlTransfer = (sql) => psql(cfg.pgContainer, cfg.pgUser, cfg.pgDb, sql);

async function preVoo() {
  for (const [nome, url] of [
    ['account-service', `${cfg.accountUrl}/actuator/health`],
    ['transfer-service', `${cfg.transferUrl}/actuator/health`],
    ['prometheus', `${cfg.prometheusUrl}/-/ready`],
  ]) {
    try {
      const r = await fetch(url, { signal: AbortSignal.timeout(3000) });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
    } catch (e) {
      throw new Error(`Pre-voo falhou: ${nome} indisponivel em ${url} (${e.message}).`);
    }
  }
  const id = idDaConta(1);
  const r = await fetch(`${cfg.accountUrl}/api/accounts/${id}/balance`, {
    headers: { Authorization: `Bearer ${gerarToken(id, cfg.jwtSecret)}` },
    signal: AbortSignal.timeout(5000),
  });
  if (r.status === 404) {
    throw new Error('Contas de carga nao existem. Rode load-tests/seed-contas-carga.sql (ver README).');
  }
  if (!r.ok) {
    throw new Error(`Token gerado nao foi aceito pelo account-service (HTTP ${r.status}). Confira JWT_SECRET.`);
  }
}

const resultados = []; // { tag (indice do estagio), ms, status, erro }

function estatisticasCliente(stage) {
  const r = resultados.filter((x) => x.tag === stage.index);
  const ok = r.filter((x) => x.status === 202);
  const lat = ok.map((x) => x.ms).sort((a, b) => a - b);
  const por = (cond) => r.filter(cond).length;
  return {
    enviadas: stage.enviadas,
    respondidas: r.length,
    ok202: ok.length,
    http429: por((x) => x.status === 429),
    http422: por((x) => x.status === 422),
    http5xx: por((x) => x.status >= 500),
    timeouts: por((x) => x.status === 0),
    p50: percentil(lat, 50),
    p95: percentil(lat, 95),
    p99: percentil(lat, 99),
    max: lat.length ? lat[lat.length - 1] : null,
    taxaErro: r.length ? 1 - ok.length / r.length : 0,
  };
}

function analiseSagas(stage) {
  const janela = `created_at >= to_timestamp(${stage.inicioMs}/1000.0) AND created_at < to_timestamp(${stage.fimMs}/1000.0)
                  AND account_origem_id::text LIKE '00000000-0000-4000-8000-%'`;
  const dur = 'extract(epoch FROM updated_at - created_at) * 1000';
  const q = `
    SELECT count(*),
           count(*) FILTER (WHERE status = 'COMPLETED'),
           count(*) FILTER (WHERE status = 'FAILED'),
           count(*) FILTER (WHERE status NOT IN ('COMPLETED','FAILED')),
           round((percentile_cont(0.5)  WITHIN GROUP (ORDER BY ${dur}) FILTER (WHERE status='COMPLETED'))::numeric),
           round((percentile_cont(0.95) WITHIN GROUP (ORDER BY ${dur}) FILTER (WHERE status='COMPLETED'))::numeric),
           round((percentile_cont(0.99) WITHIN GROUP (ORDER BY ${dur}) FILTER (WHERE status='COMPLETED'))::numeric),
           round((max(${dur}) FILTER (WHERE status='COMPLETED'))::numeric)
    FROM saga_instances WHERE ${janela}`;
  const [total, completed, failed, pendentes, p50, p95, p99, max] = sqlTransfer(q).split('|');
  const num = (v) => (v === '' ? null : Number(v));
  const motivos = sqlTransfer(
    `SELECT coalesce(failure_reason,'(sem motivo)'), count(*) FROM saga_instances
     WHERE ${janela} AND status = 'FAILED' GROUP BY 1 ORDER BY 2 DESC`,
  )
    .split('\n')
    .filter(Boolean)
    .map((l) => l.split('|').join('='));
  return {
    total: Number(total),
    completed: Number(completed),
    failed: Number(failed),
    pendentes: Number(pendentes),
    p50: num(p50),
    p95: num(p95),
    p99: num(p99),
    max: num(max),
    motivos,
    // Coorte: sagas CRIADAS no estagio que terminaram COMPLETED, por segundo de estagio. Contar
    // conclusoes pela janela de tempo (updated_at) subestimaria a vazao nas bordas: com uma saga
    // levando ~2,5s, as ultimas ~2,5s * taxa sagas do estagio ainda estao em voo ao fim da janela.
    conclusoesPorSeg: Number(completed) / ((stage.fimMs - stage.inicioMs) / 1000),
  };
}

async function p95PorEtapa(stage) {
  const instante = (stage.fimMs + 10_000) / 1000;
  const r = await prom(
    cfg.prometheusUrl,
    'histogram_quantile(0.95, sum by (le, step) (rate(saga_step_duration_seconds_bucket[30s])))',
    instante,
  );
  const mapa = {};
  for (const s of r) {
    const v = Number(s.value[1]);
    if (!Number.isNaN(v)) mapa[s.metric.step] = v * 1000;
  }
  return mapa;
}

function relatorio(executados) {
  const L = [];
  L.push(`# Teste de carga com rampa — ${new Date().toISOString()}`);
  L.push('');
  L.push(`Contas de origem em rodizio: ${cfg.accounts} | estagios planejados: ` +
    cfg.stages.map((s) => `${s.rate}tx/s×${s.seconds}s`).join(', '));
  L.push('');
  L.push('## 1. Cliente — POST /api/transfers (a resposta 202)');
  L.push('');
  L.push('| Estagio | tx/s | enviadas | 202 | 429 | 422 | 5xx | timeout | p50 ms | p95 ms | p99 ms | max ms |');
  L.push('|---|---|---|---|---|---|---|---|---|---|---|---|');
  for (const s of executados) {
    const c = s.cliente;
    L.push(`| ${s.warmup ? 'aquecimento' : s.index} | ${s.rate} | ${c.enviadas} | ${c.ok202} | ${c.http429} | ${c.http422} | ${c.http5xx} | ${c.timeouts} | ${fmt(c.p50)} | ${fmt(c.p95)} | ${fmt(c.p99)} | ${fmt(c.max)} |`);
  }
  L.push('');
  L.push('## 2. Saga ponta a ponta (saga_instances: updated_at − created_at das COMPLETED)');
  L.push('');
  L.push('| Estagio | tx/s oferecidas | criadas | COMPLETED | FAILED | pendentes | concluidas/s | p50 ms | p95 ms | p99 ms | max ms |');
  L.push('|---|---|---|---|---|---|---|---|---|---|---|');
  for (const s of executados) {
    const a = s.sagas;
    L.push(`| ${s.warmup ? 'aquecimento' : s.index} | ${s.rate} | ${a.total} | ${a.completed} | ${a.failed} | ${a.pendentes} | ${fmt(a.conclusoesPorSeg, 1)} | ${fmt(a.p50)} | ${fmt(a.p95)} | ${fmt(a.p99)} | ${fmt(a.max)} |`);
  }
  const motivos = executados.filter((s) => s.sagas.motivos.length);
  if (motivos.length) {
    L.push('');
    L.push('Motivos de falha: ' + motivos.map((s) => `estagio ${s.index}: ${s.sagas.motivos.join(', ')}`).join(' | '));
  }
  L.push('');
  L.push('## 3. Servidor — p95 por etapa (Prometheus, janela de 30s ao fim de cada estagio), em ms');
  L.push('');
  const etapas = ['reserve', 'antifraud_check', 'debit', 'credit', 'notify',
    'orchestrate.reserve_ack', 'orchestrate.antifraud_result', 'orchestrate.debit_ack', 'orchestrate.credit_result'];
  L.push('| Estagio | ' + etapas.join(' | ') + ' |');
  L.push('|---|' + etapas.map(() => '---').join('|') + '|');
  for (const s of executados) {
    L.push(`| ${s.warmup ? 'aquecimento' : s.index} (${s.rate} tx/s) | ` + etapas.map((e) => fmt(s.etapas[e])).join(' | ') + ' |');
  }
  return L.join('\n');
}

// Heuristica explicita (nao um oraculo): estagios em que alguma condicao de saturacao aparece.
// Os criterios sao impressos junto para o leitor julgar.
function diagnostico(executados) {
  const base = executados.find((s) => !s.warmup && s.sagas.p95 !== null);
  const criterios = [];
  for (const s of executados.filter((x) => !x.warmup)) {
    const motivos = [];
    if (s.cliente.taxaErro > 0.05) motivos.push(`${fmt(s.cliente.taxaErro * 100, 1)}% de respostas != 202`);
    if (s.sagas.total > 0 && s.sagas.pendentes / s.sagas.total > 0.05) {
      motivos.push(`${s.sagas.pendentes}/${s.sagas.total} sagas ainda pendentes apos o dreno`);
    }
    if (s.sagas.total > 0 && s.sagas.failed / s.sagas.total > 0.05) {
      motivos.push(`${s.sagas.failed}/${s.sagas.total} sagas terminaram FAILED (ver motivos)`);
    }
    if (base && s.sagas.p95 !== null && s.sagas.p95 > 2 * base.sagas.p95) {
      motivos.push(`p95 da saga ${fmt(s.sagas.p95)} ms > 2x o do primeiro estagio medido (${fmt(base.sagas.p95)} ms)`);
    }
    if (motivos.length) criterios.push({ estagio: s.index, rate: s.rate, motivos });
  }
  return criterios;
}

async function main() {
  console.log('== Pre-voo ==');
  await preVoo();
  const ids = Array.from({ length: cfg.accounts }, (_, i) => idDaConta(i + 1));
  const tokens = ids.map((id) => gerarToken(id, cfg.jwtSecret));
  const gerador = criarGerador({ transferUrl: cfg.transferUrl, ids, tokens, onResultado: (r) => resultados.push(r) });
  const totalPlanejado = cfg.stages.reduce((n, s) => n + s.rate * s.seconds, 0);
  console.log(`OK. ${cfg.accounts} contas, ${cfg.stages.length} estagios, ~${totalPlanejado} transferencias planejadas.`);

  const executados = [];
  const inicioGeral = new Date();
  for (const stage of cfg.stages) {
    console.log(`\n== Estagio ${stage.index}${stage.warmup ? ' (aquecimento)' : ''}: ${stage.rate} tx/s por ${stage.seconds}s ==`);
    stage.inicioMs = Date.now();
    stage.enviadas = await rodarAberto(stage.rate, stage.seconds, () => gerador.disparar(stage.index));
    stage.fimMs = Date.now();
    await sleep(1500); // deixa as ultimas respostas do estagio chegarem
    const c = estatisticasCliente(stage);
    console.log(`   enviadas=${c.enviadas} 202=${c.ok202} 429=${c.http429} 5xx=${c.http5xx} timeout=${c.timeouts} p95=${fmt(c.p95)}ms emVoo=${gerador.emVoo}`);
    executados.push({ ...stage, cliente: c });
    if (!stage.warmup && (c.taxaErro > cfg.abortErrorRate || (c.p95 !== null && c.p95 > cfg.abortP95Ms))) {
      console.log(`   >> Interrompendo a rampa: erro=${fmt(c.taxaErro * 100, 1)}% p95=${fmt(c.p95)}ms (limites: ${cfg.abortErrorRate * 100}% / ${cfg.abortP95Ms}ms).`);
      break;
    }
  }

  console.log(`\n== Dreno: aguardando ${cfg.drainSeconds}s para as sagas em andamento terminarem ==`);
  await sleep(cfg.drainSeconds * 1000);

  console.log('== Analise (banco + Prometheus) ==');
  for (const s of executados) {
    s.sagas = analiseSagas(s);
    s.etapas = await p95PorEtapa(s);
  }
  const diag = diagnostico(executados);
  let texto = relatorio(executados) + '\n\n## 4. Diagnostico (heuristica)\n\n';
  texto += '_Criterios: >5% de respostas != 202; >5% de sagas pendentes apos o dreno; >5% de sagas FAILED; p95 da saga >2x o do primeiro estagio medido._\n\n';
  texto += diag.length
    ? diag.map((d) => `- **Estagio ${d.estagio} (${d.rate} tx/s)**: ${d.motivos.join('; ')}`).join('\n')
    : '- Nenhum criterio de saturacao foi atingido nos estagios executados — a rampa nao chegou ao limite.';
  texto += `\n\nInicio: ${inicioGeral.toISOString()} | Fim: ${new Date().toISOString()}\n`;
  console.log('\n' + texto);

  const carimbo = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const pasta = new URL('./results/', import.meta.url);
  mkdirSync(pasta, { recursive: true });
  writeFileSync(new URL(`ramp-${carimbo}.md`, pasta), texto);
  writeFileSync(new URL(`ramp-${carimbo}.json`, pasta), JSON.stringify(executados, null, 2));
  console.log(`\nRelatorio salvo em load-tests/results/ramp-${carimbo}.md`);
}

main().catch((e) => {
  console.error('ERRO:', e.message);
  process.exit(1);
});

// Teste de longa duracao (soak) + monitoramento do banco-digital-saga (Node 18+, sem dependencias).
//
// Objetivo: nao e medir o pico (isso e o ramp-test.mjs), e ver o que so aparece com o TEMPO —
// vazamento de memoria/threads, deriva de latencia, fila crescendo, sagas que travam, DLQ
// enchendo — e quanto o sistema leva para se recuperar de um pico.
//
// Ciclos repetidos ate o horario-limite (DEADLINE, padrao 14:00 locais):
//   sustentado : SUSTAIN_RATE tx/s por SUSTAIN_SECONDS (1 em cada 10 e cenario de falha rotativo:
//                limite excedido, blacklist, conta bloqueada — mantem os caminhos de compensacao vivos)
//   pico       : PEAK_RATE tx/s por PEAK_SECONDS (acima do joelho medido na rampa)
//   recuperacao: sem carga; mede quanto tempo ate nao haver saga pendente nem Outbox acumulado
//   ocioso     : resto do ciclo
//
// Amostra a cada SAMPLE_SECONDS: heap/CPU/threads/pool/erros por servico, saude, sagas pendentes e
// presas, Outbox pendente por servico, memoria do Jaeger e do Kafka. Imprime linhas ALERTA/CICLO/FIM
// (ponto de integracao com o Monitor) e grava timeline em load-tests/results/.
//
// Valvula de seguranca: o Jaeger all-in-one guarda traces em memoria sem limite; se passar de
// MAX_JAEGER_MB, a carga PARA (o monitoramento continua) para nao derrubar o Docker.

import { mkdirSync, writeFileSync, appendFileSync } from 'node:fs';
import {
  criarGerador, docker, dockerAsync, env, fmt, gerarToken, idDaConta, prom, promValor, psql, psqlAsync,
  rodarAberto, sleep,
} from './lib.mjs';

const cfg = {
  transferUrl: env('TRANSFER_URL', 'http://localhost:8082'),
  accountUrl: env('ACCOUNT_URL', 'http://localhost:8081'),
  prometheusUrl: env('PROMETHEUS_URL', 'http://localhost:9090'),
  jwtSecret: env('JWT_SECRET', 'banco-digital-saga-jwt-secret-desenvolvimento-nao-usar-em-producao'),
  accounts: Number(env('ACCOUNTS', '200')),
  sustainRate: Number(env('SUSTAIN_RATE', '1')),
  sustainSeconds: Number(env('SUSTAIN_SECONDS', '480')),
  peakRate: Number(env('PEAK_RATE', '12')),
  peakSeconds: Number(env('PEAK_SECONDS', '30')),
  cycleSeconds: Number(env('CYCLE_SECONDS', '900')),
  recoveryMaxSeconds: Number(env('RECOVERY_MAX_SECONDS', '240')),
  sampleSeconds: Number(env('SAMPLE_SECONDS', '30')),
  maxJaegerMb: Number(env('MAX_JAEGER_MB', '2600')),
  finalDrainSeconds: Number(env('FINAL_DRAIN_SECONDS', '60')),
  pg: {
    account: { c: 'bank-postgres-account', u: 'account', d: 'account_db' },
    transfer: { c: 'bank-postgres-transfer', u: 'transfer', d: 'transfer_db' },
    antifraud: { c: 'bank-postgres-antifraud', u: 'antifraud', d: 'antifraud_db' },
  },
  mongoContainer: 'bank-mongo-notification',
  services: { 'account-service': 8081, 'transfer-service': 8082, 'antifraud-service': 8083, 'notification-service': 8084 },
};

const inicio = new Date();
const deadline = (() => {
  if (process.env.DURATION_MIN) return new Date(Date.now() + Number(process.env.DURATION_MIN) * 60_000);
  const [h, m] = env('DEADLINE', '14:00').split(':').map(Number);
  const d = new Date();
  d.setHours(h, m, 0, 0);
  return d;
})();
const COORTE = `account_origem_id::text LIKE '00000000-0000-4000-8000-%' AND created_at >= to_timestamp(${inicio.getTime()}/1000.0)`;
const sqlPg = (k, sql) => psql(cfg.pg[k].c, cfg.pg[k].u, cfg.pg[k].d, sql);
const hhmmss = (d = new Date()) => d.toLocaleTimeString('pt-BR', { hour12: false });
const passouDoPrazo = () => Date.now() >= deadline.getTime();

// ---------------------------------------------------------------- estado
let fase = 'iniciando';
let pausado = false; // valvula: para de enviar, continua monitorando
let faltaDeResposta = 0;
const resultados = [];
const timeline = [];
const alertasEmitidos = new Map();
let jaegerMb = 0;

function alerta(chave, msg, reemitirMin = 10) {
  const ultimo = alertasEmitidos.get(chave) ?? 0;
  if (Date.now() - ultimo < reemitirMin * 60_000) return;
  alertasEmitidos.set(chave, Date.now());
  console.log(`ALERTA [${hhmmss()}] ${msg}`);
}

// ---------------------------------------------------------------- amostragem
async function porJob(query) {
  const r = await prom(cfg.prometheusUrl, query);
  const m = {};
  for (const s of r) m[s.metric.job] = Number(s.value[1]);
  return m;
}

// Tudo que roda durante a carga e ASSINCRONO (ver comentario em lib.mjs): senao o amostrador
// trava o laco de eventos do gerador e distorce justamente o pico que se quer medir.
async function memDocker() {
  const out = {};
  try {
    for (const l of (await dockerAsync(['stats', '--no-stream', '--format', '{{.Name}}|{{.MemUsage}}'])).split('\n')) {
      const [nome, uso] = l.split('|');
      const m = /([\d.]+)\s*(KiB|MiB|GiB)/.exec(uso ?? '');
      if (m) out[nome] = Number(m[1]) * { KiB: 1 / 1024, MiB: 1, GiB: 1024 }[m[2]];
    }
  } catch {
    /* docker indisponivel nesta amostra */
  }
  return out;
}

const sqlPgAsync = (k, sql) => psqlAsync(cfg.pg[k].c, cfg.pg[k].u, cfg.pg[k].d, sql);

async function estadoSagasEOutbox() {
  const chaves = ['account', 'transfer', 'antifraud'];
  const [s, ...obs] = await Promise.all([
    sqlPgAsync('transfer', `
      SELECT count(*) FILTER (WHERE status NOT IN ('COMPLETED','FAILED')),
             count(*) FILTER (WHERE status NOT IN ('COMPLETED','FAILED') AND updated_at < now() - interval '150 seconds')
      FROM saga_instances WHERE ${COORTE}`),
    ...chaves.map((k) => sqlPgAsync(k, `SELECT count(*) FROM outbox_events WHERE status = 'PENDING'`)),
  ]);
  const [pendentes, presas] = s.split('|').map(Number);
  const ob = {};
  chaves.forEach((k, i) => {
    ob[k] = Number(obs[i]);
  });
  return { pendentes, presas, outbox: ob };
}

async function dlts() {
  try {
    const l = await dockerAsync(['exec', 'bank-kafka', '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'localhost:9092', '--list']);
    return l.split('\n').filter((t) => t.endsWith('-dlt'));
  } catch {
    return null;
  }
}

async function amostrar() {
  const saude = {};
  await Promise.all(
    Object.entries(cfg.services).map(async ([nome, porta]) => {
      try {
        const r = await fetch(`http://localhost:${porta}/actuator/health`, { signal: AbortSignal.timeout(3000) });
        saude[nome] = r.ok;
      } catch {
        saude[nome] = false;
      }
    }),
  );
  const [heap, cpu, threads, poolPend, erros, http5xx, gc] = await Promise.all([
    porJob('sum by (job) (jvm_memory_used_bytes{area="heap"})'),
    porJob('process_cpu_usage'),
    porJob('jvm_threads_live_threads'),
    porJob('max by (job) (hikaricp_connections_pending)'),
    porJob('sum by (job) (increase(logback_events_total{level="error"}[1m]))'),
    porJob('sum by (job) (increase(http_server_requests_seconds_count{status=~"5.."}[1m]))'),
    porJob('rate(jvm_gc_pause_seconds_sum[1m])'),
  ]);
  const cpuSys = await promValor(cfg.prometheusUrl, 'avg(system_cpu_usage)');
  const [e, mem] = await Promise.all([estadoSagasEOutbox(), memDocker()]);
  jaegerMb = mem['bank-jaeger'] ?? 0;
  const am = {
    t: new Date().toISOString(), fase, emVoo: gerador.emVoo, saude,
    heapMb: Object.fromEntries(Object.entries(heap).map(([k, v]) => [k, v / 1048576])),
    cpuProc: cpu, cpuSys, threads, poolPend, errosLog1m: erros, http5xx1m: http5xx, gcSegPorSeg: gc,
    pendentes: e.pendentes, presas: e.presas, outbox: e.outbox,
    jaegerMb, kafkaMb: mem['bank-kafka'] ?? null, mongoMb: mem['bank-mongo-notification'] ?? null,
  };
  timeline.push(am);
  appendFileSync(arquivoTimeline, JSON.stringify(am) + '\n');

  const h = (j) => fmt(am.heapMb[j]);
  console.log(
    `${hhmmss()} ${fase.padEnd(11)} voo=${String(am.emVoo).padStart(3)} heap(a/t/f/n)=${h('account-service')}/${h('transfer-service')}/${h('antifraud-service')}/${h('notification-service')}MB ` +
    `cpuSys=${fmt((cpuSys ?? 0) * 100)}% pend=${e.pendentes} presas=${e.presas} outbox(a/t/f)=${e.outbox.account}/${e.outbox.transfer}/${e.outbox.antifraud} ` +
    `jaeger=${fmt(jaegerMb)}MB kafka=${fmt(am.kafkaMb)}MB`,
  );

  // ---- alertas
  for (const [nome, ok] of Object.entries(saude)) if (!ok) alerta(`saude-${nome}`, `${nome} NAO esta UP no /actuator/health.`, 2);
  for (const [j, v] of Object.entries(http5xx)) if (v > 0) alerta(`5xx-${j}`, `${j}: ${fmt(v)} respostas 5xx no ultimo minuto.`, 5);
  for (const [j, v] of Object.entries(erros)) if (v > 5) alerta(`log-${j}`, `${j}: ${fmt(v)} logs ERROR no ultimo minuto.`, 5);
  for (const [j, v] of Object.entries(poolPend)) if (v > 0) alerta(`pool-${j}`, `${j}: fila no pool de conexoes (pending=${v}).`, 5);
  if (e.presas > 0) alerta('presas', `${e.presas} sagas sem progresso ha >150s (acima do timeout de 120s) — a compensacao por timeout nao agiu.`, 5);
  const ob = Object.entries(e.outbox).filter(([, n]) => n > 100);
  if (ob.length && fase !== 'pico' && fase !== 'recuperacao') {
    alerta('outbox', `Outbox acumulado fora do pico: ${ob.map(([k, n]) => `${k}=${n}`).join(', ')}.`, 5);
  }
  const lista = await dlts();
  if (lista && lista.length) alerta('dlt', `Topicos de dead-letter existem: ${lista.join(', ')}.`, 30);
  if (jaegerMb > cfg.maxJaegerMb && !pausado) {
    pausado = true;
    alerta('valvula', `Jaeger em ${fmt(jaegerMb)} MB (> ${cfg.maxJaegerMb}). Valvula acionada: a CARGA foi pausada; o monitoramento continua.`, 1);
  }
  return am;
}

// ---------------------------------------------------------------- analise por janela
function analiseJanela(iniMs, fimMs) {
  const w = `account_origem_id::text LIKE '00000000-0000-4000-8000-%'
             AND created_at >= to_timestamp(${iniMs}/1000.0) AND created_at < to_timestamp(${fimMs}/1000.0)`;
  const dur = 'extract(epoch FROM updated_at - created_at) * 1000';
  const [n, ok, fail, pend, p50, p95, p99, max] = sqlPg('transfer', `
    SELECT count(*), count(*) FILTER (WHERE status='COMPLETED'), count(*) FILTER (WHERE status='FAILED'),
           count(*) FILTER (WHERE status NOT IN ('COMPLETED','FAILED')),
           round((percentile_cont(0.5)  WITHIN GROUP (ORDER BY ${dur}) FILTER (WHERE status='COMPLETED'))::numeric),
           round((percentile_cont(0.95) WITHIN GROUP (ORDER BY ${dur}) FILTER (WHERE status='COMPLETED'))::numeric),
           round((percentile_cont(0.99) WITHIN GROUP (ORDER BY ${dur}) FILTER (WHERE status='COMPLETED'))::numeric),
           round((max(${dur}) FILTER (WHERE status='COMPLETED'))::numeric)
    FROM saga_instances WHERE ${w}`).split('|');
  const v = (x) => (x === '' ? null : Number(x));
  return { n: Number(n), ok: Number(ok), fail: Number(fail), pend: Number(pend), p50: v(p50), p95: v(p95), p99: v(p99), max: v(max) };
}

async function pisoDeHeap(iniMs, fimMs) {
  // menor uso de heap observado na janela por servico (~conjunto vivo apos GC) — tendencia de vazamento
  const dur = Math.max(60, Math.round((fimMs - iniMs) / 1000));
  const r = await prom(
    cfg.prometheusUrl,
    `min_over_time(sum by (job) (jvm_memory_used_bytes{area="heap"})[${dur}s:15s])`,
    fimMs / 1000,
  );
  const m = {};
  for (const s of r) m[s.metric.job] = Number(s.value[1]) / 1048576;
  return m;
}

// ---------------------------------------------------------------- carga
const ids = Array.from({ length: cfg.accounts }, (_, i) => idDaConta(i + 1));
const tokens = ids.map((id) => gerarToken(id, cfg.jwtSecret));
const gerador = criarGerador({ transferUrl: cfg.transferUrl, ids, tokens, onResultado: (r) => resultados.push(r) });
const ANA_BLACKLIST = '11111111-1111-1111-1111-111111111111';
const HUGO_BLOQUEADO = 'dddddddd-dddd-dddd-dddd-dddddddddddd';
let contadorMix = 0;

function dispararComMix(tag) {
  contadorMix++;
  if (contadorMix % 10 === 0) {
    const tipo = (contadorMix / 10) % 3;
    if (tipo === 0) return gerador.disparar(tag, { valor: 10500 });
    if (tipo === 1) return gerador.disparar(tag, { destinoId: ANA_BLACKLIST });
    return gerador.disparar(tag, { destinoId: HUGO_BLOQUEADO });
  }
  return gerador.disparar(tag);
}

function taxaErroDaFase(tag) {
  const r = resultados.filter((x) => x.tag === tag);
  return { n: r.length, erro: r.length ? r.filter((x) => x.status !== 202).length / r.length : 0 };
}

async function recuperar(aPartirDe) {
  // tempo ate nao haver saga pendente (coorte) nem Outbox pendente em nenhum servico
  const t0 = Date.now();
  while (Date.now() - t0 < cfg.recoveryMaxSeconds * 1000) {
    const e = await estadoSagasEOutbox();
    if (e.pendentes === 0 && e.outbox.account + e.outbox.transfer + e.outbox.antifraud === 0) {
      return (Date.now() - aPartirDe) / 1000;
    }
    await sleep(2000);
  }
  return null; // nao recuperou dentro do limite
}

const ciclos = [];
const arquivoTimeline = new URL(`./results/soak-${inicio.toISOString().replace(/[:.]/g, '-').slice(0, 19)}.jsonl`, import.meta.url);

async function ciclo(numero) {
  const cInicio = Date.now();
  const tag = `c${numero}`;

  let atrasoS = 0, atrasoP = 0;
  fase = 'sustentado';
  const sIni = Date.now();
  await rodarAberto(cfg.sustainRate, cfg.sustainSeconds, () => dispararComMix(`${tag}s`), () => pausado || passouDoPrazo(),
    (ms) => { atrasoS = Math.max(atrasoS, ms); });
  const sFim = Date.now();

  let pIni = null, pFim = null;
  if (!pausado && !passouDoPrazo()) {
    fase = 'pico';
    pIni = Date.now();
    await rodarAberto(cfg.peakRate, cfg.peakSeconds, () => dispararComMix(`${tag}p`), () => pausado || passouDoPrazo(),
      (ms) => { atrasoP = Math.max(atrasoP, ms); });
    pFim = Date.now();
  }

  fase = 'recuperacao';
  const refRec = pFim ?? sFim;
  await sleep(2000);
  const recuperacaoS = await recuperar(refRec);

  fase = pausado ? 'pausado' : 'ocioso';
  while (Date.now() - cInicio < cfg.cycleSeconds * 1000 && !passouDoPrazo()) await sleep(1000);

  // ---- resumo do ciclo
  const sus = analiseJanela(sIni, sFim);
  const pico = pIni ? analiseJanela(pIni, pFim) : null;
  const piso = await pisoDeHeap(cInicio, Date.now());
  const errS = taxaErroDaFase(`${tag}s`), errP = taxaErroDaFase(`${tag}p`);
  const c = { numero, inicio: new Date(cInicio).toISOString(), sustentado: sus, pico, recuperacaoS, pisoHeapMb: piso,
    nao202Sust: errS.erro, nao202Pico: errP.erro, atrasoGeradorMs: Math.max(atrasoS, atrasoP) };
  ciclos.push(c);
  const confiavel = c.atrasoGeradorMs <= 1000;
  console.log(
    `CICLO ${numero} [${hhmmss()}] sustentado n=${sus.n} p50=${fmt(sus.p50)} p95=${fmt(sus.p95)} p99=${fmt(sus.p99)}ms ` +
    `${pico ? `| pico n=${pico.n}/${cfg.peakRate * cfg.peakSeconds} p50=${fmt(pico.p50)} p95=${fmt(pico.p95)} max=${fmt(pico.max)}ms ` : '| pico (pulado) '}` +
    `| atrasoGerador=${fmt(c.atrasoGeradorMs)}ms${confiavel ? '' : ' (MEDICAO NAO CONFIAVEL)'} ` +
    `| recuperacao=${recuperacaoS === null ? `>${cfg.recoveryMaxSeconds}s (NAO RECUPEROU)` : fmt(recuperacaoS) + 's'} ` +
    `| pisoHeap(a/t/f/n)=${['account-service', 'transfer-service', 'antifraud-service', 'notification-service'].map((j) => fmt(piso[j])).join('/')}MB`,
  );

  // ---- tendencias vs. ciclo de referencia (o primeiro)
  const ref = ciclos[0];
  if (numero > 1 && ref) {
    for (const j of Object.keys(piso)) {
      const base = ref.pisoHeapMb[j];
      if (base && piso[j] > base * 1.25) alerta(`vaz-${j}`, `${j}: piso de heap ${fmt(piso[j])} MB e >25% acima do ciclo 1 (${fmt(base)} MB) — possivel vazamento.`, 30);
    }
    if (ref.sustentado.p95 && sus.p95 && sus.p95 > 1.5 * ref.sustentado.p95) {
      alerta('deriva', `p95 da saga sustentada ${fmt(sus.p95)} ms e >1,5x o do ciclo 1 (${fmt(ref.sustentado.p95)} ms) — deriva de latencia.`, 30);
    }
  }
  if (recuperacaoS === null) alerta('recup', `Ciclo ${numero}: o sistema NAO voltou a zero pendente em ${cfg.recoveryMaxSeconds}s apos o pico.`, 5);
  if (!confiavel) alerta('gerador', `Ciclo ${numero}: o gerador de carga atrasou ${fmt(c.atrasoGeradorMs)} ms em relacao ao plano — os numeros do pico deste ciclo NAO sao confiaveis.`, 1);
  if (errS.erro > 0.05) alerta('erro-sust', `Ciclo ${numero}: ${fmt(errS.erro * 100, 1)}% de respostas != 202 na fase sustentada.`, 5);
}

// ---------------------------------------------------------------- relatorio final
function mongoEval(js) {
  return docker(['exec', cfg.mongoContainer, 'mongosh', '--quiet', 'notification_db', '--eval', js]);
}

async function relatorioFinal() {
  fase = 'dreno-final';
  await sleep(cfg.finalDrainSeconds * 1000);
  const total = sqlPg('transfer', `
    SELECT count(*), count(*) FILTER (WHERE status='COMPLETED'), count(*) FILTER (WHERE status='FAILED'),
           count(*) FILTER (WHERE status NOT IN ('COMPLETED','FAILED'))
    FROM saga_instances WHERE ${COORTE}`).split('|').map(Number);
  const motivos = sqlPg('transfer', `
    SELECT coalesce(failure_reason,'(sem motivo)'), count(*) FROM saga_instances
    WHERE ${COORTE} AND status='FAILED' GROUP BY 1 ORDER BY 2 DESC`).split('\n').filter(Boolean).map((l) => l.replace('|', '='));
  let notif = null, dup = null;
  try {
    const iso = inicio.toISOString();
    notif = Number(mongoEval(`print(db.notifications.countDocuments({createdAt: {$gte: ISODate("${iso}")}}))`));
    dup = Number(mongoEval(
      `print(db.notifications.aggregate([{$match:{createdAt:{$gte: ISODate("${iso}")}}},{$group:{_id:"$transferId",n:{$sum:1}}},{$match:{n:{$gt:1}}}]).toArray().length)`));
  } catch {
    /* mongosh indisponivel */
  }
  const terminais = total[1] + total[2];
  const fim = new Date();
  const L = [];
  L.push(`# Teste de longa duracao (soak) — ${inicio.toISOString()} a ${fim.toISOString()}`);
  L.push('');
  L.push(`Perfil por ciclo de ${cfg.cycleSeconds}s: sustentado ${cfg.sustainRate} tx/s x ${cfg.sustainSeconds}s (1 em 10 e cenario de falha), ` +
    `pico ${cfg.peakRate} tx/s x ${cfg.peakSeconds}s, depois recuperacao e ocioso. Ciclos executados: ${ciclos.length}.`);
  L.push('');
  L.push('## Ciclos');
  L.push('');
  L.push('| # | inicio | sust. n | sust. p50 | sust. p95 | sust. p99 | pico n | pico p95 | pico max | recuperacao (s) | piso heap a/t/f/n (MB) |');
  L.push('|---|---|---|---|---|---|---|---|---|---|---|');
  for (const c of ciclos) {
    const s = c.sustentado, p = c.pico;
    L.push(`| ${c.numero} | ${c.inicio.slice(11, 19)}Z | ${s.n} | ${fmt(s.p50)} | ${fmt(s.p95)} | ${fmt(s.p99)} | ${p ? p.n : '-'} | ${p ? fmt(p.p95) : '-'} | ${p ? fmt(p.max) : '-'} | ${c.recuperacaoS === null ? 'NAO' : fmt(c.recuperacaoS)} | ` +
      ['account-service', 'transfer-service', 'antifraud-service', 'notification-service'].map((j) => fmt(c.pisoHeapMb[j])).join('/') + ' |');
  }
  L.push('');
  L.push('## Integridade ao final');
  L.push('');
  L.push(`- Sagas criadas pela carga: **${total[0]}** | COMPLETED ${total[1]} | FAILED ${total[2]} | pendentes ${total[3]}`);
  L.push(`- Motivos de FAILED: ${motivos.length ? motivos.join(', ') : '(nenhum)'}`);
  L.push(`- Notificacoes no MongoDB no periodo: **${notif ?? 'n/d'}** (esperado: uma por saga terminal = ${terminais}) | transferId duplicado: **${dup ?? 'n/d'}**`);
  const lista = await dlts();
  L.push(`- Topicos de dead-letter: ${lista === null ? 'n/d' : lista.length ? lista.join(', ') : 'nenhum'}`);
  const heapPrimeiro = ciclos[0]?.pisoHeapMb ?? {}, heapUltimo = ciclos[ciclos.length - 1]?.pisoHeapMb ?? {};
  L.push('- Piso de heap (primeiro -> ultimo ciclo): ' + Object.keys(heapUltimo).map((j) => `${j} ${fmt(heapPrimeiro[j])} -> ${fmt(heapUltimo[j])} MB`).join(' | '));
  const maxJ = Math.max(0, ...timeline.map((t) => t.jaegerMb ?? 0));
  L.push(`- Pico de memoria do Jaeger: ${fmt(maxJ)} MB${pausado ? ' (VALVULA ACIONADA: a carga foi interrompida)' : ''}`);
  L.push(`- Amostras de monitoramento: ${timeline.length} (a cada ${cfg.sampleSeconds}s) em load-tests/results/*.jsonl`);
  const texto = L.join('\n') + '\n';
  const carimbo = inicio.toISOString().replace(/[:.]/g, '-').slice(0, 19);
  writeFileSync(new URL(`./results/soak-${carimbo}.md`, import.meta.url), texto);
  console.log(`FIM [${hhmmss()}] relatorio em load-tests/results/soak-${carimbo}.md`);
  console.log(texto);
}

// ---------------------------------------------------------------- main
async function main() {
  mkdirSync(new URL('./results/', import.meta.url), { recursive: true });
  const r = await fetch(`${cfg.accountUrl}/api/accounts/${ids[0]}/balance`, {
    headers: { Authorization: `Bearer ${tokens[0]}` }, signal: AbortSignal.timeout(5000),
  }).catch(() => null);
  if (!r || !r.ok) throw new Error('Pre-voo falhou: account-service indisponivel ou contas de carga ausentes (rode o seed).');
  console.log(`INICIO [${hhmmss(inicio)}] soak ate ${hhmmss(deadline)} | ciclo ${cfg.cycleSeconds}s | sustentado ${cfg.sustainRate} tx/s | pico ${cfg.peakRate} tx/s | valvula Jaeger ${cfg.maxJaegerMb}MB`);

  const timer = setInterval(() => amostrar().catch((e) => console.log(`amostragem falhou: ${e.message}`)), cfg.sampleSeconds * 1000);
  let n = 0;
  while (!passouDoPrazo()) await ciclo(++n);
  clearInterval(timer);
  await relatorioFinal();
}

main().catch((e) => {
  console.error('ERRO:', e.message);
  process.exit(1);
});

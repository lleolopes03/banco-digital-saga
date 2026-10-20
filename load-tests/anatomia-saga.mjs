// Anatomia da latência da saga: onde o tempo realmente se perde.
//
// Usa as colunas created_at/published_at do Outbox dos 3 serviços (nenhuma instrumentação extra):
//  - espera_publicacao = published_at - created_at   (fila no Outbox + poller + envio ao Kafka)
//  - espera_consumo    = created_at do próximo evento - published_at do anterior
//                        (entrega Kafka + consumer + processamento + gravação do próximo evento)
// Compara a mediana com a cauda (p95+) e diz qual salto domina a cauda.
//
// Uso (janela em UTC, a mesma do relatório da rampa):
//   node load-tests/anatomia-saga.mjs 2026-10-20T18:43:00Z 2026-10-20T18:52:00Z
// Requer os containers bank-postgres-{account,transfer,antifraud} no ar.
import { execFileSync } from 'node:child_process';

const [ini, fim] = [process.argv[2], process.argv[3]]; // ISO UTC
const bancos = [
  ['account', 'bank-postgres-account', 'account', 'account_db'],
  ['transfer', 'bank-postgres-transfer', 'transfer', 'transfer_db'],
  ['antifraud', 'bank-postgres-antifraud', 'antifraud', 'antifraud_db'],
];
const sagas = new Map();
for (const [nome, c, u, d] of bancos) {
  const sql = `SELECT aggregate_id, event_type, (extract(epoch from created_at)*1000)::bigint, ` +
    `coalesce((extract(epoch from published_at)*1000)::bigint,0) FROM outbox_events ` +
    `WHERE created_at >= '${ini}' AND created_at < '${fim}' AND event_type NOT IN ('DepositCompleted')`;
  const out = execFileSync('docker', ['exec', '-i', c, 'psql', '-U', u, '-d', d, '-At', '-F', '|', '-c', sql],
    { encoding: 'utf8', maxBuffer: 1 << 28 });
  for (const l of out.split('\n').filter(Boolean)) {
    const [id, tipo, cr, pub] = l.split('|');
    if (!sagas.has(id)) sagas.set(id, []);
    sagas.get(id).push({ nome, tipo, cr: Number(cr), pub: Number(pub) });
  }
}
const pct = (a, p) => { const s = [...a].sort((x, y) => x - y); return s[Math.min(s.length - 1, Math.floor(p * s.length))]; };
const linhas = [];
for (const [id, evs] of sagas) {
  evs.sort((a, b) => a.cr - b.cr);
  if (evs.length < 6 || evs.some((e) => !e.pub)) continue;
  const pubW = evs.map((e) => e.pub - e.cr);
  const gaps = evs.slice(1).map((e, i) => e.cr - evs[i].pub);
  const total = evs[evs.length - 1].pub - evs[0].cr;
  const maiorGap = Math.max(...gaps), i = gaps.indexOf(maiorGap);
  const maiorPub = Math.max(...pubW), j = pubW.indexOf(maiorPub);
  linhas.push({ id, total, somaPub: pubW.reduce((a, b) => a + b, 0), somaGap: gaps.reduce((a, b) => a + b, 0),
    maiorGap, ondeGap: `${evs[i].tipo}->${evs[i + 1].tipo}`, maiorPub, ondePub: evs[j].tipo });
}
linhas.sort((a, b) => a.total - b.total);
const n = linhas.length;
console.log(`sagas completas analisadas: ${n}`);
const grupo = (nome, ls) => {
  const m = (k) => Math.round(ls.reduce((a, l) => a + l[k], 0) / ls.length);
  console.log(`${nome} (n=${ls.length}): total medio=${m('total')} ms | soma espera_publicacao=${m('somaPub')} ms | soma espera_consumo=${m('somaGap')} ms | maior gap medio=${m('maiorGap')} ms | maior pub medio=${m('maiorPub')} ms`);
};
grupo('mediana (40-60%)', linhas.slice(Math.floor(n * 0.4), Math.floor(n * 0.6)));
grupo('cauda p95+', linhas.slice(Math.floor(n * 0.95)));
const cauda = linhas.slice(Math.floor(n * 0.95));
const cont = {};
for (const l of cauda) { const k = l.maiorGap > l.maiorPub ? `consumo ${l.ondeGap}` : `publicacao ${l.ondePub}`; cont[k] = (cont[k] || 0) + 1; }
console.log('maior contribuinte por saga da cauda:', JSON.stringify(cont, null, 1));
console.log('percentis total ms: p50=%d p95=%d p99=%d max=%d', pct(linhas.map((l) => l.total), 0.5),
  pct(linhas.map((l) => l.total), 0.95), pct(linhas.map((l) => l.total), 0.99), linhas[n - 1].total);
console.log('piores 5:'); for (const l of linhas.slice(-5)) console.log(JSON.stringify(l));

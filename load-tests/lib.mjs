// Utilitarios compartilhados por ramp-test.mjs e soak-test.mjs (Node 18+, sem dependencias).
import { createHmac, randomUUID } from 'node:crypto';
import { execFile, execFileSync } from 'node:child_process';
import { performance } from 'node:perf_hooks';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);

export const env = (k, d) => process.env[k] ?? d;
export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
export const fmt = (v, casas = 0) =>
  v === null || v === undefined || Number.isNaN(v) ? '-' : Number(v).toFixed(casas);

const b64url = (b) => Buffer.from(b).toString('base64url');

/** IDs deterministicos das contas semeadas por seed-contas-carga.sql (n a partir de 1). */
export const idDaConta = (n) => `00000000-0000-4000-8000-${n.toString(16).padStart(12, '0')}`;

/**
 * Token HS256 equivalente ao que o account-service emitiria no login para essa conta. Gerado
 * localmente (em vez de N logins + N usuarios) porque o objetivo e medir a saga, nao o BCrypt.
 */
export function gerarToken(accountId, secret) {
  const agora = Math.floor(Date.now() / 1000);
  const header = b64url(JSON.stringify({ alg: 'HS256', typ: 'JWT' }));
  const payload = b64url(
    JSON.stringify({ sub: randomUUID(), accountId, jti: randomUUID(), iat: agora, exp: agora + 6 * 3600 }),
  );
  const assinatura = createHmac('sha256', secret).update(`${header}.${payload}`).digest('base64url');
  return `${header}.${payload}.${assinatura}`;
}

export function percentil(ordenados, p) {
  if (ordenados.length === 0) return null;
  const i = Math.min(ordenados.length - 1, Math.ceil((p / 100) * ordenados.length) - 1);
  return ordenados[Math.max(0, i)];
}

export function docker(args) {
  return execFileSync('docker', args, { encoding: 'utf8', maxBuffer: 16 * 1024 * 1024 }).trim();
}

export function psql(container, user, db, sql) {
  return docker(['exec', '-i', container, 'psql', '-U', user, '-d', db, '-At', '-F', '|', '-c', sql]);
}

/**
 * Versoes ASSINCRONAS — obrigatorias para qualquer coisa que rode ENQUANTO o gerador de carga
 * esta enviando. Uma chamada sincrona (execFileSync) trava o laco de eventos do Node, e como o
 * gerador vive no mesmo processo, a carga "engasga" e depois despeja o atrasado de uma vez
 * (observado: 66 requisicoes no mesmo segundo apos um travamento de ~7s do `docker stats`).
 */
export async function dockerAsync(args) {
  const { stdout } = await execFileAsync('docker', args, { maxBuffer: 16 * 1024 * 1024 });
  return stdout.trim();
}

export function psqlAsync(container, user, db, sql) {
  return dockerAsync(['exec', '-i', container, 'psql', '-U', user, '-d', db, '-At', '-F', '|', '-c', sql]);
}

export async function prom(prometheusUrl, query, timeSeconds) {
  const t = timeSeconds ? `&time=${timeSeconds}` : '';
  try {
    const r = await fetch(`${prometheusUrl}/api/v1/query?query=${encodeURIComponent(query)}${t}`, {
      signal: AbortSignal.timeout(5000),
    });
    return (await r.json()).data.result;
  } catch {
    return [];
  }
}

/** Primeiro valor escalar de uma consulta instantanea (ou null). */
export async function promValor(prometheusUrl, query, timeSeconds) {
  const r = await prom(prometheusUrl, query, timeSeconds);
  if (!r.length) return null;
  const v = Number(r[0].value[1]);
  return Number.isNaN(v) ? null : v;
}

/**
 * Gerador de POST /api/transfers em rodizio entre as contas de carga. Cada resultado e entregue
 * a `onResultado({ tag, ms, status, erro })`. `valor` fica entre 1 e 50 (nunca aciona o limite de
 * R$ 10.000 do antifraude) e origem != destino sempre.
 */
export function criarGerador({ transferUrl, ids, tokens, timeoutMs = 30000, onResultado }) {
  let seq = 0;
  let emVoo = 0;
  const n = ids.length;
  return {
    get emVoo() {
      return emVoo;
    },
    /** `opcoes.valor` / `opcoes.destinoId` forcam um cenario (ex.: acima do limite, blacklist). */
    disparar(tag, opcoes = {}) {
      const i = seq++;
      const origem = i % n;
      let destino = (origem + 1 + (i % 7)) % n;
      if (destino === origem) destino = (destino + 1) % n;
      const corpo = JSON.stringify({
        accountOrigemId: ids[origem],
        accountDestinoId: opcoes.destinoId ?? ids[destino],
        valor: opcoes.valor ?? Number((1 + Math.random() * 49).toFixed(2)),
      });
      const inicio = performance.now();
      emVoo++;
      fetch(`${transferUrl}/api/transfers`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${tokens[origem]}`,
          'Idempotency-Key': randomUUID(),
        },
        body: corpo,
        signal: AbortSignal.timeout(timeoutMs),
      })
        .then(async (r) => {
          await r.arrayBuffer();
          onResultado({ tag, ms: performance.now() - inicio, status: r.status });
        })
        .catch((e) => onResultado({ tag, ms: performance.now() - inicio, status: 0, erro: e.name }))
        .finally(() => {
          emVoo--;
        });
    },
  };
}

/** Modelo aberto: dispara `taxa` tx/s por `segundos`, sem esperar as respostas. */
export async function rodarAberto(taxa, segundos, disparar, deveParar = () => false, onAtraso = () => {}) {
  const total = Math.round(taxa * segundos);
  const intervalo = 1000 / taxa;
  const t0 = performance.now();
  for (let i = 0; i < total; i++) {
    if (deveParar()) break;
    const espera = t0 + i * intervalo - performance.now();
    if (espera > 0) await sleep(espera);
    else onAtraso(-espera); // o gerador esta atrasado em relacao ao plano: validade da medicao
    disparar();
  }
  const resto = t0 + segundos * 1000 - performance.now();
  if (resto > 0) await sleep(resto);
  return total;
}

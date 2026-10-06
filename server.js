import 'dotenv/config';
import express from 'express';
import amqp from 'amqplib';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const PORT = Number(process.env.PORT || 3090);
const RABBITMQ_URL = process.env.RABBITMQ_URL;
const VHOST = process.env.RABBITMQ_VHOST || '/';
const ENV_LABEL = process.env.ENV_LABEL || '';
const PRESET_QUEUES = (process.env.PRESET_QUEUES || '')
  .split(',')
  .map((q) => q.trim())
  .filter(Boolean);

if (!RABBITMQ_URL) {
  console.error('Falta RABBITMQ_URL en .env (ver .env.example)');
  process.exit(1);
}

const amqpUrl = new URL(RABBITMQ_URL);
const maskedHost = `${amqpUrl.protocol}//${amqpUrl.hostname}:${amqpUrl.port || 5672}`;

function managementBase() {
  if (process.env.RABBITMQ_MGMT_URL) return process.env.RABBITMQ_MGMT_URL.replace(/\/$/, '');
  const proto = amqpUrl.protocol === 'amqps:' ? 'https:' : 'http:';
  return `${proto}//${amqpUrl.hostname}:15672`;
}

function managementAuth() {
  const user = decodeURIComponent(amqpUrl.username);
  const pass = decodeURIComponent(amqpUrl.password);
  return 'Basic ' + Buffer.from(`${user}:${pass}`).toString('base64');
}

// ---------- Conexión AMQP compartida ----------
let connection = null;
let connecting = null;

async function getConnection() {
  if (connection) return connection;
  if (connecting) return connecting;
  connecting = amqp
    .connect(RABBITMQ_URL, { clientProperties: { connection_name: 'rabbit-enqueuer' } })
    .then((conn) => {
      conn.on('close', () => (connection = null));
      conn.on('error', (err) => console.error('AMQP error:', err.message));
      connection = conn;
      return conn;
    })
    .finally(() => (connecting = null));
  return connecting;
}

// checkQueue cierra el canal si la cola no existe, por eso se usa un canal desechable.
async function queueInfo(queue) {
  const conn = await getConnection();
  const ch = await conn.createChannel();
  ch.on('error', () => {});
  try {
    return await ch.checkQueue(queue);
  } finally {
    await ch.close().catch(() => {});
  }
}

// ---------- App ----------
const app = express();
app.use(express.json({ limit: '50mb' }));
app.use(express.static(path.join(__dirname, 'public')));

app.get('/api/config', (_req, res) => {
  res.json({ host: maskedHost, vhost: VHOST, env: ENV_LABEL, presetQueues: PRESET_QUEUES });
});

app.get('/api/queues', async (_req, res) => {
  try {
    const url = `${managementBase()}/api/queues/${encodeURIComponent(VHOST)}?columns=name,messages,consumers`;
    const r = await fetch(url, {
      headers: { Authorization: managementAuth() },
      signal: AbortSignal.timeout(5000),
    });
    if (!r.ok) throw new Error(`Management API respondió ${r.status}`);
    const queues = await r.json();
    res.json({
      source: 'management',
      queues: queues
        .map((q) => ({ name: q.name, messages: q.messages ?? 0, consumers: q.consumers ?? 0 }))
        .sort((a, b) => a.name.localeCompare(b.name)),
    });
  } catch (err) {
    res.json({
      source: 'preset',
      warning: `No se pudo consultar la API de management (${err.message}). Se muestran las colas de PRESET_QUEUES.`,
      queues: PRESET_QUEUES.map((name) => ({ name })),
    });
  }
});

app.get('/api/queues/:name', async (req, res) => {
  try {
    const info = await queueInfo(req.params.name);
    res.json({ exists: true, name: info.queue, messages: info.messageCount, consumers: info.consumerCount });
  } catch (err) {
    const notFound = /NOT_FOUND/.test(err.message);
    res.status(notFound ? 404 : 502).json({
      exists: false,
      error: notFound ? `La cola "${req.params.name}" no existe` : `No se pudo conectar a ${maskedHost}: ${err.message}`,
    });
  }
});

/**
 * Publica un lote de mensajes en una cola (default exchange, routing key = nombre de la cola).
 * body: { queue, messages: object[], options: { resetRetryCount, refreshLastExecution, delayMs } }
 */
app.post('/api/publish', async (req, res) => {
  const { queue, messages, options = {} } = req.body ?? {};
  if (!queue || typeof queue !== 'string') return res.status(400).json({ error: 'queue es requerido' });
  if (!Array.isArray(messages) || messages.length === 0)
    return res.status(400).json({ error: 'messages debe ser un arreglo no vacío' });

  try {
    await queueInfo(queue);
  } catch (err) {
    return /NOT_FOUND/.test(err.message)
      ? res.status(404).json({ error: `La cola "${queue}" no existe en ${maskedHost}` })
      : res.status(502).json({ error: `No se pudo conectar a ${maskedHost}: ${err.message}` });
  }

  const delayMs = Math.max(0, Number(options.delayMs) || 0);
  const results = [];
  let ch;
  try {
    const conn = await getConnection();
    ch = await conn.createConfirmChannel();

    for (const original of messages) {
      const msg = structuredClone(original);
      if (msg.messageConfig && typeof msg.messageConfig === 'object') {
        if (options.resetRetryCount) msg.messageConfig.retryCount = 0;
        if (options.refreshLastExecution) msg.messageConfig.lastExecution = new Date().toISOString();
      }
      const traceId = randomUUID();
      const id = msg.ID_CONDICION ?? msg.ID_EVENT ?? null;
      try {
        await new Promise((resolve, reject) => {
          ch.sendToQueue(
            queue,
            Buffer.from(JSON.stringify(msg)),
            {
              persistent: true,
              contentType: 'application/json',
              headers: { 'x-trace-id': traceId, 'x-enqueued-by': 'rabbit-enqueuer' },
            },
            (err) => (err ? reject(err) : resolve()),
          );
        });
        results.push({ id, ok: true, traceId });
      } catch (err) {
        results.push({ id, ok: false, error: err.message });
      }
      if (delayMs) await new Promise((r) => setTimeout(r, delayMs));
    }

    const sent = results.filter((r) => r.ok).length;
    console.log(`[${new Date().toISOString()}] ${sent}/${messages.length} mensajes → ${queue}`);
    res.json({ queue, sent, failed: messages.length - sent, results });
  } catch (err) {
    res.status(500).json({ error: err.message, results });
  } finally {
    if (ch) await ch.close().catch(() => {});
  }
});

app.listen(PORT, () => {
  console.log(`rabbit-enqueuer escuchando en http://localhost:${PORT}  (RabbitMQ: ${maskedHost}${ENV_LABEL ? ' · ' + ENV_LABEL : ''})`);
});

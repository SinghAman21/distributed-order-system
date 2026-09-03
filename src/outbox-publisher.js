// Flow:
// 1. Polls every 1s for PENDING rows where available_at <= NOW()
// 2. FOR UPDATE SKIP LOCKED — safe for multiple publisher instances
// 3. Marks batch as PROCESSING in one transaction, then commits
// 4. Publishes each event to Kafka via existing produce()
// 5. On success → PUBLISHED with published_at
// 6. On failure → increments attempts, sets available_at to +30s. After 5 attempts → FAILED (dead letter)

const { initDatabase, getDb, closeDatabase } = require('./db');
const logger = require('./logger');
const { connectProducer, disconnectProducer, ensureTopics, produce } = require('./kafka');
const config = require('./config');

const POLL_INTERVAL_MS = 1000;
const BATCH_SIZE = 50;
const MAX_ATTEMPTS = 5;

async function fetchPendingEvents(client) {
  const { rows } = await client.query(
    `SELECT id, topic, event_key, event_type, payload
     FROM outbox_events
     WHERE status = 'PENDING'
       AND available_at <= NOW()
     ORDER BY created_at
     FOR UPDATE SKIP LOCKED
     LIMIT $1`,
    [BATCH_SIZE]
  );
  return rows;
}

async function markProcessing(client, ids) {
  if (!ids.length) return;
  await client.query(
    `UPDATE outbox_events
     SET status = 'PROCESSING'
     WHERE id = ANY($1)`,
    [ids]
  );
}

async function markPublished(client, id) {
  await client.query(
    `UPDATE outbox_events
     SET status = 'PUBLISHED', published_at = NOW()
     WHERE id = $1`,
    [id]
  );
}

async function markFailed(client, id, error) {
  await client.query(
    `UPDATE outbox_events
     SET status = CASE WHEN attempts + 1 >= $2 THEN 'FAILED' ELSE 'PENDING' END,
         attempts = attempts + 1,
         available_at = NOW() + INTERVAL '30 seconds',
         last_error = $3
     WHERE id = $1`,
    [id, MAX_ATTEMPTS, String(error)]
  );
}

async function processBatch() {
  const db = getDb();
  const client = await db.connect();

  try {
    await client.query('BEGIN');
    const events = await fetchPendingEvents(client);
    if (!events.length) {
      await client.query('ROLLBACK');
      return;
    }

    const ids = events.map((e) => e.id);
    await markProcessing(client, ids);
    await client.query('COMMIT');

    for (const event of events) {
      try {
        const payload = typeof event.payload === 'string'
          ? JSON.parse(event.payload)
          : event.payload;
        await produce(event.topic, payload);
        const pubClient = await db.connect();
        try {
          await markPublished(pubClient, event.id);
        } finally {
          pubClient.release();
        }
        logger.info({ eventId: event.id, topic: event.topic }, 'Outbox event published');
      } catch (err) {
        logger.error({ eventId: event.id, err }, 'Failed to publish outbox event');
        const failClient = await db.connect();
        try {
          await markFailed(failClient, event.id, err.message);
        } finally {
          failClient.release();
        }
      }
    }
  } catch (err) {
    await client.query('ROLLBACK');
    logger.error({ err }, 'Outbox publisher batch failed');
  } finally {
    client.release();
  }
}

let running = true;

async function pollLoop() {
  while (running) {
    await processBatch();
    await new Promise((r) => setTimeout(r, POLL_INTERVAL_MS));
  }
}

async function start() {
  await initDatabase();
  await connectProducer();
  await ensureTopics();
  logger.info('Outbox publisher started');
  await pollLoop();
}

async function shutdown(signal) {
  logger.info({ signal }, 'Outbox publisher shutting down');
  running = false;
  await disconnectProducer();
  await closeDatabase();
  process.exit(0);
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

start().catch((err) => {
  logger.error({ err }, 'Outbox publisher failed to start');
  process.exit(1);
});

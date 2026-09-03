// Flow:
// 1. Polls every 1s for PENDING rows where available_at <= NOW()
// 2. FOR UPDATE SKIP LOCKED — safe for multiple publisher instances
// 3. Marks batch as PROCESSING in one transaction, then commits
// 4. Publishes each event to Kafka via existing produce()
// 5. On success → PUBLISHED with published_at
// 6. On failure → increments attempts with exponential backoff:
//    30s, 2min, 10min, 30min
// 7. After MAX_ATTEMPTS → moved to outbox_failed_events (DLQ)
//
// Replay DLQ: node src/outbox-publisher.js --replay

const { initDatabase, getDb, closeDatabase } = require('./db');
const logger = require('./logger');
const { connectProducer, disconnectProducer, ensureTopics, produce } = require('./kafka');
const config = require('./config');

const POLL_INTERVAL_MS = 1000;
const BATCH_SIZE = 50;
const MAX_ATTEMPTS = 5;

const BACKOFF_INTERVALS = [
  "30 seconds",
  "2 minutes",
  "10 minutes",
  "30 minutes",
];

function getBackoffInterval(attempt) {
  const index = Math.min(attempt, BACKOFF_INTERVALS.length - 1);
  return BACKOFF_INTERVALS[index];
}

async function fetchPendingEvents(client) {
  const { rows } = await client.query(
    `SELECT id, topic, event_key, event_type, payload, attempts
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

async function markFailed(client, id, error, attempts) {
  const isDeadLetter = attempts + 1 >= MAX_ATTEMPTS;
  const backoff = getBackoffInterval(attempts);

  if (isDeadLetter) {
    await client.query(
      `INSERT INTO outbox_failed_events (id, topic, event_key, event_type, payload, attempts, last_error, created_at)
       SELECT id, topic, event_key, event_type, payload, attempts + 1, $2, created_at
       FROM outbox_events
       WHERE id = $1`,
      [id, String(error)]
    );
    await client.query('DELETE FROM outbox_events WHERE id = $1', [id]);
  } else {
    await client.query(
      `UPDATE outbox_events
       SET status = 'PENDING',
           attempts = attempts + 1,
           available_at = NOW() + INTERVAL '${backoff}',
           last_error = $3
       WHERE id = $1`,
      [id, backoff, String(error)]
    );
  }
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
          await markFailed(failClient, event.id, err.message, event.attempts);
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

async function replayFailedEvents() {
  const db = getDb();
  const { rows: failed } = await db.query(
    `SELECT id, topic, event_key, event_type, payload, attempts, last_error
     FROM outbox_failed_events
     ORDER BY failed_at ASC`
  );

  if (!failed.length) {
    logger.info('No failed events to replay');
    return;
  }

  logger.info({ count: failed.length }, 'Replaying failed events');

  for (const f of failed) {
    try {
      const payload = typeof f.payload === 'string'
        ? JSON.parse(f.payload)
        : f.payload;
      await produce(f.topic, payload);
      await db.query('DELETE FROM outbox_failed_events WHERE id = $1', [f.id]);
      logger.info({ eventId: f.id, topic: f.topic }, 'Failed event replayed successfully');
    } catch (err) {
      logger.error({ eventId: f.id, lastError: f.last_error, err }, 'Failed to replay event');
    }
  }
}

async function pollLoop() {
  while (running) {
    await processBatch();
    await new Promise((r) => setTimeout(r, POLL_INTERVAL_MS));
  }
}

async function start() {
  await initDatabase();

  if (process.argv.includes('--replay')) {
    await connectProducer();
    await ensureTopics();
    await replayFailedEvents();
    await disconnectProducer();
    await closeDatabase();
    process.exit(0);
  }

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

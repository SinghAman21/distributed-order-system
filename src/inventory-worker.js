const { initDatabase, closeDatabase } = require('./db');
const logger = require('./logger');

const { startInventoryConsumer } = require('./events/consumers/inventory.consumer');

let consumer;

async function start() {
  await initDatabase();
  consumer = await startInventoryConsumer();
  logger.info('Inventory worker started');
}

async function shutdown(signal) {
  logger.info({ signal }, 'Inventory worker shutting down');
  if (consumer) await consumer.disconnect();
  await closeDatabase();
  process.exit(0);
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

start().catch((err) => {
  logger.error({ err }, 'Inventory worker failed to start');
  process.exit(1);
});

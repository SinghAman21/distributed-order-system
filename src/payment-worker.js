const { initDatabase, closeDatabase } = require('./db');
const logger = require('./logger');

const { startPaymentConsumer } = require('./events/consumers/payment.consumer');

let consumer;

async function start() {
  await initDatabase();
  consumer = await startPaymentConsumer();
  logger.info('Payment worker started');
}

async function shutdown(signal) {
  logger.info({ signal }, 'Payment worker shutting down');
  if (consumer) await consumer.disconnect();
  await closeDatabase();
  process.exit(0);
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

start().catch((err) => {
  logger.error({ err }, 'Payment worker failed to start');
  process.exit(1);
});

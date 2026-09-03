const logger = require('../../logger');
const config = require('../../config');
const { createConsumer } = require('../../kafka');
const { getDb } = require('../../db');
const { EVENT_TYPES, normalizeEvent } = require('../schema');
const { claimEvent } = require('../idempotency');

async function startNotificationConsumer() {
  const consumer = createConsumer('notification');
  await consumer.connect();
  await consumer.subscribe({ topic: config.kafka.topics.orderPayment, fromBeginning: false });
  await consumer.subscribe({ topic: config.kafka.topics.orderInventory, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const value = message.value?.toString();
      if (!value) return;

      const event = normalizeEvent(JSON.parse(value));
      const orderId = event.orderId;
      const client = await getDb().connect();

      try {
        await client.query('BEGIN');
        const claimed = await claimEvent(client, event);
        await client.query('COMMIT');
        if (!claimed) {
          logger.info({ eventId: event.eventId, orderId }, 'Notification event already processed');
          return;
        }
      } catch (err) {
        await client.query('ROLLBACK').catch(() => {});
        throw err;
      } finally {
        client.release();
      }

      if (event.eventType === EVENT_TYPES.PAYMENT_FAILED) {
        logger.info({ topic, partition, orderId, status: 'PAYMENT_FAILED', reason: event.payload?.failureReason }, 'Notification worker: payment failed notification queued');
        return;
      }

      if (event.eventType === EVENT_TYPES.INVENTORY_FAILED) {
        logger.info({ topic, partition, orderId, status: 'INVENTORY_FAILED', reason: event.payload?.failureReason }, 'Notification worker: inventory failure notification queued');
        return;
      }

      if (event.eventType === EVENT_TYPES.INVENTORY_RESERVED) {
        logger.info({ topic, partition, orderId, status: 'CONFIRMED' }, 'Notification worker: order confirmation notification queued');
      }
    },
  });

  return consumer;
}

module.exports = { startNotificationConsumer };

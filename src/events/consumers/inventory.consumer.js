const { getDb } = require('../../db');
const logger = require('../../logger');
const config = require('../../config');
const { createConsumer } = require('../../kafka');
const { EVENT_TYPES, createEvent } = require('../schema');
const { claimEvent } = require('../idempotency');

async function startInventoryConsumer() {
  const consumer = createConsumer('inventory');
  await consumer.connect();
  await consumer.subscribe({ topic: config.kafka.topics.orderPayment, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const value = message.value?.toString();
      if (!value) return;

      const event = JSON.parse(value);
      if (event.eventType !== EVENT_TYPES.PAYMENT_COMPLETED) return;

      const db = getDb();
      const client = await db.connect();
      try {
        await client.query('BEGIN');
        const claimed = await claimEvent(client, event);
        if (!claimed) {
          await client.query('ROLLBACK');
          logger.info({ eventId: event.eventId, orderId: event.orderId }, 'Inventory event already processed');
          return;
        }

        const orderResult = await client.query('SELECT * FROM order_details WHERE id = $1 FOR UPDATE', [event.orderId]);
        if (!orderResult.rows.length) {
          await client.query('ROLLBACK');
          logger.warn({ orderId: event.orderId }, 'Inventory worker skipped missing order');
          return;
        }

        const order = orderResult.rows[0];
        if (order.status !== 'PAYMENT_COMPLETED') {
          await client.query('ROLLBACK');
          return;
        }

        const inventoryResult = await client.query(
          'SELECT * FROM inventory WHERE inventory_id = $1 FOR UPDATE',
          [order.inventory_id]
        );
        if (!inventoryResult.rows.length) {
          await client.query('ROLLBACK');
          return;
        }

        const item = inventoryResult.rows[0];
        const hasInventory = item.quantity >= order.quantity;

        if (hasInventory) {
          await client.query(
            'UPDATE inventory SET quantity = quantity - $1, updated_at = NOW() WHERE inventory_id = $2',
            [order.quantity, order.inventory_id]
          );
        }

        const nextStatus = hasInventory ? 'CONFIRMED' : 'INVENTORY_FAILED';
        const failureReason = hasInventory ? null : 'INSUFFICIENT_INVENTORY';
        await client.query(
          'UPDATE order_details SET status = $1, failure_reason = $2, updated_at = NOW() WHERE id = $3',
          [nextStatus, failureReason, order.id]
        );

        const nextType = hasInventory ? EVENT_TYPES.INVENTORY_RESERVED : EVENT_TYPES.INVENTORY_FAILED;
        const nextEvent = createEvent(nextType, String(order.id), {
          orderId: order.id,
          userId: order.user_id,
          inventoryId: order.inventory_id,
          quantity: order.quantity,
          totalCost: order.total_cost,
          status: nextStatus,
          failureReason,
        });

        await client.query(
          `INSERT INTO outbox_events (id, topic, event_key, event_type, payload)
           VALUES ($1, $2, $3, $4, $5)`,
          [nextEvent.eventId, config.kafka.topics.orderInventory, String(order.id), nextType, JSON.stringify(nextEvent)]
        );
        await client.query('COMMIT');
        logger.info({ topic, partition, orderId: order.id, status: nextStatus }, 'Inventory event processed');
      } catch (err) {
        await client.query('ROLLBACK').catch(() => {});
        logger.error({ err, orderId: event.orderId }, 'Inventory worker failed');
        throw err;
      } finally {
        client.release();
      }
    },
  });

  return consumer;
}

module.exports = { startInventoryConsumer };

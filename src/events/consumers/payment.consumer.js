const { getDb } = require('../../db');
const logger = require('../../logger');
const config = require('../../config');
const { createConsumer } = require('../../kafka');
const { EVENT_TYPES, createEvent } = require('../schema');
const { claimEvent } = require('../idempotency');

async function startPaymentConsumer() {
  const consumer = createConsumer('payment');
  await consumer.connect();
  await consumer.subscribe({ topic: config.kafka.topics.orderCreated, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const value = message.value?.toString();
      if (!value) return;

      const event = JSON.parse(value);
      if (event.eventType !== EVENT_TYPES.ORDER_CREATED) return;

      const db = getDb();
      const client = await db.connect();
      try {
        await client.query('BEGIN');
        const claimed = await claimEvent(client, event);
        if (!claimed) {
          await client.query('ROLLBACK');
          logger.info({ eventId: event.eventId, orderId: event.orderId }, 'Payment event already processed');
          return;
        }

        const orderResult = await client.query(
          `SELECT o.*, u.balance
           FROM order_details o
           JOIN users u ON u.id = o.user_id
           WHERE o.id = $1
           FOR UPDATE`,
          [event.orderId]
        );

        if (!orderResult.rows.length) {
          await client.query('ROLLBACK');
          logger.warn({ orderId: event.orderId }, 'Payment worker skipped missing order');
          return;
        }

        const order = orderResult.rows[0];
        if (order.status !== 'PENDING') {
          await client.query('ROLLBACK');
          return;
        }

        const hasBalance = Number(order.balance) >= Number(order.total_cost);
        const nextStatus = hasBalance ? 'PAYMENT_COMPLETED' : 'PAYMENT_FAILED';
        const failureReason = hasBalance ? null : 'INSUFFICIENT_BALANCE';

        if (hasBalance) {
          await client.query('UPDATE users SET balance = balance - $1, updated_at = NOW() WHERE id = $2', [order.total_cost, order.user_id]);
        }

        await client.query(
          'UPDATE order_details SET status = $1, failure_reason = $2, updated_at = NOW() WHERE id = $3',
          [nextStatus, failureReason, order.id]
        );

        const nextType = hasBalance ? EVENT_TYPES.PAYMENT_COMPLETED : EVENT_TYPES.PAYMENT_FAILED;
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
          [nextEvent.eventId, config.kafka.topics.orderPayment, String(order.id), nextType, JSON.stringify(nextEvent)]
        );
        await client.query('COMMIT');

        logger.info({ topic, partition, orderId: order.id, status: nextStatus }, 'Payment event processed');
      } catch (err) {
        await client.query('ROLLBACK').catch(() => {});
        logger.error({ err, orderId: event.orderId }, 'Payment worker failed');
        throw err;
      } finally {
        client.release();
      }
    },
  });

  return consumer;
}

module.exports = { startPaymentConsumer };

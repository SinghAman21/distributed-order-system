const { Router } = require('express');
const { z } = require('zod');
const { getDb } = require('./db');
const logger = require('./logger');
const validate = require('./middleware/validate');
const config = require('./config');
const { EVENT_TYPES, createEvent } = require('./events/schema');
const { broadcast } = require('./ws');

const router = Router();

const ALLOWED_STATUSES = [
  'PENDING',
  'PAYMENT_COMPLETED',
  'PAYMENT_FAILED',
  'INVENTORY_RESERVED',
  'INVENTORY_FAILED',
  'CONFIRMED',
  'FAILED',
];

const createOrderSchema = z.object({
  userId: z.number().int().positive('userId must be a positive integer'),
  inventoryId: z.number().int().positive('inventoryId must be a positive integer'),
  quantity: z.number().int().positive('quantity must be a positive integer'),
});

const updateStatusSchema = z.object({
  status: z.enum(ALLOWED_STATUSES, { error: `status must be one of: ${ALLOWED_STATUSES.join(', ')}` }),
});

router.post('/', validate(createOrderSchema), async (req, res) => {
  const { userId, inventoryId, quantity } = req.body;
  const db = getDb();
  const client = await db.connect();

  try {
    await client.query('BEGIN');

    const userResult = await client.query('SELECT id FROM users WHERE id = $1', [userId]);
    if (!userResult.rows.length) {
      await client.query('ROLLBACK');
      return res.status(400).json({ success: false, error: { code: 'INVALID_USER', message: 'User not found' } });
    }

    const inventoryResult = await client.query(
      'SELECT inventory_id, cost FROM inventory WHERE inventory_id = $1',
      [inventoryId]
    );
    if (!inventoryResult.rows.length) {
      await client.query('ROLLBACK');
      return res.status(400).json({ success: false, error: { code: 'INVALID_INVENTORY', message: 'Inventory item not found' } });
    }

    const item = inventoryResult.rows[0];
    const totalCost = Number((Number(item.cost) * quantity).toFixed(2));

    const result = await client.query(
      `INSERT INTO order_details (user_id, inventory_id, quantity, unit_cost, total_cost, status)
       VALUES ($1, $2, $3, $4, $5, 'PENDING')
       RETURNING *`,
      [userId, inventoryId, quantity, item.cost, totalCost]
    );

    const order = result.rows[0];
    const event = createEvent(EVENT_TYPES.ORDER_CREATED, String(order.id), {
      orderId: order.id,
      userId: order.user_id,
      inventoryId: order.inventory_id,
      quantity: order.quantity,
      totalCost: order.total_cost,
      status: order.status,
    });

    await client.query(
      `INSERT INTO outbox_events (id, topic, event_key, event_type, payload)
       VALUES ($1, $2, $3, $4, $5)`,
      [event.eventId, config.kafka.topics.orderCreated, event.orderId, event.eventType, JSON.stringify(event)]
    );

    await client.query('COMMIT');

    broadcast({ type: 'order_event', stage: 'created', orderId: order.id, timestamp: Date.now() });
    logger.info({ requestId: req.requestId, orderId: order.id }, 'Order captured');
    return res.status(201).json({ success: true, data: order });
  } catch (err) {
    await client.query('ROLLBACK');
    logger.error({ requestId: req.requestId, err }, 'Failed to create order');
    return res.status(500).json({ success: false, error: { code: 'DB_ERROR', message: 'Failed to create order' } });
  } finally {
    client.release();
  }
});

router.get('/', async (req, res) => {
  try {
    const result = await getDb().query(
      `SELECT o.*, u.name AS user_name, i.name AS inventory_name
       FROM order_details o
       JOIN users u ON u.id = o.user_id
       JOIN inventory i ON i.inventory_id = o.inventory_id
       ORDER BY o.created_at DESC, o.id DESC`
    );
    return res.json({ success: true, data: result.rows, meta: { total: result.rowCount } });
  } catch (err) {
    logger.error({ requestId: req.requestId, err }, 'Failed to list orders');
    return res.status(500).json({ success: false, error: { code: 'DB_ERROR', message: 'Failed to list orders' } });
  }
});

router.get('/:id', async (req, res) => {
  try {
    const result = await getDb().query(
      `SELECT o.*, u.name AS user_name, i.name AS inventory_name
       FROM order_details o
       JOIN users u ON u.id = o.user_id
       JOIN inventory i ON i.inventory_id = o.inventory_id
       WHERE o.id = $1`,
      [req.params.id]
    );
    if (!result.rows.length) return res.status(404).json({ success: false, error: { code: 'NOT_FOUND', message: 'Order not found' } });
    return res.json({ success: true, data: result.rows[0] });
  } catch (err) {
    logger.error({ requestId: req.requestId, err, orderId: req.params.id }, 'Failed to get order');
    return res.status(500).json({ success: false, error: { code: 'DB_ERROR', message: 'Failed to get order' } });
  }
});

router.patch('/:id/status', validate(updateStatusSchema), async (req, res) => {
  try {
    const result = await getDb().query(
      'UPDATE order_details SET status = $1, updated_at = NOW() WHERE id = $2 RETURNING *',
      [req.body.status, req.params.id]
    );
    if (!result.rows.length) return res.status(404).json({ success: false, error: { code: 'NOT_FOUND', message: 'Order not found' } });
    const order = result.rows[0];
    const stage = req.body.status.startsWith('PAYMENT_') ? 'payment' : req.body.status.startsWith('INVENTORY_') ? 'inventory' : req.body.status === 'CONFIRMED' ? 'confirmed' : 'created';
    broadcast({ type: 'order_event', stage, orderId: order.id, status: order.status, timestamp: Date.now() });
    return res.json({ success: true, data: order });
  } catch (err) {
    logger.error({ requestId: req.requestId, err, orderId: req.params.id }, 'Failed to update order status');
    return res.status(500).json({ success: false, error: { code: 'DB_ERROR', message: 'Failed to update order status' } });
  }
});

module.exports = { router, ALLOWED_STATUSES };

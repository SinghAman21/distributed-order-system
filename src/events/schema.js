const { randomUUID } = require('crypto');

const EVENT_TYPES = {
  ORDER_CREATED: 'ORDER_CREATED',
  PAYMENT_COMPLETED: 'PAYMENT_COMPLETED',
  PAYMENT_FAILED: 'PAYMENT_FAILED',
  INVENTORY_RESERVED: 'INVENTORY_RESERVED',
  INVENTORY_FAILED: 'INVENTORY_FAILED',
};

const CURRENT_EVENT_VERSION = 1;
const SUPPORTED_EVENT_VERSIONS = new Set([1]);

function normalizeEvent(event) {
  const eventVersion = Number(event?.eventVersion || CURRENT_EVENT_VERSION);

  if (!SUPPORTED_EVENT_VERSIONS.has(eventVersion)) {
    throw new Error(`Unsupported eventVersion: ${eventVersion}`);
  }

  return {
    ...event,
    eventVersion,
  };
}

function createEvent(eventType, orderId, payload) {
  return normalizeEvent({
    eventId: randomUUID(),
    eventType,
    orderId,
    payload,
    timestamp: new Date().toISOString(),
  });
}

module.exports = {
  EVENT_TYPES,
  CURRENT_EVENT_VERSION,
  normalizeEvent,
  createEvent,
};

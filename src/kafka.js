const { Kafka, logLevel } = require('kafkajs');
const config = require('./config');
const logger = require('./logger');

const kafka = new Kafka({
  clientId: config.kafka.clientId,
  brokers: config.kafka.brokers,
  logLevel: logLevel.NOTHING,
});

const producer = kafka.producer();
let producerConnected = false;
let producerConnecting = null;
let topicsEnsured = false;
let topicsEnsuring = null;

async function connectProducer() {
  if (producerConnected) return true;
  if (producerConnecting) return producerConnecting;

  producerConnecting = producer.connect()
    .then(() => {
      producerConnected = true;
      logger.info({ brokers: config.kafka.brokers }, 'Kafka producer connected');
      return true;
    })
    .catch((err) => {
      producerConnected = false;
      throw err;
    })
    .finally(() => {
      producerConnecting = null;
    });

  return producerConnecting;
}

async function disconnectProducer() {
  if (!producerConnected) return;
  await producer.disconnect();
  producerConnected = false;
  logger.info('Kafka producer disconnected');
}

function isProducerConnected() {
  return producerConnected;
}

async function produce(topic, event) {
  if (!producerConnected) {
    await connectProducer();
  }

  await producer.send({
    topic,
    messages: [
      {
        key: event.orderId,
        value: JSON.stringify(event),
      },
    ],
  });

  logger.info(
    {
      topic,
      eventType: event.eventType,
      orderId: event.orderId,
      eventId: event.eventId,
    },
    'Kafka event produced'
  );
}

async function ensureTopics() {
  if (topicsEnsured) return true;
  if (topicsEnsuring) return topicsEnsuring;

  topicsEnsuring = (async () => {
    const admin = kafka.admin();
    await admin.connect();
    try {
      await admin.createTopics({
        waitForLeaders: true,
        topics: [
          { topic: config.kafka.topics.orderCreated, numPartitions: 1, replicationFactor: 1 },
          { topic: config.kafka.topics.orderPayment, numPartitions: 1, replicationFactor: 1 },
          { topic: config.kafka.topics.orderInventory, numPartitions: 1, replicationFactor: 1 },
          { topic: config.kafka.topics.outboxDlq, numPartitions: 1, replicationFactor: 1 },
        ],
      });
      topicsEnsured = true;
      logger.info({ topics: Object.values(config.kafka.topics) }, 'Kafka topics ensured');
      return true;
    } finally {
      await admin.disconnect();
      topicsEnsuring = null;
    }
  })();

  return topicsEnsuring;
}

function createConsumer(groupIdSuffix) {
  return kafka.consumer({
    groupId: `${config.kafka.groupId}-${groupIdSuffix}`,
  });
}

module.exports = {
  connectProducer,
  disconnectProducer,
  ensureTopics,
  isProducerConnected,
  produce,
  createConsumer,
};

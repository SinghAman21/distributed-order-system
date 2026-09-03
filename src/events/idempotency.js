async function claimEvent(client, event) {
  if (!event?.eventId) {
    throw new Error('Kafka event is missing eventId');
  }

  const result = await client.query(
    `INSERT INTO processed_events (event_id, event_version, event_type, order_id)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (event_id) DO NOTHING
     RETURNING event_id`,
    [event.eventId, event.eventVersion || 1, event.eventType || null, event.orderId || null]
  );

  return result.rowCount === 1;
}

module.exports = { claimEvent };

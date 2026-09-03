-- One-time replacement of the previous customers/products/orders model.
-- It runs only before order_details exists, so normal restarts never delete data.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'order_details'
  ) THEN
    DROP TABLE IF EXISTS orders CASCADE;
    DROP TABLE IF EXISTS products CASCADE;
    DROP TABLE IF EXISTS customers CASCADE;
  END IF;
END $$;

-- Users
CREATE TABLE IF NOT EXISTS users (
    id          BIGSERIAL PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    balance     NUMERIC(12, 2) NOT NULL DEFAULT 0 CHECK (balance >= 0),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Inventory items
CREATE TABLE IF NOT EXISTS inventory (
    inventory_id BIGSERIAL PRIMARY KEY,
    name         VARCHAR(255) NOT NULL,
    quantity     INTEGER NOT NULL DEFAULT 0 CHECK (quantity >= 0),
    cost         NUMERIC(12, 2) NOT NULL CHECK (cost >= 0),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Captured orders
CREATE TABLE IF NOT EXISTS order_details (
    id           BIGSERIAL PRIMARY KEY,
    user_id      BIGINT NOT NULL REFERENCES users(id),
    inventory_id BIGINT NOT NULL REFERENCES inventory(inventory_id),
    quantity     INTEGER NOT NULL CHECK (quantity > 0),
    unit_cost    NUMERIC(12, 2) NOT NULL CHECK (unit_cost >= 0),
    total_cost   NUMERIC(12, 2) NOT NULL CHECK (total_cost >= 0),
    status       VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    failure_reason TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_order_details_created_at ON order_details (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_order_details_user_id ON order_details (user_id);
CREATE INDEX IF NOT EXISTS idx_order_details_inventory_id ON order_details (inventory_id);

-- Durable deduplication for Kafka consumers. A unique event ID makes retries safe.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     TEXT PRIMARY KEY,
    event_type   VARCHAR(100),
    order_id     BIGINT,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS outbox_events (
  id UUID PRIMARY KEY,
  topic TEXT NOT NULL,
  event_key TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'PENDING',
  attempts INTEGER NOT NULL DEFAULT 0,
  available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  locked_at TIMESTAMPTZ,
  published_at TIMESTAMPTZ,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX outbox_pending_idx
  ON outbox_events (available_at, created_at)
  WHERE status = 'PENDING';

-- Dead-letter queue for events that exhausted all retries
CREATE TABLE IF NOT EXISTS outbox_failed_events (
  id UUID PRIMARY KEY,
  topic TEXT NOT NULL,
  event_key TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL,
  attempts INTEGER NOT NULL,
  last_error TEXT,
  failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_outbox_failed_events_created_at ON outbox_failed_events (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_processed_events_order_id ON processed_events (order_id);

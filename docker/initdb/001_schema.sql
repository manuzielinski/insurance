CREATE SCHEMA IF NOT EXISTS quoting;

CREATE TABLE IF NOT EXISTS quoting.quotes (
  id            BIGSERIAL PRIMARY KEY,
  customer_id   TEXT NOT NULL,
  product_code  TEXT NOT NULL,
  premium       NUMERIC(12,2) NOT NULL,
  status        TEXT NOT NULL DEFAULT 'CREATED',
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  trace_id      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS quoting.outbox_events (
  id            BIGSERIAL PRIMARY KEY,
  event_id      TEXT NOT NULL,
  event_type    TEXT NOT NULL,
  payload       JSONB NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at  TIMESTAMPTZ NULL
);

-- índice para no publicar dos veces
CREATE UNIQUE INDEX IF NOT EXISTS uq_outbox_event_id ON quoting.outbox_events(event_id);

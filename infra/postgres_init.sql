-- PostgreSQL Read Model Schema
-- CQRS Query Side — denormalized for fast reads

CREATE TABLE IF NOT EXISTS orders (
    order_id        TEXT PRIMARY KEY,
    customer_id     TEXT NOT NULL,
    customer_email  TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'PENDING',
    total_amount    NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    version         INTEGER NOT NULL DEFAULT 1,
    payment_id      TEXT,
    tracking_id     TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_items (
    id              SERIAL PRIMARY KEY,
    order_id        TEXT NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id      TEXT NOT NULL,
    product_name    TEXT NOT NULL,
    quantity        INTEGER NOT NULL,
    unit_price      NUMERIC(10, 2) NOT NULL,
    line_total      NUMERIC(12, 2) NOT NULL,
    UNIQUE(order_id, product_id)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_orders_customer   ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_status     ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_created    ON orders(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_items_order       ON order_items(order_id);

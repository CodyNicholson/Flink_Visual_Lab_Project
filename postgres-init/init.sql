CREATE TABLE customers_cdc_source (
    customer_id VARCHAR(32) PRIMARY KEY,
    full_name VARCHAR(100) NOT NULL,
    tier VARCHAR(32) NOT NULL,
    credit_limit NUMERIC(12, 2) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Log every column of the old row in the WAL so UPDATE/DELETE carry the "before" image (-U / -D)
ALTER TABLE customers_cdc_source REPLICA IDENTITY FULL;

INSERT INTO customers_cdc_source (customer_id, full_name, tier, credit_limit) VALUES
('CUST-101', 'Alice Rivera', 'GOLD', 15000.00),
('CUST-202', 'Brandon Chen', 'SILVER', 5000.00);

CREATE TABLE enriched_orders_sink (
    order_id VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(32) NOT NULL,
    source_type VARCHAR(32) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    customer_tier VARCHAR(32) NOT NULL,
    running_customer_total NUMERIC(12, 2) NOT NULL,
    watermark_status VARCHAR(32) NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

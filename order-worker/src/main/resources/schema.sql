CREATE TABLE IF NOT EXISTS processed_orders (
    order_id UUID PRIMARY KEY,
    event_id UUID NOT NULL,
    status VARCHAR(30) NOT NULL,
    total_amount DECIMAL(19, 2) NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE NOT NULL
);

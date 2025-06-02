CREATE TABLE IF NOT EXISTS cdm.user_product_counters (
    id SERIAL PRIMARY KEY,
    user_id uuid NOT NULL,
    product_id uuid NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    order_cnt INT NOT null CHECK(order_cnt >= 0)
);
CREATE UNIQUE INDEX idx_user_product
ON cdm.user_product_counters (user_id, product_id)
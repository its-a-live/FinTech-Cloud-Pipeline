CREATE TABLE IF NOT EXISTS cdm.user_category_counters (
    id SERIAL PRIMARY KEY,
    user_id uuid NOT NULL,
    category_id uuid NOT NULL,
    category_name VARCHAR(255) NOT NULL,
    order_cnt INT NOT null CHECK(order_cnt >= 0)
);
CREATE UNIQUE INDEX idx_user_category
ON cdm.user_category_counters (user_id, category_id)
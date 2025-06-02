CREATE TABLE IF NOT EXISTS dds.h_restaurant (
    h_restaurant_pk uuid PRIMARY KEY,
    restaurant_id VARCHAR(255) NOT null UNIQUE,
    load_dt timestamp NOT NULL,
    load_src VARCHAR(255) NOT NULL
);

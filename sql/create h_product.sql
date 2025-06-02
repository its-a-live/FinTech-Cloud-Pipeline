CREATE TABLE IF NOT EXISTS dds.h_product (
    h_product_pk uuid PRIMARY KEY,
    product_id VARCHAR(255) NOT null UNIQUE,
    load_dt timestamp NOT NULL,
    load_src VARCHAR(255) NOT NULL
);
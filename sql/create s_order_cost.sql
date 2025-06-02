CREATE TABLE IF NOT EXISTS dds.s_order_cost (
    h_order_pk uuid REFERENCES dds.h_order(h_order_pk) PRIMARY KEY,
    "cost" decimal(19, 5) NOT null,
    payment decimal(19, 5) NOT null,
    hk_order_cost_hashdiff uuid not null,
    load_dt timestamp NOT NULL,
    load_src VARCHAR(255) NOT NULL
);
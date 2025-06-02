CREATE TABLE IF NOT EXISTS dds.s_order_status (
    h_order_pk uuid REFERENCES dds.h_order(h_order_pk) PRIMARY KEY,
    status VARCHAR(255) NOT null,
    hk_order_status_hashdiff uuid not null,
    load_dt timestamp NOT NULL,
    load_src VARCHAR(255) NOT NULL
);

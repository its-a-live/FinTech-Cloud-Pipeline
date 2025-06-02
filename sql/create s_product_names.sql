CREATE TABLE IF NOT EXISTS dds.s_product_names (
    h_product_pk uuid REFERENCES dds.h_product(h_product_pk) PRIMARY KEY,
    "name" VARCHAR(255) NOT null,
    hk_product_names_hashdiff uuid not null,
    load_dt timestamp NOT NULL,
    load_src VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.l_order_product (
    hk_order_product_pk uuid PRIMARY KEY,
    h_order_pk uuid REFERENCES dds.h_order(h_order_pk) NOT null,
    h_product_pk uuid REFERENCES dds.h_product(h_product_pk) NOT null,
    load_dt timestamp NOT NULL,
    load_src VARCHAR(255) NOT NULL
);
alter table dds.l_order_product  add CONSTRAINT l_order_product_uindex UNIQUE (h_order_pk, h_product_pk);

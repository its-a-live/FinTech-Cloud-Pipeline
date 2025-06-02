CREATE TABLE IF NOT EXISTS dds.l_product_restaurant (
    hk_product_restaurant_pk uuid PRIMARY KEY,
    h_restaurant_pk uuid REFERENCES dds.h_restaurant(h_restaurant_pk) NOT null,
    h_product_pk uuid REFERENCES dds.h_product(h_product_pk) NOT null,
    load_dt timestamp NOT NULL,
    load_src VARCHAR(255) NOT NULL
);
alter table dds.l_product_restaurant add CONSTRAINT l_product_restaurant_uindex UNIQUE (h_restaurant_pk, h_product_pk);

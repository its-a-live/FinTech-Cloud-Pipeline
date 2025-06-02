CREATE TABLE IF NOT EXISTS dds.l_product_category (
    hk_product_category_pk uuid PRIMARY KEY,
    h_category_pk uuid REFERENCES dds.h_category(h_category_pk) NOT null,
    h_product_pk uuid REFERENCES dds.h_product(h_product_pk) NOT null,
    load_dt timestamp NOT NULL,
    load_src VARCHAR(255) NOT NULL
);
alter table dds.l_product_category add CONSTRAINT l_product_category_uindex UNIQUE (h_category_pk, h_product_pk);

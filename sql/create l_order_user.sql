CREATE TABLE IF NOT EXISTS dds.l_order_user (
    hk_order_user_pk uuid PRIMARY KEY,
    h_user_pk uuid REFERENCES dds.h_user(h_user_pk) NOT null,
    h_order_pk uuid REFERENCES dds.h_order(h_order_pk) NOT null,
    load_dt timestamp NOT NULL,
    load_src VARCHAR(255) NOT NULL
);
alter table dds.l_order_user add CONSTRAINT l_order_user_uindex UNIQUE (h_user_pk, h_order_pk);
CREATE TABLE IF NOT EXISTS dds.h_order (
    h_order_pk uuid PRIMARY KEY,
    order_id int4 NOT null,
    order_dt timestamp NOT NULL,
    load_dt timestamp NOT NULL,
    load_src VARCHAR(255) NOT NULL
);
alter table dds.h_order add CONSTRAINT h_order_id_uindex UNIQUE (order_id)

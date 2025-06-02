CREATE TABLE IF NOT EXISTS dds.h_category (
    h_category_pk uuid PRIMARY KEY,
    category_name VARCHAR NOT NULL,
    load_dt timestamp NOT NULL,
    load_src VARCHAR(255) NOT NULL
);
alter table dds.h_category add CONSTRAINT h_category_name_uindex UNIQUE (category_name);
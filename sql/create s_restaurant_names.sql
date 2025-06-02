CREATE TABLE IF NOT EXISTS dds.s_restaurant_names (
    h_restaurant_pk uuid REFERENCES dds.h_restaurant(h_restaurant_pk) PRIMARY KEY,
    "name" VARCHAR(255) NOT null,
    hk_restaurant_names_hashdiff uuid not null,
    load_dt timestamp NOT NULL,
    load_src VARCHAR(255) NOT NULL
);

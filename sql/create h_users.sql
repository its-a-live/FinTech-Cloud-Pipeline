CREATE TABLE IF NOT EXISTS dds.h_user (
    h_user_pk uuid PRIMARY KEY,
    user_id VARCHAR(255) NOT null UNIQUE,
    load_dt timestamp NOT NULL,
    load_src VARCHAR(255) NOT NULL
);
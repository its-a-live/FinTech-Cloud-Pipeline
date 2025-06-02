CREATE TABLE IF NOT EXISTS dds.s_user_names (
    h_user_pk uuid REFERENCES dds.h_user(h_user_pk) PRIMARY KEY,
    username VARCHAR(255) NOT null,
    userlogin VARCHAR(255) NOT null,
    hk_user_names_hashdiff uuid not null,
    load_dt timestamp NOT NULL,
    load_src VARCHAR(255) NOT NULL
);
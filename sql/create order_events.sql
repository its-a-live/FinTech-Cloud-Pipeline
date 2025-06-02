CREATE SCHEMA IF NOT EXISTS stg AUTHORIZATION me;

CREATE TABLE IF NOT EXISTS stg.order_events (
    id SERIAL PRIMARY KEY,
    object_id int4 NOT null UNIQUE,
    payload json NOT NULL,
    object_type VARCHAR(255) NOT NULL,
    sent_dttm timestamp NOT NULL
);

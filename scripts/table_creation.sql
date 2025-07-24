CREATE TABLE IF NOT EXISTS staging_events (
    event_time TIMESTAMP,
    event_type VARCHAR,
    product_id BIGINT,
    category_id BIGINT,
    category_code VARCHAR,
    brand VARCHAR,
    price NUMERIC,
    user_id BIGINT,
    user_session VARCHAR,
    event_date DATE,
    loaded_at DATE
);

CREATE TABLE IF NOT EXISTS fact_events (
    event_date DATE,
    event_type TEXT,
    product_id BIGINT,
    category_id BIGINT,
    brand TEXT,
    user_id BIGINT,
    user_session TEXT,
    total_events INT,
    total_revenue FLOAT
);

CREATE TABLE IF NOT EXISTS dim_user (
    user_id BIGINT PRIMARY KEY,
    user_session TEXT
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id BIGINT PRIMARY KEY,
    category_id BIGINT,
    category_code TEXT,
    brand TEXT,
    price NUMERIC
);

CREATE TABLE IF NOT EXISTS dim_date (
    date DATE PRIMARY KEY,
    day INT,
    month INT,
    year INT,
    weekday INT,
    week INT
);


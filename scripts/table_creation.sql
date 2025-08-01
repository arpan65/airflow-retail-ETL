-- ✅ STAGING TABLE
DROP TABLE IF EXISTS staging_events;
CREATE TABLE staging_events (
    event_time TIMESTAMP NOT NULL,
    event_type VARCHAR NOT NULL,
    product_id BIGINT,
    category_id BIGINT,
    category_code VARCHAR,
    brand VARCHAR,
    price NUMERIC,
    user_id BIGINT,
    user_session VARCHAR,
    event_date DATE NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ✅ FACT TABLE
DROP TABLE IF EXISTS fact_events;
CREATE TABLE fact_events (
    event_date DATE,
    event_type TEXT,
    product_id BIGINT,     -- FK to dim_product
    user_id BIGINT,        -- FK to dim_user
    total_events INT,
    total_revenue FLOAT
);

-- ✅ DIM USER
DROP TABLE IF EXISTS dim_user;
CREATE TABLE dim_user (
    user_id BIGINT PRIMARY KEY,
    user_session TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ✅ DIM PRODUCT
DROP TABLE IF EXISTS dim_product;
CREATE TABLE dim_product (
    product_id BIGINT PRIMARY KEY,
    category_id BIGINT,
    category_code TEXT,
    subcategory1 TEXT,
    subcategory2 TEXT,
    brand TEXT,
    price NUMERIC,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ✅ DIM DATE
DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
    date DATE PRIMARY KEY,
    day INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL,
    week INT NOT NULL
);

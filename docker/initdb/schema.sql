-- Dimension Tables
CREATE TABLE dim_users (
    user_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100),
    location VARCHAR(100),
    signup_date DATE
);

CREATE TABLE dim_products (
    product_id VARCHAR(50) PRIMARY KEY,
    category VARCHAR(50),
    price DECIMAL(10,2),
    brand VARCHAR(50)
);

CREATE TABLE dim_time (
    timestamp TIMESTAMP PRIMARY KEY,
    hour INTEGER,
    day INTEGER,
    month INTEGER,
    year INTEGER
);

-- Fact Tables
CREATE TABLE fact_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) REFERENCES dim_users(user_id),
    product_id VARCHAR(50) REFERENCES dim_products(product_id),
    timestamp TIMESTAMP REFERENCES dim_time(timestamp),
    revenue DECIMAL(10,2),
    quantity INTEGER
);

CREATE TABLE fact_clicks (
    click_id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) REFERENCES dim_users(user_id),
    product_id VARCHAR(50) REFERENCES dim_products(product_id),
    timestamp TIMESTAMP REFERENCES dim_time(timestamp),
    session_id VARCHAR(100)
);

CREATE TABLE fact_payments (
    payment_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50) REFERENCES fact_orders(order_id),
    amount DECIMAL(10,2),
    payment_method VARCHAR(50),
    status VARCHAR(50)
);


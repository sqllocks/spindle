-- AdventureWorks-style DDL for Smart Inference Engine integration testing.
-- This is a representative subset of the AdventureWorks OLTP schema,
-- adapted for Spindle's DDL parser (no schema qualifiers, standard SQL types).
-- INTERNAL TEST FIXTURE — not for documentation or publication.

CREATE TABLE persons (
    person_id INT IDENTITY(1,1) PRIMARY KEY,
    first_name NVARCHAR(50) NOT NULL,
    last_name NVARCHAR(50) NOT NULL,
    email NVARCHAR(100),
    phone NVARCHAR(25),
    birth_date DATE,
    gender NVARCHAR(1),
    created_at DATETIME2 NOT NULL,
    modified_at DATETIME2 NOT NULL
);

CREATE TABLE addresses (
    address_id INT IDENTITY(1,1) PRIMARY KEY,
    address_line1 NVARCHAR(100) NOT NULL,
    city NVARCHAR(50) NOT NULL,
    state NVARCHAR(50) NOT NULL,
    postal_code NVARCHAR(15) NOT NULL,
    country NVARCHAR(50) NOT NULL DEFAULT 'US'
);

CREATE TABLE person_addresses (
    person_id INT NOT NULL,
    address_id INT NOT NULL,
    address_type NVARCHAR(20) NOT NULL,
    PRIMARY KEY (person_id, address_id),
    FOREIGN KEY (person_id) REFERENCES persons (person_id),
    FOREIGN KEY (address_id) REFERENCES addresses (address_id)
);

CREATE TABLE product_categories (
    category_id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(50) NOT NULL,
    parent_category_id INT,
    FOREIGN KEY (parent_category_id) REFERENCES product_categories (category_id)
);

CREATE TABLE products (
    product_id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(100) NOT NULL,
    product_number NVARCHAR(25) NOT NULL,
    color NVARCHAR(15),
    unit_price DECIMAL(18,2) NOT NULL,
    unit_cost DECIMAL(18,2) NOT NULL,
    weight_kg DECIMAL(8,2),
    category_id INT,
    is_active BIT NOT NULL DEFAULT 1,
    sell_start_date DATE NOT NULL,
    sell_end_date DATE,
    created_at DATETIME2 NOT NULL,
    modified_at DATETIME2 NOT NULL,
    FOREIGN KEY (category_id) REFERENCES product_categories (category_id)
);

CREATE TABLE customers (
    customer_id INT IDENTITY(1,1) PRIMARY KEY,
    person_id INT NOT NULL,
    account_number NVARCHAR(20) NOT NULL,
    status NVARCHAR(20) NOT NULL DEFAULT 'active',
    territory NVARCHAR(50),
    created_at DATETIME2 NOT NULL,
    FOREIGN KEY (person_id) REFERENCES persons (person_id)
);

CREATE TABLE sales_orders (
    order_id INT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date DATE NOT NULL,
    ship_date DATE,
    due_date DATE,
    status NVARCHAR(20) NOT NULL DEFAULT 'pending',
    subtotal DECIMAL(18,2) NOT NULL,
    tax_amount DECIMAL(18,2) NOT NULL,
    freight DECIMAL(18,2) NOT NULL,
    total_amount DECIMAL(18,2) NOT NULL,
    payment_method NVARCHAR(30),
    territory NVARCHAR(50),
    created_at DATETIME2 NOT NULL,
    modified_at DATETIME2 NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
);

CREATE TABLE order_details (
    detail_id INT IDENTITY(1,1) PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity SMALLINT NOT NULL,
    unit_price DECIMAL(18,2) NOT NULL,
    unit_cost DECIMAL(18,2) NOT NULL,
    discount_pct DECIMAL(5,2) NOT NULL DEFAULT 0,
    line_total DECIMAL(18,2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES sales_orders (order_id),
    FOREIGN KEY (product_id) REFERENCES products (product_id)
);

CREATE TABLE product_reviews (
    review_id INT IDENTITY(1,1) PRIMARY KEY,
    product_id INT NOT NULL,
    customer_id INT NOT NULL,
    rating INT NOT NULL,
    comment NVARCHAR(1000),
    review_date DATE NOT NULL,
    status NVARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at DATETIME2 NOT NULL,
    FOREIGN KEY (product_id) REFERENCES products (product_id),
    FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
);

CREATE TABLE inventory_log (
    log_id INT IDENTITY(1,1) PRIMARY KEY,
    product_id INT NOT NULL,
    quantity_change INT NOT NULL,
    reason NVARCHAR(50) NOT NULL,
    created_at DATETIME2 NOT NULL,
    FOREIGN KEY (product_id) REFERENCES products (product_id)
);

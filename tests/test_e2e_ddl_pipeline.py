"""E2E tests: DDL import → inference → generate → integrity."""

from __future__ import annotations

import pytest

from sqllocks_spindle import Spindle
from sqllocks_spindle.schema.ddl_parser import DdlParser


SQL_SERVER_DDL = """\
CREATE TABLE [dbo].[customer] (
    customer_id INT IDENTITY(1,1) NOT NULL,
    first_name NVARCHAR(50) NOT NULL,
    last_name NVARCHAR(50) NOT NULL,
    email NVARCHAR(100),
    is_active BIT DEFAULT 1,
    created_at DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT PK_customer PRIMARY KEY (customer_id)
);

CREATE TABLE [dbo].[product] (
    product_id INT IDENTITY(1,1) NOT NULL,
    product_name NVARCHAR(200) NOT NULL,
    price DECIMAL(10,2),
    category VARCHAR(50),
    CONSTRAINT PK_product PRIMARY KEY (product_id)
);

CREATE TABLE [dbo].[order] (
    order_id INT IDENTITY(1,1) NOT NULL,
    customer_id INT NOT NULL,
    order_date DATE NOT NULL,
    total DECIMAL(10,2),
    status VARCHAR(20),
    CONSTRAINT PK_order PRIMARY KEY (order_id),
    CONSTRAINT FK_order_customer FOREIGN KEY (customer_id)
        REFERENCES [dbo].[customer](customer_id)
);

CREATE TABLE [dbo].[order_line] (
    line_id INT IDENTITY(1,1) NOT NULL,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT DEFAULT 1,
    line_total DECIMAL(10,2),
    CONSTRAINT PK_order_line PRIMARY KEY (line_id),
    CONSTRAINT FK_line_order FOREIGN KEY (order_id) REFERENCES [dbo].[order](order_id),
    CONSTRAINT FK_line_product FOREIGN KEY (product_id) REFERENCES [dbo].[product](product_id)
);
"""

POSTGRES_DDL = """\
CREATE TABLE customer (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100)
);

CREATE TABLE "order" (
    order_id BIGSERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customer(customer_id),
    order_date DATE NOT NULL,
    total NUMERIC(10,2)
);
"""


class TestDdlParsing:
    def test_sql_server_parse(self):
        parser = DdlParser()
        schema = parser.parse_string(SQL_SERVER_DDL)
        assert "customer" in schema.tables
        assert "order" in schema.tables
        assert "order_line" in schema.tables
        assert "product" in schema.tables

    def test_postgres_parse(self):
        parser = DdlParser()
        schema = parser.parse_string(POSTGRES_DDL)
        assert "customer" in schema.tables
        assert "order" in schema.tables


class TestDdlToGeneration:
    def test_sql_server_ddl_generates_data(self):
        parser = DdlParser()
        schema = parser.parse_string(SQL_SERVER_DDL)
        result = Spindle().generate(schema=schema, scale="small", seed=42)
        assert len(result.tables) == 4
        for table_name, df in result.tables.items():
            assert len(df) > 0, f"{table_name} generated 0 rows"

    def test_sql_server_ddl_fk_integrity(self):
        parser = DdlParser()
        schema = parser.parse_string(SQL_SERVER_DDL)
        result = Spindle().generate(schema=schema, scale="small", seed=42)
        errors = result.verify_integrity()
        assert errors == [], f"FK integrity errors: {errors}"

    def test_postgres_ddl_generates_data(self):
        parser = DdlParser()
        schema = parser.parse_string(POSTGRES_DDL)
        result = Spindle().generate(schema=schema, scale="small", seed=42)
        assert len(result.tables) >= 2
        errors = result.verify_integrity()
        assert errors == [], f"FK integrity errors: {errors}"

    def test_ddl_pk_detected(self):
        parser = DdlParser()
        schema = parser.parse_string(SQL_SERVER_DDL)
        assert schema.tables["customer"].primary_key == ["customer_id"]
        assert schema.tables["order"].primary_key == ["order_id"]

    def test_ddl_fk_relationships_detected(self):
        parser = DdlParser()
        schema = parser.parse_string(SQL_SERVER_DDL)
        rel_names = [r.name for r in schema.relationships]
        assert len(schema.relationships) >= 3  # order→customer, line→order, line→product

    def test_ddl_strategy_inference(self):
        parser = DdlParser()
        schema = parser.parse_string(SQL_SERVER_DDL)
        # email should get faker
        email_gen = schema.tables["customer"].columns["email"].generator
        assert email_gen["strategy"] == "faker"
        # status should get weighted_enum
        status_gen = schema.tables["order"].columns["status"].generator
        assert status_gen["strategy"] == "weighted_enum"

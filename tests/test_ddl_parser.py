"""Tests for the DDL parser."""

from __future__ import annotations

import pytest

from sqllocks_spindle.schema.ddl_parser import DdlParser


@pytest.fixture()
def parser():
    return DdlParser()


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

MYSQL_DDL = """\
CREATE TABLE IF NOT EXISTS `customer` (
    `customer_id` INT AUTO_INCREMENT PRIMARY KEY,
    `first_name` VARCHAR(50) NOT NULL,
    `email` VARCHAR(100)
);

CREATE TABLE `order` (
    `order_id` INT AUTO_INCREMENT PRIMARY KEY,
    `customer_id` INT NOT NULL,
    `order_date` DATE NOT NULL,
    FOREIGN KEY (`customer_id`) REFERENCES `customer`(`customer_id`)
);
"""


class TestDdlParserTables:
    def test_sql_server_parses_two_tables(self, parser):
        schema = parser.parse_string(SQL_SERVER_DDL)
        assert "customer" in schema.tables
        assert "order" in schema.tables

    def test_postgres_parses_two_tables(self, parser):
        schema = parser.parse_string(POSTGRES_DDL)
        assert "customer" in schema.tables
        assert "order" in schema.tables

    def test_mysql_parses_two_tables(self, parser):
        schema = parser.parse_string(MYSQL_DDL)
        assert "customer" in schema.tables
        assert "order" in schema.tables


class TestDdlParserColumns:
    def test_customer_columns_present(self, parser):
        schema = parser.parse_string(SQL_SERVER_DDL)
        cols = set(schema.tables["customer"].columns.keys())
        assert "customer_id" in cols
        assert "first_name" in cols
        assert "last_name" in cols
        assert "email" in cols

    def test_order_columns_present(self, parser):
        schema = parser.parse_string(SQL_SERVER_DDL)
        cols = set(schema.tables["order"].columns.keys())
        assert "order_id" in cols
        assert "customer_id" in cols
        assert "order_date" in cols
        assert "total" in cols
        assert "status" in cols


class TestDdlParserPrimaryKeys:
    def test_customer_pk_detected(self, parser):
        schema = parser.parse_string(SQL_SERVER_DDL)
        pk = schema.tables["customer"].primary_key
        assert "customer_id" in pk

    def test_order_pk_detected(self, parser):
        schema = parser.parse_string(SQL_SERVER_DDL)
        pk = schema.tables["order"].primary_key
        assert "order_id" in pk

    def test_serial_pk_detected(self, parser):
        schema = parser.parse_string(POSTGRES_DDL)
        pk = schema.tables["customer"].primary_key
        assert "customer_id" in pk


class TestDdlParserForeignKeys:
    def test_explicit_fk_detected(self, parser):
        schema = parser.parse_string(SQL_SERVER_DDL)
        fk_rels = [
            r for r in schema.relationships
            if r.child == "order" and r.parent == "customer"
        ]
        assert len(fk_rels) >= 1

    def test_inline_fk_detected(self, parser):
        schema = parser.parse_string(POSTGRES_DDL)
        fk_rels = [
            r for r in schema.relationships
            if r.child == "order" and r.parent == "customer"
        ]
        assert len(fk_rels) >= 1

    def test_mysql_fk_detected(self, parser):
        schema = parser.parse_string(MYSQL_DDL)
        fk_rels = [
            r for r in schema.relationships
            if r.child == "order" and r.parent == "customer"
        ]
        assert len(fk_rels) >= 1


class TestDdlParserStrategies:
    def test_identity_column_gets_sequence(self, parser):
        schema = parser.parse_string(SQL_SERVER_DDL)
        gen = schema.tables["customer"].columns["customer_id"].generator
        assert gen["strategy"] == "sequence"

    def test_first_name_gets_faker(self, parser):
        schema = parser.parse_string(SQL_SERVER_DDL)
        gen = schema.tables["customer"].columns["first_name"].generator
        assert gen["strategy"] == "faker"

    def test_email_gets_faker(self, parser):
        schema = parser.parse_string(SQL_SERVER_DDL)
        gen = schema.tables["customer"].columns["email"].generator
        assert gen["strategy"] == "faker"
        assert gen.get("provider") == "email"

    def test_date_column_gets_temporal(self, parser):
        schema = parser.parse_string(SQL_SERVER_DDL)
        gen = schema.tables["order"].columns["order_date"].generator
        assert gen["strategy"] == "temporal"

    def test_decimal_column_gets_distribution(self, parser):
        schema = parser.parse_string(SQL_SERVER_DDL)
        gen = schema.tables["order"].columns["total"].generator
        assert gen["strategy"] == "distribution"

    def test_status_column_gets_weighted_enum(self, parser):
        schema = parser.parse_string(SQL_SERVER_DDL)
        gen = schema.tables["order"].columns["status"].generator
        assert gen["strategy"] == "weighted_enum"

    def test_bit_column_gets_weighted_enum(self, parser):
        schema = parser.parse_string(SQL_SERVER_DDL)
        gen = schema.tables["customer"].columns["is_active"].generator
        assert gen["strategy"] == "weighted_enum"


class TestDdlParserScales:
    def test_scales_generated(self, parser):
        schema = parser.parse_string(SQL_SERVER_DDL)
        scales = schema.generation.scales
        assert "small" in scales
        assert "medium" in scales
        assert "large" in scales

    def test_scale_has_all_tables(self, parser):
        schema = parser.parse_string(SQL_SERVER_DDL)
        for scale_name, counts in schema.generation.scales.items():
            assert "customer" in counts
            assert "order" in counts


class TestDdlParserEndToEnd:
    def test_parsed_schema_generates_data(self, parser):
        from sqllocks_spindle import Spindle

        schema = parser.parse_string(SQL_SERVER_DDL)
        result = Spindle().generate(schema=schema, scale="small", seed=42)
        assert "customer" in result.tables
        assert "order" in result.tables
        assert len(result["customer"]) > 0
        assert len(result["order"]) > 0
        errors = result.verify_integrity()
        assert errors == [], f"FK integrity errors: {errors}"

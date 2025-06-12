--Create Database, schema and tables 

CREATE DATABASE FinancialDW;

CREATE SCHEMA financial;

USE FinancialDW

CREATE TABLE financial.dim_card(
    "card_id" INT NOT NULL,
    "card_brand" INT NOT NULL,
    "card_type" INT NOT NULL,
    "has_chip" BIT NOT NULL,
    "num_cards_issued" INT NOT NULL,
    "credit_limit" FLOAT(53) NOT NULL,
    "acct_open_date" VARCHAR(255) NOT NULL,
    "year_pin_last_changed" INT NOT NULL,
    "card_on_dark_web" BIT NOT NULL,
    "expires" VARCHAR(10) NOT NULL
);



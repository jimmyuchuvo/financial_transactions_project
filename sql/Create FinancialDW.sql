--Create Database, schema and tables 

CREATE DATABASE FinancialDW;

CREATE SCHEMA financial;

-- Create the financial.dim_card table
-- This table will store information about credit cards, including their attributes and metadata.
USE FinancialDW;

IF OBJECT_ID('financial.dim_card', 'U') IS NULL
BEGIN
  CREATE TABLE financial.dim_card (
    card_id INT PRIMARY KEY,
    card_brand VARCHAR(50),
    card_type VARCHAR(50),
    credit_limit DECIMAL(18,2),
    has_chip VARCHAR(3),
    num_cards_issued INT,
    card_on_dark_web VARCHAR(3),
    expires DATE,    
    account_open_date DATE,
    year_pin_last_changed SMALLINT,
    -- Metadata columns
    load_timestamp DATETIME2 DEFAULT SYSDATETIME() NOT NULL
  );
END;



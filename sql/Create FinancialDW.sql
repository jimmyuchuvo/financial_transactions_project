-- =========================================================================
-- Step 1️⃣: Create Database and Schema
-- =========================================================================
-- Create the main database and schema for the data warehouse

IF DB_ID('FinancialDW') IS NULL
    CREATE DATABASE FinancialDW;
GO

USE FinancialDW;
GO

IF SCHEMA_ID('financial') IS NULL
    EXEC('CREATE SCHEMA financial');
GO

-- =========================================================================
-- Step 2️⃣: Create Dimension Tables
-- =========================================================================

-- ---------------------------------------------
-- Table: financial.dim_card
-- ---------------------------------------------
-- Stores credit card information such as brand, type, and limits
-- Drop the table only if it exists

USE FinancialDW;
GO
    DROP TABLE IF EXISTS financial.dim_card;
GO

CREATE TABLE financial.dim_card (
    card_key INT NOT NULL IDENTITY(1,1),
    card_id INT NOT NULL UNIQUE,
    user_id INT,
    card_brand VARCHAR(50),
    card_type VARCHAR(50),
    credit_limit DECIMAL(18,2),
    has_chip VARCHAR(20),
    expires DATE,
    account_open_date DATE,
    year_pin_last_changed INT,
    num_cards_issued INT,
    card_on_dark_web VARCHAR(10),
    -- Metadata
    dw_load_timestamp DATETIME2 DEFAULT SYSDATETIME() NOT NULL
);
GO

-- ---------------------------------------------
-- Table: financial.dim_users
-- ---------------------------------------------
-- Stores user demographics and financial metrics
USE FinancialDW;
GO
    DROP TABLE IF EXISTS financial.dim_users;
GO

CREATE TABLE financial.dim_users (
    user_key INT NOT NULL  IDENTITY(1,1),
    user_id INT NOT NULL UNIQUE,
    gender VARCHAR(10),
    current_age INT,
    birth_year SMALLINT,
    birth_month SMALLINT,
    retirement_age SMALLINT,
    total_debt DECIMAL(18,2),
    per_capita_income DECIMAL(18,2),
    yearly_income DECIMAL(18,2),
    num_credit_cards SMALLINT,
    credit_score SMALLINT,
    user_address VARCHAR(100),
    latitude FLOAT,
    longitude FLOAT,
    -- Metadata
    dw_load_timestamp DATETIME2 DEFAULT SYSDATETIME() NOT NULL
);
GO

-- ---------------------------------------------
-- Table: financial.dim_merchant
-- ---------------------------------------------
-- Stores merchant location and category info
USE FinancialDW;
GO
    DROP TABLE IF EXISTS financial.dim_merchant;
GO

CREATE TABLE financial.dim_merchant (
    merchant_key INT NOT NULL IDENTITY(1,1),
    merchant_id INT NOT NULL UNIQUE,
    merchant_state VARCHAR(100),
    merchant_city VARCHAR(100),
    merchant_zip INT,
    mcc INT,
    mcc_description VARCHAR(255),
    -- Metadata
    dw_load_timestamp DATETIME2 DEFAULT SYSDATETIME() NOT NULL
);
GO

-- ---------------------------------------------
-- Table: financial.dim_date
-- ---------------------------------------------
-- Stores calendar-related details for time-based analysis
USE FinancialDW;
GO
DROP TABLE  IF EXISTS financial.dim_date;
GO

CREATE TABLE financial.dim_date (
    date_key BIGINT NOT NULL IDENTITY(1,1),
    year INT,
    quarter INT,
    month INT,
    month_name VARCHAR(50),
    day INT,
    day_name VARCHAR(50),
    day_of_week INT,
    day_type VARCHAR(20),
    hour INT,
    minute_block VARCHAR(20),
    -- Metadata
    dw_load_timestamp DATETIME2 DEFAULT SYSDATETIME() NOT NULL
);
GO

-- =========================================================================
-- Step 3️⃣: Create Fact Table
-- =========================================================================

-- ---------------------------------------------
-- Table: financial.fact_transactions
-- ---------------------------------------------
-- Stores transactional records and foreign keys to dimension tables
-- Data is partitioned by transaction_date for performance
USE FinancialDW;
GO
DROP TABLE IF EXISTS financial.fact_transactions;
GO
CREATE TABLE financial.fact_transactions (
    transaction_id INT NOT NULL UNIQUE,
    transaction_date DATETIME,
    date_key BIGINT NOT NULL,
    user_key INT NOT NULL,
    card_key INT NOT NULL,
    merchant_key INT NOT NULL,
    amount DECIMAL(10, 2),
    card_entry_method VARCHAR(50),
    transaction_error VARCHAR(100),
    -- Metadata
    dw_load_timestamp DATETIME2 DEFAULT SYSDATETIME() NOT NULL
) ON scheme_partition_by_year (transaction_date);
GO
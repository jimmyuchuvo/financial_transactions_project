--Create Database, schema and tables 

CREATE DATABASE FinancialDW;

CREATE SCHEMA financial;

-- Create the financial.dim_card table
-- This table will store information about credit cards, including their attributes and metadata.
GO
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
      dw_load_timestamp DATETIME2 DEFAULT SYSDATETIME() NOT NULL
    );
  END;
-- Create the financial.dim_users table
-- This table will store user information, including demographics, financial status, and metadata.
GO
USE FinancialDW
BEGIN
  IF OBJECT_ID('financial.dim_users', 'U') IS NOT NULL
  BEGIN
      DROP TABLE financial.dim_users;
  END;
  CREATE TABLE financial.dim_users (
    user_id INT PRIMARY KEY,
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
    -- Metadata columns
    dw_load_timestamp DATETIME2 DEFAULT SYSDATETIME() NOT NULL
  )
END;

-- Create the financial.dim_merchant table
GO
USE FinancialDW
BEGIN
  IF OBJECT_ID('financial.dim_merchant', 'U') IS NOT NULL
  BEGIN
      DROP TABLE financial.dim_merchant;
  END;

  CREATE TABLE financial.dim_merchant (
    merchant_key INT PRIMARY KEY,
    merchant_id INT,
    merchant_state VARCHAR(100),
    merchant_city VARCHAR(100),
    merchant_zip INT,
    mcc INT,
    mcc_description VARCHAR(255),
    -- Metadata columns
    dw_load_timestamp DATETIME2 DEFAULT SYSDATETIME() NOT NULL    
  )
END;

 -- Create the financial.dim_date table
USE FinancialDW;
BEGIN
-- Drop the table if it exists
  IF OBJECT_ID('financial.dim_date', 'U') IS NOT NULL
  BEGIN
      DROP TABLE financial.dim_date;
  END;

  -- Create the table
  CREATE TABLE financial.dim_date (
      date_key BIGINT PRIMARY KEY,
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
      -- Metadata columns
      dw_load_timestamp DATETIME2 DEFAULT SYSDATETIME() NOT NULL    
  );

END
 

--create financial.fact_transations table
USE FinancialDW;
BEGIN
-- Drop the table if it exists
  IF OBJECT_ID('financial.fact_transactions', 'U') IS NOT NULL
  BEGIN
      DROP TABLE financial.fact_transactions;
  END;

  -- Create the table
  CREATE TABLE financial.fact_transactions (
      transaction_id INT PRIMARY KEY,
      transaction_date DATETIME,
      user_id INT,
      card_id INT,
      date_key BIGINT,
      merchant_key INT,
      amount DECIMAL(10, 2),
      card_entry_method VARCHAR(50),
      transaction_error VARCHAR(100),
      -- Metadata columns
      dw_load_timestamp DATETIME2 DEFAULT SYSDATETIME() NOT NULL    
  );

END
 

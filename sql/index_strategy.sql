-- =========================================================================
-- ✅ Step 1: Create Clustered Columnstore Index on the Fact Table
-- =========================================================================
-- A clustered columnstore index makes this table read-optimized for reporting and aggregation.
CREATE CLUSTERED COLUMNSTORE INDEX idx_CS_fact_transactions
ON financial.fact_transactions;

-- =========================================================================
-- ✅ Step 2: Add Primary Key Constraints (Clustered Rowstore Indexes) on Dimension Tables
-- =========================================================================
-- These will create clustered rowstore indexes, which are optimal for lookup joins.

ALTER TABLE financial.dim_card
ADD CONSTRAINT PK_dim_card_card_key PRIMARY KEY (card_key);

ALTER TABLE financial.dim_date
ADD CONSTRAINT PK_dim_date_date_key PRIMARY KEY (date_key);

ALTER TABLE financial.dim_merchant
ADD CONSTRAINT PK_dim_merchant_merchant_key PRIMARY KEY (merchant_key);

ALTER TABLE financial.dim_users
ADD CONSTRAINT PK_dim_users_user_key PRIMARY KEY (user_key);




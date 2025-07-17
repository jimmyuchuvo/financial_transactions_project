-- 1️⃣ Check for duplicate transaction IDs
-- transaction_id should be the primary key and unique
SELECT 
    transaction_id,
    COUNT(*) AS total_count
FROM financial.fact_transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1;


-- 2️⃣ Check for NULLs in critical columns
-- Ensures core transaction fields are populated
SELECT 
    COUNT(*) AS total_rows,
    COUNT(transaction_id) AS non_null_transaction_id,
    COUNT(transaction_date) AS non_null_transaction_date,
    COUNT(user_id) AS non_null_user_id,
    COUNT(card_id) AS non_null_card_id,
    COUNT(date_key) AS non_null_date_key,
    COUNT(merchant_key) AS non_null_merchant_key,
    COUNT(amount) AS non_null_amount,
    COUNT(card_entry_method) AS non_null_entry_method,
    COUNT(dw_load_timestamp) AS non_null_dw_timestamp,
    COUNT(transaction_error) AS non_null_transaction_error
FROM financial.fact_transactions;


-- 3️⃣ Check for invalid or negative transaction amounts
-- Amounts should not be negative unless representing a refund (depends on business rules)
SELECT *
FROM financial.fact_transactions
WHERE amount < 0;


-- 4️⃣ Identify unusually transactions (potential outliers)

SELECT TOP 10 *
FROM financial.fact_transactions
ORDER BY amount DESC;

SELECT TOP 10 *
FROM financial.fact_transactions
ORDER BY amount ASC;

-- 5️⃣ Check for future-dated transactions
-- These could indicate a data entry or ETL timestamp issue
SELECT *
FROM financial.fact_transactions
WHERE transaction_date > GETDATE();


-- 6️⃣ Check for missing or unknown card_entry_method values
-- Ensures input method (e.g., chip, swipe, tap) is consistently recorded
SELECT DISTINCT card_entry_method
FROM financial.fact_transactions
ORDER BY card_entry_method;


-- 7️⃣ Analyze frequency of transaction_error types
-- Useful for identifying data anomalies, common failure types, or ETL errors
SELECT 
    transaction_error,
    COUNT(*) AS error_count
FROM financial.fact_transactions
WHERE transaction_error IS NOT NULL AND transaction_error <> ''
GROUP BY transaction_error
ORDER BY error_count DESC;


-- 8️⃣ Check for referential integrity with dim tables (foreign keys)

-- 8a. Check for orphan user_id values not found in dim_users
SELECT DISTINCT ft.user_id
FROM financial.fact_transactions ft
LEFT JOIN financial.dim_users du ON ft.user_id = du.user_id
WHERE du.user_id IS NULL;

-- 8b. Check for orphan card_id values not found in dim_card
SELECT DISTINCT ft.card_id
FROM financial.fact_transactions ft
LEFT JOIN financial.dim_card dc ON ft.card_id = dc.card_id
WHERE dc.card_id IS NULL;

-- 8c. Check for orphan merchant_key values not found in dim_merchant
SELECT DISTINCT ft.merchant_key
FROM financial.fact_transactions ft
LEFT JOIN financial.dim_merchant dm ON ft.merchant_key = dm.merchant_key
WHERE dm.merchant_key IS NULL;

-- 8d. Check for orphan date_key values not found in dim_date
SELECT DISTINCT ft.date_key
FROM financial.fact_transactions ft
LEFT JOIN financial.dim_date dd ON ft.date_key = dd.date_key
WHERE dd.date_key IS NULL;


-- 9️⃣ Check that dw_load_timestamp is not null and recent (optional threshold)
-- Useful for verifying ETL recency and completeness
SELECT *
FROM financial.fact_transactions
WHERE dw_load_timestamp IS NULL;

-- Optional: Check if any transactions loaded more than 30 days ago
SELECT *
FROM financial.fact_transactions
WHERE dw_load_timestamp < DATEADD(DAY, -30, GETDATE());

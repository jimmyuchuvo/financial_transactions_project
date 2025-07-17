
-- 1️⃣ Preview the first 1000 rows in the dim_users table
-- Quick inspection of data structure and contents before proceeding to card-level checks
SELECT TOP 1000 *
FROM financial.dim_users;


-- 🔍 Data Quality Checks for dim_card ------------------------------------

-- 2️⃣ Check for duplicates in the primary identifier (card_id)
-- Ensures uniqueness of card_id, which is expected to be a unique key
SELECT
    card_id,
    COUNT(*) AS total_count
FROM financial.dim_card
GROUP BY card_id
HAVING COUNT(*) > 1;


-- 3️⃣ Count NULL values in card_id
-- Identifies missing or unassigned IDs, which may indicate invalid or incomplete records
SELECT
    COUNT(*) - COUNT(card_id) AS total_null_values
FROM financial.dim_card;


-- 4️⃣ Inspect distinct values in key categorical fields
-- Helps validate that values for card_brand and card_type are clean, expected, and consistent
SELECT DISTINCT
    card_brand
FROM financial.dim_card;

SELECT DISTINCT
    card_type
FROM financial.dim_card;


-- 5️⃣ Check for outliers in credit_limit (e.g., negative values)
-- Credit limits should not be negative; identifies data entry or logic errors
SELECT 
    credit_limit
FROM financial.dim_card
WHERE credit_limit < 0;


-- 6️⃣ Identify top 3 credit limits
-- Detects unusually high values which may be outliers or require review
SELECT TOP 3  
    credit_limit
FROM financial.dim_card
ORDER BY credit_limit DESC;


-- 7️⃣ Identify bottom 3 credit limits
-- Detects extremely low values which may also signal incorrect or suspicious data
SELECT TOP 3 
    credit_limit
FROM financial.dim_card
ORDER BY credit_limit ASC;


-- 8️⃣ Review distinct values in select features
-- Validates categorical fields and flags unusual or unexpected values
SELECT DISTINCT
    num_cards_issued
FROM financial.dim_card;

SELECT DISTINCT
    card_on_dark_web
FROM financial.dim_card;

SELECT DISTINCT
    year_pin_last_changed
FROM financial.dim_card
ORDER BY year_pin_last_changed DESC;


-- 9️⃣ Explore date-related fields for outliers and timeline issues

-- List all distinct expiration years for cards
-- Useful for spotting data entry errors or future-dated anomalies
SELECT DISTINCT
    YEAR(expires) AS distinct_year
FROM financial.dim_card	
ORDER BY distinct_year DESC;

-- List all distinct account opening years
-- Helps verify historical depth and identify questionable years
SELECT DISTINCT
    YEAR(account_open_date) AS distinct_year
FROM financial.dim_card	
ORDER BY distinct_year DESC;

-- Identify logically inconsistent dates: expiration before account open date
-- This would suggest invalid card lifecycle information
SELECT *
FROM financial.dim_card
WHERE expires < account_open_date;


-- 🔢 Check if 'expires' is stored as a valid date
-- Detects improperly typed or malformed date values
SELECT *
FROM financial.dim_card
WHERE TRY_CAST(expires AS DATE) IS NULL;

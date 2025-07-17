-- 1️⃣ Preview the first 1000 rows in the users dimension table
-- Basic inspection of the dataset structure and values
SELECT TOP 1000 *
FROM financial.dim_users;


-- 2️⃣ Check for duplicate user_id entries
SELECT
    user_id,
    COUNT(*) AS total_count
FROM financial.dim_users
GROUP BY user_id
HAVING COUNT(*) > 1;


-- 3️⃣ Count NULL values in the user_id column
-- This checks for missing identifiers, which may indicate incomplete or invalid records
SELECT 
    COUNT(*) - COUNT(user_id) AS total_null_values
FROM financial.dim_users;


-- 4️⃣ Analyze gender distribution
-- Useful for detecting unexpected values, missing categories, or invalid entries
SELECT 
    gender,
    COUNT(*) AS total_count
FROM financial.dim_users
GROUP BY gender;


-- 5️⃣ Distribution of current_age values
-- Helps validate expected age ranges and detect outliers (e.g., negative or unusually high ages)
SELECT
    current_age,
    COUNT(*) AS total_count
FROM financial.dim_users
GROUP BY current_age
ORDER BY current_age;


-- 6️⃣ Cross-check calculated age from birth_year vs. stored current_age
-- Detects anomalies where current_age may be incorrect or misaligned
-- Assumes latest transaction date represents the current reference year
SELECT DISTINCT
    *,
    (date_diff - current_age) AS difference 
FROM (
    SELECT
        DATEDIFF(YEAR, CAST(CAST(birth_year AS VARCHAR) AS DATE),
        (SELECT MAX(CAST(transaction_date AS DATE)) FROM financial.fact_transactions)) AS date_diff,
        current_age
    FROM financial.dim_users
) t;


-- 7️⃣ Frequency of birth_month values
-- Checks for distribution and ensures all months are represented correctly (1–12 only)
SELECT 
    birth_month,
    COUNT(*) AS total_count
FROM financial.dim_users
GROUP BY birth_month
ORDER BY birth_month;


-- 8️⃣ List all distinct retirement_age values
-- Verifies consistency and helps identify unusual or unrealistic retirement ages
SELECT 
    DISTINCT retirement_age
FROM financial.dim_users
ORDER BY retirement_age;


-- 9️⃣ Top 3 users with highest total_debt
-- Useful for detecting outlier or extreme values
SELECT 
    TOP 3 total_debt
FROM financial.dim_users
ORDER BY total_debt DESC;


-- 🔟 Bottom 3 unique total_debt values
-- Reveals lowest debt figures and checks for suspicious values (e.g., negative or zero)
SELECT 
    DISTINCT TOP 3 total_debt
FROM financial.dim_users
ORDER BY total_debt ASC;


-- 1️⃣1️⃣ Top 3 users by per_capita_income
-- Assesses income distribution to find potential high-income outliers
SELECT 
    TOP 3 per_capita_income
FROM financial.dim_users
ORDER BY per_capita_income DESC;


-- 1️⃣2️⃣ Bottom 3 unique per_capita_income values
-- Identifies users with very low (or possibly invalid) income values
SELECT 
    DISTINCT TOP 3 per_capita_income
FROM financial.dim_users
ORDER BY per_capita_income ASC;


-- 1️⃣3️⃣ Top 3 yearly_income values
-- Helps check for income outliers and validate expected max range
SELECT 
    TOP 3 yearly_income
FROM financial.dim_users
ORDER BY yearly_income DESC;


-- 1️⃣4️⃣ Bottom 3 unique yearly_income values
-- Identifies suspiciously low or potentially incorrect values
SELECT 
    DISTINCT TOP 3 yearly_income
FROM financial.dim_users
ORDER BY yearly_income ASC;


-- 1️⃣5️⃣ Validate that per_capita_income does not exceed yearly_income
-- Flags data inconsistency: per_capita income should logically not be higher than total yearly income
SELECT 
    yearly_income,
    per_capita_income
FROM financial.dim_users
WHERE per_capita_income > yearly_income;

SELECT 
	TOP 1000 *
FROM financial.dim_merchant

--check for duplicated
SELECT
	merchant_key
	,COUNT(*) total_count
FROM financial.dim_merchant
GROUP BY merchant_key
HAVING COUNT(*) > 1

--check for null values
SELECT 
	COUNT(*) - COUNT(merchant_key) AS total_null_values
FROM financial.dim_merchant

-- 1️⃣ Check for non-standard merchant_state values 

SELECT DISTINCT 
    merchant_state,
    merchant_city,
    merchant_zip
FROM financial.dim_merchant
WHERE merchant_state = LOWER('online');

-- 2️⃣ Analyze the length of merchant_zip codes
-- This helps detect incomplete or unusually long ZIP codes, indicating possible data entry errors.
SELECT 
    MIN(LEN(merchant_zip)) AS min_length,
    MAX(LEN(merchant_zip)) AS max_length
FROM financial.dim_merchant;

-- 3️⃣ Check length variation of merchant_zip for a specific merchant_state
-- This focuses the ZIP code length analysis on records with merchant_state = 'ONLINE',
-- potentially revealing formatting inconsistencies specific to that subset.
SELECT DISTINCT 
    LEN(merchant_zip)
FROM financial.dim_merchant
WHERE merchant_state = 'ONLINE';

-- 4️⃣ Identify merchant_state values with leading or trailing spaces
-- Leading/trailing whitespace can cause grouping, filtering, and matching issues.
SELECT 
    merchant_state
FROM financial.dim_merchant
WHERE merchant_state <> LTRIM(RTRIM(merchant_state));

-- 5️⃣ Identify mcc_description values with leading or trailing spaces
-- Similar to above, trims inconsistencies in description fields that may affect reporting or categorization.
SELECT 
    mcc_description
FROM financial.dim_merchant
WHERE mcc_description <> LTRIM(RTRIM(mcc_description));

-- 6️⃣ Check frequency distribution of mcc_description
-- This helps verify the consistency and completeness of merchant category descriptions.
-- It can also reveal:
--   - Unexpected or misspelled values (e.g., "Restuarant" vs. "Restaurant")
--   - Overuse of generic descriptions
--   - Potential missing values if some descriptions are very underrepresented

SELECT 
    mcc_description,
    COUNT(mcc_description) AS total_count 
FROM financial.dim_merchant
GROUP BY mcc_description
ORDER BY mcc_description;


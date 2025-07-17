    -- 1️⃣ Check for duplicate date_key values
    -- date_key should be the primary key, so this should return zero rows
    SELECT 
        date_key,
        COUNT(*) AS total_count
    FROM financial.dim_date
    GROUP BY date_key
    HAVING COUNT(*) > 1;


    -- 2️⃣ Check for NULL values in key columns
    -- Ensures all core date parts are populated
    SELECT 
        COUNT(*) AS total_rows,
        COUNT(date_key) AS non_null_date_key,
        COUNT(year) AS non_null_year,
        COUNT(quarter) AS non_null_quarter,
        COUNT(month) AS non_null_month,
        COUNT(day) AS non_null_day,
        COUNT(day_name) AS non_null_day_name,
        COUNT(month_name) AS non_null_month_name,
        COUNT(day_of_week) AS non_null_day_of_week,
        COUNT(day_type) AS non_null_day_type,
        COUNT(hour) AS non_null_hour,
        COUNT(minute_block) AS non_null_minute_block
    FROM financial.dim_date;


    -- 4️⃣ Check for inconsistent or unexpected day names
    -- Ensures only valid weekday names are present (e.g., "Monday", "Tuesday", etc.)
    SELECT DISTINCT day_name
    FROM financial.dim_date
    ORDER BY day_name;


    -- 5️⃣ Check for inconsistent or unexpected month names
    -- Validates month_name contains expected values like "January" to "December"
    SELECT DISTINCT month_name
    FROM financial.dim_date
    ORDER BY month_name;


    -- 6️⃣ Check for logical consistency: day_of_week vs. day_name
    -- Optional: validate that numeric day_of_week matches day_name (e.g., 1 = Sunday)
    -- This is a logic check and assumes U.S. standard (Sunday=1, Monday=2, etc.)
    SELECT 
        day_name,
        day_of_week,
        COUNT(*) AS total_count
    FROM financial.dim_date
    GROUP BY day_name, day_of_week
    ORDER BY day_of_week;


    -- 7️⃣ Check for duplicate combinations of date parts
    -- There should be no repeated combinations of year/month/day/hour/minute_block
    SELECT 
        year, month, day, hour, minute_block,
        COUNT(*) AS total_count
    FROM financial.dim_date
    GROUP BY year, month, day, hour, minute_block
    HAVING COUNT(*) > 1;


    -- 8️⃣ Check for missing values in metadata column
    -- Ensures ETL loaded rows with a valid timestamp
    SELECT *
    FROM financial.dim_date
    WHERE dw_load_timestamp IS NULL;


    -- 9️⃣ Check date range coverage (e.g., for completeness)
    -- Ensures the calendar covers the expected number of years
    SELECT 
        MIN(year) AS min_year,
        MAX(year) AS max_year,
        COUNT(DISTINCT year) AS total_years
    FROM financial.dim_date;

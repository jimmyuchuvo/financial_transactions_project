-- =========================================================================
-- Step 1️⃣: Create Filegroups for Partition Storage
-- =========================================================================
-- These filegroups will store the data for each partition (2017, 2018, 2019)
ALTER DATABASE financialDW ADD FILEGROUP FG_2017;
ALTER DATABASE financialDW ADD FILEGROUP FG_2018;
ALTER DATABASE financialDW ADD FILEGROUP FG_2019;
ALTER DATABASE financialDW ADD FILEGROUP FG_FUTURE;
GO

-- =========================================================================
-- Step 2️⃣: Add Data Files to Each Filegroup
-- =========================================================================
-- Assign .ndf files to physical filegroups for storage segregation
ALTER DATABASE financialDW 
ADD FILE (
  NAME = P_2017, 
  FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\DATA\P_2017.ndf'
) TO FILEGROUP FG_2017;

ALTER DATABASE financialDW 
ADD FILE (
  NAME = P_2018, 
  FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\DATA\P_2018.ndf'
) TO FILEGROUP FG_2018;

ALTER DATABASE financialDW 
ADD FILE (
  NAME = P_2019, 
  FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\DATA\P_2019.ndf'
) TO FILEGROUP FG_2019;
GO

ALTER DATABASE financialDW 
ADD FILE (
  NAME = P_FUTURE, 
  FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\DATA\P_FUTURE.ndf'
) TO FILEGROUP FG_FUTURE;
GO

-- =========================================================================
-- Step 3️⃣: Create Partition Function
-- =========================================================================
-- Defines partition boundaries using full-precision datetimes
CREATE PARTITION FUNCTION partition_by_year (DATETIME)
AS RANGE LEFT FOR VALUES (
  '2017-12-31T23:59:59.997',  -- Data <= 2017
  '2018-12-31T23:59:59.997',  -- Data <= 2018
  '2019-12-31T23:59:59.997'   -- Data <= 2019
);
GO

-- =========================================================================
-- Step 5️⃣: Create Partition Scheme
-- =========================================================================
-- Maps each logical partition to a filegroup. The last one uses PRIMARY for overflow.
CREATE PARTITION SCHEME scheme_partition_by_year
AS PARTITION partition_by_year
TO (FG_2017, FG_2018, FG_2019, FG_FUTURE);
GO

-- =========================================================================
-- Step 6️⃣: Query lists all Partition Scheme
-- =========================================================================
SELECT
    ps.name AS PartitionSchemeName,
    pf.name AS PartitionFunctionName,
    ds.destination_id AS PartitionNumber,
    fg.name AS FilegroupName
FROM sys.partition_schemes ps
JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
JOIN sys.destination_data_spaces ds ON ps.data_space_id = ds.partition_scheme_id
JOIN sys.filegroups fg ON ds.data_space_id = fg.data_space_id;

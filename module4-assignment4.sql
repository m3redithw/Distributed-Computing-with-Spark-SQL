-- Delta Lake

-- Create a new Delta table called fireCallsPartitioned partitioned by Priority.

CREATE OR REPLACE TEMPORARY VIEW fireCallsParquet
USING parquet
OPTIONS (
  path "/mnt/davis/fire-calls/fire-calls-clean.parquet/"
)

-- TODO
CREATE DATABASE IF NOT EXISTS Databricks;
USE Databricks;
DROP TABLE IF EXISTS fireCallsPartitioned;

CREATE TABLE fireCallsPartitioned
USING DELTA 
PARTITIONED BY (Priority)
AS
  SELECT * FROM fireCallsParquet

-- Question 1
-- How many folders were created? 
-- Enter the number of records you see from the output below (include the _delta_log in your count).

%fs ls dbfs:/user/hive/warehouse/databricks.db/firecallspartitioned

-- 9 folders

-- Question 2
-- Delete all the records where City is null. How many records are left in the delta table?

DELETE FROM fireCallsPartitioned WHERE City IS NULL;
SELECT COUNT(*) FROM fireCallsPartitioned

-- 416869

-- Question 3
-- After you deleted all records where the City is null, how many files were removed? 
-- Hint: Look at operationsMetrics in the transaction log using the DESCRIBE HISTORY command.

DESCRIBE HISTORY fireCallsPartitioned

-- 22

-- Question 4
-- There are quite a few missing Call_Type_Group values. Use the UPDATE command to replace any null values with Non Life-threatening.
-- After you replace the null values, how many Non Life-threatening call types are there?

UPDATE fireCallsPartitioned SET Call_Type_Group = "Non Life-threatening" WHERE Call_Type_Group IS NULL;
SELECT COUNT(*) FROM fireCallsPartitioned WHERE Call_Type_Group = "Non Life-threatening"

-- Question 5
-- Travel back in time to the earliest version of the Delta table (version 0). How many records were there?

SELECT COUNT(*) 
FROM fireCallsPartitioned 
  VERSION AS OF 0

-- 417419

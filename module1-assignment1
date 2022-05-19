-- Queries in Spark SQL

-- Create a Table

CREATE TABLE IF NOT EXISTS fireIncidents
USING csv
OPTIONS (
  header "true",
  path "/mnt/davis/fire-incidents/fire-incidents-2016.csv",
  inferSchema "true"
)

-- Question 1
-- What is the first value for 'Incident Number'?
SELECT * FROM fireIncidents
LIMIT 10;
-- 16000003

-- Question 2
-- What is the first value for "Incident Number" on April 4th, 2016?
SELECT * FROM fireIncidents
WHERE `Incident Date` = '04/04/2016'
-- 16037478

-- Question 3
-- Is the first fire call in this table on Brooke or Conor's birthday?
SELECT * FROM fireIncidents
WHERE `Incident Date` LIKE '04/04/%' OR `Incident Date` LIKE '09/27/%'
-- Brooke's

-- Question 4
-- What is the "Station Area" for the first fire call in this table?
SELECT * FROM fireIncidents
WHERE (`Incident Date` LIKE '04/04/%' OR `Incident Date` LIKE '09/27/%') AND `Station Area` >20
-- 29

-- Question 5
-- How many incidents were on Conor's birthday in 2016?
SELECT COUNT(`Incident Number`) FROM fireIncidents
WHERE `Incident Date` = '04/04/2016'
-- 80

-- Question 6
-- How many fire calls had an "Ignition Cause" of "4 act of nature"?
SELECT `Ignition Cause`, COUNT(`Ignition Cause`)
FROM fireIncidents
GROUP BY `Ignition Cause`
-- 5

-- Question 7
-- What is the most common "Ignition Cause"? (Put the entire string)
SELECT `Ignition Cause`, COUNT(`Ignition Cause`) as count
FROM fireIncidents
GROUP BY `Ignition Cause`
ORDER BY count DESC
-- 2 unintentional

-- Question 8
-- What is the total incidents from the two joined tables?
SELECT COUNT(incidents.`incident number`)
FROM fireIncidents as incidents
INNER JOIN fireCalls as calls
ON incidents.Battalion = calls.battalion
-- 847094402

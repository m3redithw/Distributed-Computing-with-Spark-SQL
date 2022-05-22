-- Spark Internals

-- Question 1
-- How many fire calls are in our fireCalls table?

SELECT count(*) FROM fireCalls

-- 240613

-- Question 2
-- How large is our fireCalls dataset in memory? Input just the numeric value (e.g. 51.2) to Coursera.
-- 59.8 MiB

-- Question3 
-- Which "Unit Type" is the most common?
SELECT `Unit Type`, COUNT(`Unit Type`) FROM firecalls
GROUP BY `Unit Type`
ORDER BY COUNT(`Unit Type`)
-- ENGINE

-- Question 4
-- **What type of transformation, wide or narrow, did the GROUP BY and ORDER BY queries result in? **
-- Wide

-- Question 5
-- Looking at the query below, how many tasks are in the last stage of the last job?
-- 1
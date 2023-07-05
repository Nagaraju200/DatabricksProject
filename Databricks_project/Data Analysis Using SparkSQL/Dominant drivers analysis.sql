-- Databricks notebook source
SELECT * FROM f1_presentation.calculated_race_results

-- COMMAND ----------

CREATE TABLE f1_presentation.avg_points_2010_2020
AS 
SELECT driver_name,SUM(correct_points) AS total_points_in_year, COUNT(*) AS total_races , AVG(correct_points) AS average_points
-- dense_rank() over(PARTITION BY driver_name ORDER BY SUM(correct_points) DESC ) as rank
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2010 AND 2020
GROUP BY 1
HAVING total_races > 50
ORDER BY 2 DESC


-- COMMAND ----------

SELECT * FROM f1_presentation.calculated_race_results
WHERE correct_points IS NULL

-- COMMAND ----------



-- Databricks notebook source
CREATE TABLE f1_presentation.dominant_teams_2010_2020
AS
SELECT team_name,COUNT(*) AS total_number_of_races, SUM(correct_points) AS total_points , AVG(correct_points) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2010 AND 2020
GROUP BY 1
HAVING COUNT(*) > 25
ORDER BY 2 DESC

-- COMMAND ----------

SELECT * FROM f1_presentation.dominant_teams_2010_2020

-- COMMAND ----------



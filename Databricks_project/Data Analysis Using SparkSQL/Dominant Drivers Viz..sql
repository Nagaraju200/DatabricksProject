-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Top drivers in the years b/w 2010 and 2020

-- COMMAND ----------

with s1 as(
SELECT race_year,driver_name,SUM(correct_points) AS total_points_in_year, COUNT(*) AS total_races , AVG(correct_points) AS average_points,
dense_rank() over(PARTITION BY race_year ORDER BY AVG(correct_points) DESC, COUNT(*) DESC ) as driver_rank
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2010 AND 2020
GROUP BY 1,2
ORDER BY 1 
)

SELECT * from s1
WHERE driver_rank <=3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Top 10 drivers throughout F1 history

-- COMMAND ----------

with s1 as(
SELECT driver_name,SUM(correct_points) AS total_points, COUNT(*) AS total_races , ROUND(AVG(correct_points),1) AS average_points,
dense_rank() over(ORDER BY AVG(correct_points) DESC, COUNT(*) DESC ) as driver_rank
FROM f1_presentation.calculated_race_results
GROUP BY 1
HAVING COUNT(*) >= 50
ORDER BY 5 DESC
)

SELECT * from s1
WHERE driver_rank <=10 ;

-- COMMAND ----------

--  Despite Michael Schumacher and Lewis Hamilton having taken part in multiple races according to avg points Ayrton Senna and Jackie Stewart are the top drivers through all the years.

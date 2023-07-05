-- Databricks notebook source
USE DATABASE f1_processed

-- COMMAND ----------

CREATE TABLE f1_presentation.calculated_race_results
USING parquet
AS 
SELECT ra.race_year,r.position,r.points,r.rank,d.name AS driver_name,d.nationality,c.name AS team_name, 11- r.position AS correct_points
FROM results r
INNER JOIN drivers d ON r.driver_id = d.driver_id
INNER JOIN constructors c ON r.constructor_id = c.constructor_id
INNER JOIN races ra ON r.race_id = ra.race_id
WHERE r.position <10

-- COMMAND ----------



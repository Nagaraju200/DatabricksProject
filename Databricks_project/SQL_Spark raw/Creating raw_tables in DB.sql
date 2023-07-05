-- Databricks notebook source
-- MAGIC %run "../includes/Configuration"

-- COMMAND ----------

CREATE DATABASE if not exists formula1_raw

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS formula1_raw.circuits(circuitId , INT
circuitRef, String
                     name, String
                     location, String
                     country, String
                     lat, Float
                     lng, Float
                     alt, Integer
                     url, String
)
USING PARQUET
LOCATION f"{raw_folder}/constructors"

-- COMMAND ----------



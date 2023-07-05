# Databricks notebook source
from pyspark.sql.functions import current_timestamp,col
from pyspark.sql import DataFrameReader,DataFrameWriter

# COMMAND ----------

# MAGIC %run "../includes/Configuration"

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------


constructors = spark.read.parquet(f'{processed_folder}/constructors') \
    .withColumnRenamed('name','team')




# COMMAND ----------

circuits = spark.read.parquet(f'{processed_folder}/circuits') \
    .withColumnRenamed('location','circuit_location')

# COMMAND ----------

drivers = spark.read.parquet(f'{processed_folder}/drivers') \
    .withColumnRenamed('name','driver_name') \
    .withColumnRenamed('number','driver_number') \
    .withColumnRenamed('nationality','driver_nationality')

# COMMAND ----------

races = spark.read.parquet(f'{processed_folder}/races') \
    .withColumnRenamed('year','race_year') \
    .withColumnRenamed('name','race_name') \
    .withColumnRenamed('date','race_date')

# COMMAND ----------

results = spark.read.parquet(f'{processed_folder}/results') \
.withColumnRenamed('time','race_time')

# COMMAND ----------

D_R = drivers.join(results,drivers.driver_id == results.driver_id) 
# \
    # .select(drivers.driver_name,drivers.driver_number,drivers.driver_nationality,results.grid,results.fastest_lap,results.race_time,results.points)

# COMMAND ----------

display(D_R)

# COMMAND ----------

D_R_C = D_R.join(constructors, D_R.constructor_id == constructors.constructor_id)

# COMMAND ----------

D_R_C_R = D_R_C.join(races,D_R_C.race_id == races.race_id)

# COMMAND ----------

Final_table = D_R_C_R.join(circuits,D_R_C_R.circuit_id == circuits.circuit_id) \
    .select(circuits.circuit_location, D_R_C_R.race_year,D_R_C_R.race_name,D_R_C_R.race_date,D_R_C_R.driver_name,D_R_C_R.driver_number,D_R_C_R.driver_nationality,D_R_C_R.team,D_R_C_R.grid,D_R_C_R.fastest_lap,D_R_C_R.race_time,D_R_C_R.points,D_R_C_R.position)

# COMMAND ----------

Final_table = Final_table.withColumn('created_date',current_timestamp())

# COMMAND ----------

display(dbutils.fs.ls('/mnt/nagadatalake/processed'))

# COMMAND ----------

display(Final_table)

# COMMAND ----------

Final_table.write.format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------



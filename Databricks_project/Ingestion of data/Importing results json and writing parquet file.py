# Databricks notebook source
# MAGIC %run "../includes/Configuration"

# COMMAND ----------

dbutils.widgets.text("data_source","")
var_data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DateType,FloatType

# COMMAND ----------

results_schema = StructType(fields= [StructField("resultId",IntegerType(),True),
                         StructField("raceId",IntegerType(),False),
                         StructField("driverId",IntegerType(),False),
                         StructField("constructorId",IntegerType(),False),
                         StructField("number",IntegerType(),True),
                         StructField("grid",IntegerType(),False),
                         StructField("position",IntegerType(),True),
                         StructField("positionOrder",IntegerType(),False),
                         StructField("milliseconds",IntegerType(),True),
                         StructField("fastestLap",IntegerType(),True),
                         StructField("rank",IntegerType(),True),
                         StructField("statusId",IntegerType(),False),
                         StructField("points",FloatType(),False),
                         StructField("positionText",StringType(),False),
                         StructField("time",StringType(),True),
                         StructField("fastestLapTime",StringType(),True),
                         StructField("fastestLapSpeed",StringType(),True),
                       
])

# COMMAND ----------

results = spark.read \
.schema(results_schema) \
.json(f'{raw_folder}/results.json')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Rename the columns and clean the data

# COMMAND ----------

from pyspark.sql.functions import col,concat,current_timestamp,lit

# COMMAND ----------

results_renamed = results.withColumnRenamed('resultId' , 'result_id') \
                        .withColumnRenamed('raceId', 'race_id') \
                        .withColumnRenamed('driverId', 'driver_id') \
                        .withColumnRenamed('constructorId', 'constructor_id') \
                        .withColumnRenamed('positionOrder', 'position_order') \
                        .withColumnRenamed('fastestLap', 'fastest_lap') \
                        .withColumnRenamed('statusId', 'status_id') \
                        .withColumnRenamed('positionText', 'position_text') \
                        .withColumnRenamed('fastestLapTime', 'fastest_lap_time') \
                        .withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed') \
                        .withColumn('ingestion_date' , current_timestamp()) \
                        .withColumn('data_source' , lit(var_data_source))
                        

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### Remove unwanted columns

# COMMAND ----------

results_final = results_renamed.drop(col('status_id'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Writing DF into parquet file
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql import DataFrameWriter,DataFrameReader

# COMMAND ----------

results_final.write.mode('overwrite').partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------



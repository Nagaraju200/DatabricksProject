# Databricks notebook source
# MAGIC %run "../includes/Configuration"

# COMMAND ----------

dbutils.widgets.text("data_source","")
var_data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DateType

# COMMAND ----------

lap_times_schema = StructType(fields= [
                         StructField("raceId",IntegerType(),False),
                         StructField("driverId",IntegerType(),False),
                         StructField("lap",IntegerType(),False),
                         StructField("position",IntegerType(),True),
                         StructField("time",StringType(),True),
                         StructField("milliseconds",IntegerType(),True)
])

# COMMAND ----------

lap_times = spark.read \
.schema(lap_times_schema) \
.csv(f'{raw_folder}/lap_times')

# Can also use * for wildcards to search for particular files

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Rename the columns and clean the data

# COMMAND ----------

from pyspark.sql.functions import col,concat,current_timestamp,lit

# COMMAND ----------

lap_times_renamed = lap_times.withColumnRenamed('raceId' , 'race_id') \
                        .withColumnRenamed('driverId', 'driver_id') \
                        .withColumn('ingestion_date' , current_timestamp()) \
                        .withColumn('data_source' , lit(var_data_source)) 
                 

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### Remove unwanted columns

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Writing DF into parquet file
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql import DataFrameWriter,DataFrameReader

# COMMAND ----------

lap_times_renamed.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.laptimes")

# COMMAND ----------



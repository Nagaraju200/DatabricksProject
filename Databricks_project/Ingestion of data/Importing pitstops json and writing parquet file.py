# Databricks notebook source
# MAGIC %run "../includes/Configuration"

# COMMAND ----------

dbutils.widgets.text("data_source","")
var_data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DateType

# COMMAND ----------

pitstops_schema = StructType(fields= [StructField("driverId",IntegerType(),False),
                         StructField("raceId",IntegerType(),False),
                         StructField("lap",IntegerType(),False),
                         StructField("stop",IntegerType(),False),
                         StructField("time",StringType(),False),
                         StructField("duration",StringType(),True),
                         StructField("milliseconds",IntegerType(),True)
])

# COMMAND ----------

pitstops = spark.read \
.schema(pitstops_schema) \
.option("multiLine",True) \
.json(f'{raw_folder}/pit_stops.json')

# COMMAND ----------

display(pitstops)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Rename the columns and clean the data

# COMMAND ----------

from pyspark.sql.functions import col,concat,current_timestamp,lit

# COMMAND ----------

pitstops_renamed = pitstops.withColumnRenamed('raceId' , 'race_id') \
                        .withColumnRenamed('driverId', 'driver_id') \
                        .withColumn('ingestion_date' , current_timestamp())  \
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

pitstops_renamed.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.pitstops")

# COMMAND ----------



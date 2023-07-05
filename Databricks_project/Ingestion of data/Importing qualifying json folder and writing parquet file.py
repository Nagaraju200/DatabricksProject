# Databricks notebook source
# MAGIC %run "../includes/Configuration"

# COMMAND ----------

dbutils.widgets.text("data_source","")
var_data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DateType

# COMMAND ----------

qual_schema = StructType(fields= [
                         StructField("qualifyId",IntegerType(),False),
                         StructField("raceId",IntegerType(),False),
                         StructField("driverId",IntegerType(),False),
                         StructField("constructorId",IntegerType(),False),
                         StructField("number",IntegerType(),False),
                         StructField("position",IntegerType(),True),
                         StructField("q1",StringType(),True),
                         StructField("q2",StringType(),True),
                         StructField("q3",StringType(),True)
])

# COMMAND ----------

qual = spark.read \
.schema(qual_schema) \
.option("multiLine",True) \
.json(f'{raw_folder}/qualifying')

# Can also use * for wildcards to search for particular files

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Rename the columns and clean the data

# COMMAND ----------

from pyspark.sql.functions import col,concat,current_timestamp,lit

# COMMAND ----------

qual_renamed = qual.withColumnRenamed('raceId' , 'race_id') \
                        .withColumnRenamed('driverId', 'driver_id') \
                        .withColumnRenamed('constructorId', 'constructor_id') \
                        .withColumnRenamed('qualifyId', 'qualify_id') \
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

qual_renamed.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------



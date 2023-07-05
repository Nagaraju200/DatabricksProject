# Databricks notebook source
# MAGIC %run "../includes/Configuration"

# COMMAND ----------

dbutils.widgets.text("data_source","")
var_data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("p_date_file","2021-3-21")
v_file_date = dbutils.widgets.get("p_date_file")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DateType

# COMMAND ----------

name_schema = StructType(fields= [StructField("forename" , StringType(), True),
                         StructField("surname",StringType(),True)])

# COMMAND ----------

driver_schema = StructType(fields= [StructField("driverId",IntegerType(),False),
                         StructField("driverRef",StringType(),True),
                         StructField("code",StringType(),True),
                         StructField("nationality",StringType(),True),
                         StructField("url",StringType(),True),
                         StructField("number",IntegerType(),True),
                         StructField("dob",DateType(),False),
                         StructField("name",name_schema,False)
])

# COMMAND ----------

drivers = spark.read \
.schema(driver_schema) \
.json(f'{raw_folder}/{v_file_date}/drivers.json')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Rename the columns and clean the data

# COMMAND ----------

from pyspark.sql.functions import col,concat,current_timestamp,lit

# COMMAND ----------

drivers_renamed = drivers.withColumnRenamed('driverRef' , 'driver_ref') \
                        .withColumnRenamed('driverId', 'driver_id') \
                        .withColumn('ingestion_date' , current_timestamp()) \
                        .withColumn('name' , concat(col("name.forename") , lit(" "), col("name.surname"))) \
                        .withColumn('data_source',lit(var_data_source)) \
                        .withColumn('file_date' , lit(v_file_date))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### Remove unwanted columns

# COMMAND ----------

drivers_final = drivers_renamed.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Writing DF into parquet file
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql import DataFrameWriter,DataFrameReader

# COMMAND ----------

drivers_final.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------



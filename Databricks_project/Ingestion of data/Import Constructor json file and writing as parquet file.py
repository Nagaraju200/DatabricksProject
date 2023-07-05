# Databricks notebook source
# MAGIC %run "../includes/Configuration"

# COMMAND ----------

dbutils.widgets.text("data_source","")
var_data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-3-21")
v_date_file = dbutils.widgets.get("p_file_date")

# COMMAND ----------


from pyspark.sql.functions import concat,current_date,lit,col

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls('/mnt/formula1/rawdata'))

# COMMAND ----------

constructor = spark.read.json(f'{raw_folder}/{v_date_file}/constructors.json')



# COMMAND ----------

constructor_renamed = constructor.withColumnRenamed('constructorId', 'constructor_id')\
.withColumnRenamed('constructorRef' , 'constructor_ref') \
.withColumn('data_source', lit(var_data_source)) \
.withColumn('file_date' , lit(v_date_file))

# COMMAND ----------

from pyspark.sql.types import StringType



# COMMAND ----------

constructor_dropped = constructor_renamed.drop('url')

# COMMAND ----------

from pyspark.sql.functions import current_date

constructor_final = constructor_dropped.withColumn('ingestion_date', current_date())

# COMMAND ----------

from pyspark.sql import DataFrameWriter

constructor_final.write.format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------



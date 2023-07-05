# Databricks notebook source
# MAGIC %run "../includes/Configuration"

# COMMAND ----------

dbutils.widgets.text("data_source","")
var_data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-3-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,FloatType,StringType,DateType,TimestampType
from pyspark.sql.functions import col,lit

# COMMAND ----------

schema_races = StructType([StructField("raceId", StringType(), False),
                     StructField("year", StringType() , True),
                     StructField("round", StringType() , True),
                     StructField("circuitId", StringType() , True),
                     StructField("name", StringType() , True),
                     StructField("date", DateType() , True),
                     StructField("time", StringType() , True),
                     StructField("url", StringType() , True)])



# COMMAND ----------

races = spark.read.csv(f"{raw_folder}/{v_file_date}/races.csv",header=True,schema=schema_races)


# COMMAND ----------

races_cols = races.select(col("raceId"),col("year"),col("round"),col("circuitId"),col("name"),col("date"),col("time"))


# COMMAND ----------

races_renamed = races_cols.withColumnRenamed('raceId' , 'race_id') \
.withColumnRenamed('year','race_year')\
.withColumnRenamed('CircuitId', 'circuit_id') \
.withColumn('data_souce',lit(var_data_source)) \
.withColumn('file_date',lit(v_file_date))


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 4. Adding new column Ingestion date to circuits DF and race_timestamp to race DF

# COMMAND ----------

from pyspark.sql.functions import current_date
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import concat
from pyspark.sql.functions import lit

# COMMAND ----------

races_with_race_timestamp = races_renamed.withColumn('race_timestamp', 
                                                to_timestamp(
                                                    concat(col('date'), 
                                                           lit(' ') , col('time')),'yyyy-MM-dd HH:mm:ss')) 
races_with_race_timestamp = races_with_race_timestamp.withColumn('ingestion_date', current_date())

                                            

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 5. Writing the files to processed folder in mount folder

# COMMAND ----------

from pyspark.sql import DataFrameWriter

# COMMAND ----------

races_with_race_timestamp.write.format("parquet").saveAsTable("f1_processed.races")

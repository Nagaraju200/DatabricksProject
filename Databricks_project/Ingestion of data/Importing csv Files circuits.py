# Databricks notebook source
# MAGIC %run "../includes/Configuration"

# COMMAND ----------

dbutils.widgets.text("data_source","")
var_data_source = dbutils.widgets.get("data_source")


# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-3-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,FloatType,StringType,DateType,TimestampType
from pyspark.sql.functions import col,lit

# COMMAND ----------

schema_circuits = StructType([StructField("circuitId", IntegerType(), False),
                     StructField("circuitRef", StringType() , True),
                     StructField("name", StringType() , True),
                     StructField("location", StringType() , True),
                     StructField("country", StringType() , True),
                     StructField("lat", FloatType() , True),
                     StructField("lng", FloatType() , True),
                     StructField("alt", IntegerType() , True),
                     StructField("url", StringType() , True)])

# COMMAND ----------

circuits = spark.read.csv(f"{raw_folder}/{v_file_date}/circuits.csv",header=True,schema=schema_circuits)

# COMMAND ----------

circuits_cols = circuits.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

circuits_renamed = circuits_cols.withColumnRenamed('CircuitId', 'circuit_id') \
.withColumnRenamed('circuitRef', 'circuit_ref') \
.withColumnRenamed('lat', 'latitude') \
.withColumnRenamed('lng', 'longitude') \
.withColumnRenamed('alt', 'altitude') \
.withColumn('data_source',lit(var_data_source)) \
.withColumn('file_date' , lit(v_file_date))




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

circuits_final = circuits_renamed.withColumns({'ingested_date' : current_date()})

# COMMAND ----------

display(circuits_final)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 5. Writing the files to processed folder in mount folder

# COMMAND ----------

from pyspark.sql import DataFrameWriter

# COMMAND ----------

circuits_final.write.format("parquet").saveAsTable("f1_processed.circuits")


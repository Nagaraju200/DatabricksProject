# Databricks notebook source
# MAGIC %run "../includes/Configuration"

# COMMAND ----------

race_results = spark.read.parquet(f'{presentation_folder}/analysis')

# COMMAND ----------

from pyspark.sql.functions import sum,count,col,when

constructor_results = race_results.groupBy('race_year','team') \
    .agg(sum("points").alias('total_points'),
         count(when(col("position") == 1,True)).alias('wins'))


# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import desc,rank

Window_spec = Window.partitionBy('race_year').orderBy(desc('total_points'),desc('wins'))

constructor_final_table = constructor_results.withColumn('rank' , rank().over(Window_spec))

# COMMAND ----------

constructor_final_table.write.format("parquet").saveAsTable("f1_presentation.constructor_standings")

# COMMAND ----------



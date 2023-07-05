# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Produce driver standings using window functions and group by

# COMMAND ----------

# MAGIC %run "../includes/Configuration"

# COMMAND ----------

from pyspark.sql import DataFrameReader

# COMMAND ----------

race_results = spark.read.parquet(f'{presentation_folder}/analysis')

# COMMAND ----------

display(race_results)

# COMMAND ----------

from pyspark.sql.functions import sum,count,col,when


driver_standings = race_results \
.groupBy("race_year","driver_name","driver_nationality","team") \
.agg(sum("points").alias('total_points'),
     count(when(col("position") == 1,True)).alias('wins'))


# COMMAND ----------

display(driver_standings.filter("race_year = 2020" ))

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import desc,rank

Windowspec =  Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = driver_standings.withColumn('rank' , rank().over(Windowspec))

# COMMAND ----------

display(final_df.filter("race_year == 2020"))

# COMMAND ----------

final_df.write.format("parquet").saveAsTable("f1_presentation.driver_standings")

# COMMAND ----------



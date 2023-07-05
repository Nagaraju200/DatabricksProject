# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.nagadatalake.dfs.core.windows.net",
    "pljW8t1nomd3KbTnVHvJ1RQdJYIChauwH8+mRITm/k4xsCyOwyHDqOsgOYh5arhcWygIEWQODcew+AStH4UECQ=="
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@nagadatalake.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv('abfss://demo@nagadatalake.dfs.core.windows.net/circuits.csv'))

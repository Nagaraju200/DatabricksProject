# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'Nagascope')

# COMMAND ----------

key = dbutils.secrets.get(scope = 'Nagascope' , key='adlskey')

# COMMAND ----------


spark.conf.set("fs.azure.account.key.nagadatalake.dfs.core.windows.net",
                 key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@nagadatalake.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv('abfss://demo@nagadatalake.dfs.core.windows.net/circuits.csv'))

# COMMAND ----------



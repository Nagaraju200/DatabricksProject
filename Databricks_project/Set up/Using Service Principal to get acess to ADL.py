# Databricks notebook source
# Steps to follow
# - Register Azure AD application
# - Generate a secret 
# - Set Spark Config ID
# - Assign role Storage Blob Data Contributor to the data lake

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula-1-scope',key = 'formula1-client-id')

tenant_id = dbutils.secrets.get(scope = 'formula-1-scope',key = 'formula1-tenant-id')

secret = dbutils.secrets.get(scope = 'formula-1-scope',key = 'formula1-secret')

# dbutils.secrets.listScopes()

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.nagadatalake.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.nagadatalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.nagadatalake.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.nagadatalake.dfs.core.windows.net", secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.nagadatalake.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@nagadatalake.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv('abfss://demo@nagadatalake.dfs.core.windows.net/circuits.csv'))

# COMMAND ----------



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

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://raw@nagadatalake.dfs.core.windows.net/",
  mount_point = "/mnt/formula1/rawdata",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1/rawdata"))

# COMMAND ----------

display(spark.read.csv('abfss://demo@nagadatalake.dfs.core.windows.net/circuits.csv'))

# COMMAND ----------



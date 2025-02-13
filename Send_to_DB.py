# Databricks notebook source
import json
from pyspark.sql import SparkSession

# COMMAND ----------

df_gold1 = spark.table("gold.report_dept_hires_above_avg")
df_gold2 = spark.table("gold.report_hired_by_quarter")

# COMMAND ----------

json_content = dbutils.fs.head("dbfs:/FileStore/config.json", 1000)
config = json.loads(json_content)

jdbcUrl = config["jdbc_url"]
jdbcUser = config["jdbc_user"]
jdbcPassword = config["jdbc_password"]

jdbcUrl = "jdbc:postgresql://aws-0-us-west-1.pooler.supabase.com:6543/postgres?user=postgres.neoroijdfwsvlsuujijo&password=vIsmy6-hivrab-wynsem&prepareThreshold=0"

connectionProperties = {
    "user": jdbcUser, 
    "password": jdbcPassword,  
    "driver": "org.postgresql.Driver"
}

# COMMAND ----------


df_gold1.write.jdbc(url=jdbcUrl, table="public.report_dept_hires_above_avg", mode="overwrite", properties=connectionProperties)
df_gold2.write.jdbc(url=jdbcUrl, table="public.report_hired_by_quarter", mode="overwrite", properties=connectionProperties)
print("Data Migration Successful")

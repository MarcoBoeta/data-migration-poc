# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check the tables are stored

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/"))


# COMMAND ----------

spark = SparkSession.builder.appName("DataMigration").getOrCreate()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Load the csv files

# COMMAND ----------

df_hired = spark.read.csv("/FileStore/tables/hired_employees.csv", header=True, inferSchema=True)
df_departments = spark.read.csv("/FileStore/tables/departments.csv", header=False, inferSchema=True)
df_jobs = spark.read.csv("/FileStore/tables/jobs.csv", header=False, inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create tables 

# COMMAND ----------

df_hired.write.format("delta").mode("overwrite").saveAsTable("bronze.hired_employees")
df_departments.write.format("delta").mode("overwrite").saveAsTable("bronze.departments")
df_jobs.write.format("delta").mode("overwrite").saveAsTable("bronze.jobs")

print("Data loaded to the bronze layer")


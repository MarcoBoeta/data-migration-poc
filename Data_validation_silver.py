# Databricks notebook source
from functools import reduce
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, isnan, when, count, lit, current_timestamp
from pyspark.sql.types import IntegerType, StringType, TimestampType

spark = SparkSession.builder.appName("SilverLayer").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the files from bronze layer

# COMMAND ----------

hired_df = spark.table("bronze.hired_employees")
departments_df = spark.table("bronze.departments")
jobs_df = spark.table("bronze.jobs")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Correct column names in tables

# COMMAND ----------


departments_df = departments_df.toDF("department_id", "department")
jobs_df = jobs_df.toDF("job_id", "job")
hired_df = hired_df.withColumnRenamed("Name", "name")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Departments data validation

# COMMAND ----------

dept_valid_cond = (col("department_id").cast(IntegerType()).isNotNull()) & (col("department").isNotNull()) & (col("department") != "")
departments_valid = departments_df.filter(dept_valid_cond)
departments_invalid = departments_df.filter(~dept_valid_cond) \
    .withColumn("error_reason", lit("Error in departments validation: invalid department_id or department"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Jobs data validation

# COMMAND ----------

job_valid_cond = (col("job_id").cast(IntegerType()).isNotNull()) & (col("job").isNotNull()) & (col("job") != "")
jobs_valid = jobs_df.filter(job_valid_cond)
jobs_invalid = jobs_df.filter(~job_valid_cond) \
    .withColumn("error_reason", lit("Error in jobs validation: invalid job_id or job"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hired data validation

# COMMAND ----------

hired_df = hired_df.withColumn("datetime", to_timestamp(col("datetime"), "yyyy-MM-dd'T'HH:mm:ss"))

# COMMAND ----------

basic_hired_cond = (col("id").cast(IntegerType()).isNotNull()) & \
                   (col("name").isNotNull()) & (col("name").rlike("^[A-Za-z ]+$")) & \
                   (col("datetime").isNotNull()) & (col("datetime") <= current_timestamp()) & \
                   (col("department_id").isNotNull()) & (col("job_id").isNotNull())

hired_valid_basic = hired_df.filter(basic_hired_cond)
hired_invalid_basic = hired_df.filter(~basic_hired_cond) \
    .withColumn("error_reason", lit("Error in hired validation: invalid id, name, datetime, department_id or job_id"))

# COMMAND ----------

departments_temp = departments_valid.select(col("department_id").alias("dept_valid"))
hired_with_dept = hired_valid_basic.join(departments_temp, hired_valid_basic.department_id == departments_temp.dept_valid, how="left")
hired_invalid_fk_dept = hired_with_dept.filter(col("dept_valid").isNull()) \
    .withColumn("error_reason", lit("Error in FK: department_id not found in departments"))
hired_valid_fk = hired_with_dept.filter(col("dept_valid").isNotNull())

# COMMAND ----------

jobs_temp = jobs_valid.select(col("job_id").alias("job_valid"))
hired_with_job = hired_valid_fk.join(jobs_temp, hired_valid_fk.job_id == jobs_temp.job_valid, how="left")
hired_invalid_fk_job = hired_with_job.filter(col("job_valid").isNull()) \
    .withColumn("error_reason", lit("Error in FK: job_id not found in jobs"))
hired_valid_final = hired_with_job.filter(col("job_valid").isNotNull())

# COMMAND ----------

hired_invalid_basic = hired_invalid_basic.select("id", "name", "datetime", "department_id", "job_id", "error_reason")
hired_invalid_fk_dept = hired_invalid_fk_dept.select("id", "name", "datetime", "department_id", "job_id", "error_reason")
hired_invalid_fk_job = hired_invalid_fk_job.select("id", "name", "datetime", "department_id", "job_id", "error_reason")

from functools import reduce
from pyspark.sql import DataFrame

def union_all(dfs):
    return reduce(DataFrame.unionByName, dfs)

hired_invalid = union_all([hired_invalid_basic, hired_invalid_fk_dept, hired_invalid_fk_job])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save the invalid data logs

# COMMAND ----------

hired_invalid.write.mode("overwrite").format("delta").saveAsTable("logs.hired_errors")

departments_invalid.write.mode("overwrite").format("delta").saveAsTable("logs.departments_errors")

jobs_invalid.write.mode("overwrite").format("delta").saveAsTable("logs.jobs_errors")

print("Invalid log register created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save silver layer tables

# COMMAND ----------


hired_valid_final.write.mode("overwrite").format("delta").saveAsTable("silver.hired_employees")

departments_valid.write.mode("overwrite").format("delta").saveAsTable("silver.departments")

jobs_valid.write.mode("overwrite").format("delta").saveAsTable("silver.jobs")

print("Silver layer successfully written")


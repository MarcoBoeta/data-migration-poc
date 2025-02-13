# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName("GoldLayer").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read silver layer tables

# COMMAND ----------

hired_silver = spark.table("silver.hired_employees")
departments_silver = spark.table("silver.departments")
jobs_silver = spark.table("silver.jobs")

# COMMAND ----------

hired_silver.createOrReplaceTempView("hired")
departments_silver.createOrReplaceTempView("departments")
jobs_silver.createOrReplaceTempView("jobs")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create first report

# COMMAND ----------

query1 = """
SELECT 
    d.department,
    j.job,
    SUM(CASE WHEN quarter(h.datetime) = 1 THEN 1 ELSE 0 END) AS Q1,
    SUM(CASE WHEN quarter(h.datetime) = 2 THEN 1 ELSE 0 END) AS Q2,
    SUM(CASE WHEN quarter(h.datetime) = 3 THEN 1 ELSE 0 END) AS Q3,
    SUM(CASE WHEN quarter(h.datetime) = 4 THEN 1 ELSE 0 END) AS Q4
FROM hired h
JOIN departments d ON h.department_id = d.department_id
JOIN jobs j ON h.job_id = j.job_id
WHERE year(h.datetime) = 2021
GROUP BY d.department, j.job
ORDER BY d.department, j.job
"""

# COMMAND ----------

try:
  gold_report1 = spark.sql(query1)
except Exception as ex:
    status = "failed"
    description = str(ex)
    print(description)


# COMMAND ----------

gold_report1.write.mode("overwrite").format("delta").saveAsTable("gold.report_hired_by_quarter")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create second report
# MAGIC

# COMMAND ----------

query2 = """
WITH dept_hires AS (
    SELECT 
        d.department_id AS id, 
        d.department, 
        COUNT(*) AS hired
    FROM hired h
    JOIN departments d ON h.department_id = d.department_id
    WHERE year(h.datetime) = 2021
    GROUP BY d.department_id, d.department
)
SELECT id, department, hired
FROM dept_hires
WHERE hired > (SELECT AVG(hired) FROM dept_hires)
ORDER BY hired DESC
"""

# COMMAND ----------

gold_report2 = spark.sql(query2)

# COMMAND ----------

gold_report2.write.mode("overwrite").format("delta").saveAsTable("gold.report_dept_hires_above_avg")

# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName("BackupRestore").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create backup

# COMMAND ----------

gold_tables = {
    "report_hired_by_quarter": "gold.report_dept_hires_above_avg",
    "report_dept_hires_above_avg": "gold.report_hired_by_quarter"
}

backup_base_path = "dbfs:/mnt/backups/gold/"

# COMMAND ----------

for key, table_name in gold_tables.items():
    try:
        df = spark.table(table_name)
        
        backup_path = f"{backup_base_path}{key}_avro"
        
        df.write.format("avro").mode("overwrite").save(backup_path)
        
        print(f"Backup completado para {table_name} en {backup_path}")
    except Exception as e:
        print(f"Error al respaldar {table_name}: {str(e)}")

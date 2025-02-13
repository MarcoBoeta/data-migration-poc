# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName("BackupRestore").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create restore

# COMMAND ----------

gold_tables = {
    "report_hired_by_quarter": "dbfs:/mnt/gold/report_hired_by_quarter",
    "report_dept_hires_above_avg": "dbfs:/mnt/gold/report_dept_hires_above_avg"
}

backup_base_path = "dbfs:/mnt/backups/gold/"
restore_base_path = "dbfs:/mnt/gold_restored/"

# COMMAND ----------

for table_name, original_path in gold_tables.items():
    try:
        backup_path = f"{backup_base_path}{table_name}_avro"
        
        restore_path = f"{restore_base_path}{table_name}"

        restored_df = spark.read.format("avro").load(backup_path)

        restored_df.write.format("delta").mode("overwrite").save(restore_path)
        
        print(f"Restore completado para {table_name} en {restore_path}")
    except Exception as e:
        print(f"Error al restaurar {table_name}: {str(e)}")

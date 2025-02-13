# Data Migration Proof of Concept (PoC)

This project demonstrates a comprehensive approach to migrating historic CSV data into a new SQL database using Databricks Community Edition. The solution uses a medallion architecture (Bronze, Silver, and Gold layers) for data ingestion, validation, transformation, backup/restore, and visualization.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Approach and Methodology](#approach-and-methodology)
- [Tools and Technologies](#tools-and-technologies)
- [Code Snippets and Explanation](#code-snippets-and-explanation)
- [Challenges Encountered](#challenges-encountered)
- [Conclusion](#conclusion)

---

## Project Overview

The goal of this project is to build a PoC for migrating historical data from CSV files to a SQL database. The project requirements include:
- **Data Migration:** Extract data from CSV files and load into a new SQL database.
- **Backup & Restore:** Backup each table in AVRO format and provide a mechanism to restore them.
- **Data Validation & Logging:** Ensure data rules are enforced; invalid records are logged for review.
- **Data Exploration & Metrics:** Provide insights through aggregated reports and visualizations.
- **Documentation & GitHub Integration:** Publish code on GitHub with frequent updates to document the development process.

---

## Architecture

A **Medallion Architecture** was implemented with three layers:

- **Bronze Layer:**  
  Raw data is ingested directly from CSV files and stored as Delta tables.

- **Silver Layer:**  
  Data is cleaned and validated. Invalid records are logged separately.

- **Gold Layer:**  
  Data is aggregated and transformed into reports and metrics required by stakeholders.

Additionally, the solution includes:
- A backup and restore process (using AVRO format).
- Migration of the final data to a PostgreSQL database (using Supabase as the SQL backend).
- Visualization and reporting using BI tools (e.g., Tableau or Power BI).

---

## Approach and Methodology

1. **ETL Process:**  
   - **Extraction:** CSV files are loaded into Spark DataFrames in the Bronze layer.
   - **Transformation:** Data is validated and cleaned in the Silver layer.  
     Invalid records are captured and logged.
   - **Loading:** Clean data is stored as Delta tables in the Gold layer.
   
2. **Backup & Restore:**  
   - Each table in the SQL database is backed up in AVRO format.
   - A restore process reads the AVRO backups and repopulates the tables if necessary.

3. **Data Exploration & Metrics:**  
   - Aggregated metrics are generated from the Gold layer using Spark SQL.
   - Two key reports are created:
     - **Report 1:** Number of employees hired per job and department by quarter.
     - **Report 2:** Departments that hired more employees than the average in 2021.

4. **Database Integration:**  
   - Data from the Gold layer is migrated to a PostgreSQL database (hosted on Supabase).
   - JDBC is used to connect Databricks to the SQL database.

5. **GitHub Integration:**  
   - Code is published to a GitHub repository with frequent commits documenting the development process.
   - Secret management is implemented (or alternatives such as environment variables and external configuration files) to protect sensitive information.

---

## Tools and Technologies

- **Apache Spark & Databricks Community Edition:**  
  Used for ETL, data transformation, and building Delta tables.
  
- **Delta Lake:**  
  Provides ACID transactions, versioning, and efficient data storage.

- **PostgreSQL (Supabase):**  
  Chosen as the target SQL database for its robustness, scalability, and open-source nature.

- **JDBC:**  
  For connecting Databricks to PostgreSQL.

- **Git & GitHub:**  
  For version control and to showcase the development process.

---

## Code Snippets and Explanation

### **Example: Reading Raw Data and Storing in Bronze Layer**
```
python
# Read CSV files into DataFrames
df_hired = spark.read.csv("/FileStore/tables/hired_employees.csv", header=True, inferSchema=True)
df_departments = spark.read.csv("/FileStore/tables/departments.csv", header=True, inferSchema=True)
df_jobs = spark.read.csv("/FileStore/tables/jobs.csv", header=True, inferSchema=True)

# Save as Delta tables in the Bronze layer
df_hired.write.format("delta").mode("overwrite").saveAsTable("bronze.hired_employees")
df_departments.write.format("delta").mode("overwrite").saveAsTable("bronze.departments")
df_jobs.write.format("delta").mode("overwrite").saveAsTable("bronze.jobs")
```
### **Example: Validating and Logging Data in Silver Layer**
```
from pyspark.sql.functions import col, to_timestamp, current_timestamp, lit

# Basic validation for hired_employees
df_hired_clean = df_hired.filter(
    col("id").isNotNull() & 
    col("name").rlike("^[A-Za-z ]+$") & 
    (to_timestamp(col("datetime"), "yyyy-MM-dd'T'HH:mm:ss") <= current_timestamp())
)
df_hired_invalid = df_hired.subtract(df_hired_clean).withColumn("error_reason", lit("Invalid record"))
# Save clean data and log errors
df_hired_clean.write.format("delta").mode("overwrite").saveAsTable("silver.hired_employees")
df_hired_invalid.write.format("delta").mode("overwrite").saveAsTable("logs.hired_errors")
```
### **Example: Migrating Data to PostgreSQL**
```
# Retrieve data from Gold layer
df_gold = spark.table("gold.report_dept_hires_above_avg")

# Use secrets or environment variables for sensitive information
jdbcUrl = "jdbc:postgresql://aws-0-us-west-1.pooler.supabase.com:6543/postgres?sslmode=require&prepareThreshold=0"
connectionProperties = {
    "user": "postgres.pftimcjeovkvebhkcbev",
    "password": "cokge9-piwbyj-cunQaj",
    "driver": "org.postgresql.Driver"
}

# Write DataFrame to PostgreSQL
df_gold.write.jdbc(url=jdbcUrl, table="public.report_dept_hires_above_avg", mode="overwrite", properties=connectionProperties)
```
---

## Challenges Encountered

### Connection Issues
Several challenges were encountered while establishing JDBC connections to the SQL database, having as first option using Railway to store the database. Issues such as prepared statement conflicts were resolved by disabling the prepared statement cache (`prepareThreshold=0`).

### Secret Management in Community Edition
Due to limitations in Git integration for secret scopes in Databricks Community Edition, external configuration files were used as an alternative to protect sensitive information.

---

## Conclusion

This PoC demonstrates a robust approach to data migration by leveraging:
- **Databricks for ETL,** ensuring scalable and efficient data processing,
- **Delta Lake for reliable storage** with ACID properties and versioning,
- **PostgreSQL for the target SQL database,** chosen for its robustness and scalability.

The solution also incorporates comprehensive data validation, backup/restore mechanisms, and visualization. The project is fully documented and version-controlled via GitHub, showcasing best practices in data engineering.

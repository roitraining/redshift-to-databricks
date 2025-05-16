# Databricks notebook source
# MAGIC %md
# MAGIC ### Create Materialized View
# MAGIC This notebook is to create a materialized view based on successful creation of the Venue and Sales table.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Install the required python libraries and restart the kernel for the packages to be available.

# COMMAND ----------

# MAGIC %md
# MAGIC The following cells are designed to make a different schema under the classroom catalog for each student.   Since the student id is likely to be an email address the following will replace expected special characters in email addresses with underscores. 
# MAGIC
# MAGIC This is for LAB purposes only.

# COMMAND ----------

import re

# Get the user's email from the Databricks context
user_email = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

# Extract the portion before the @
safe_user = user_email.split('@')[0]

# Optionally sanitize the username (e.g., replace dots)
safe_user = safe_user.replace('.', '_')

print(f"Original user: {user_email}")
print(f"Safe user ID: {safe_user}")

# COMMAND ----------

# MAGIC %md
# MAGIC Set the schema for future cells to the schema created above.

# COMMAND ----------


uc_schema_name = f"{safe_user}"
print(f"UC schema name: {uc_schema_name}")
# Optional: specify catalog if using Unity Catalog
catalog_name = "classroom"  # replace with your catalog
full_schema_name = f"{catalog_name}.{uc_schema_name}"
spark.sql("USE CATALOG classroom")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {uc_schema_name}")
spark.catalog.setCurrentDatabase(uc_schema_name)

# COMMAND ----------

# MAGIC %md
# MAGIC Verify the schema was created successfully and is the current schema going forward in this notebook.  

# COMMAND ----------

catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
schema = spark.sql("SELECT current_schema()").collect()[0][0]

print(f"Current catalog: {catalog}")
print(f"Current schema: {schema}")


# COMMAND ----------

# MAGIC %md
# MAGIC Dropping our LAB table if it exists for lab consistency.  In a production scenario you would likely not drop the table each time.  

# COMMAND ----------

#table_name = "venue_sales_materialized_view"
#ull_table_name = f"{uc_schema_name}.{table_name}"
#print(full_table_name)

# Clean up the table if it exists
#sql_query = f"""
#DROP VIEW IF EXISTS {full_table_name}
#"""

# Run the query using spark.sql
#spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC Here we are using the tables created in previous process to create a Materialized View.
# MAGIC
# MAGIC Read more about materialized views here:
# MAGIC https://docs.databricks.com/aws/en/dlt/dbsql/materialized
# MAGIC
# MAGIC Notice that spark jobs are created to run processing in parrallel.

# COMMAND ----------

table_name = "venue_sales_view"
sales_table = "sales_parquet_worklow"
event_table = "event_parquet_worklow"
catalog_name = "classroom"

full_table_name = f"{catalog_name}.{uc_schema_name}.{table_name}"
full_sales_table_name = f"{catalog_name}.{uc_schema_name}.{sales_table}"
full_event_table_name = f"{catalog_name}.{uc_schema_name}.{event_table}"
print(full_table_name)



# Construct the SQL query using f-string
sql_query = f"""
CREATE OR REPLACE VIEW {full_table_name}
AS SELECT
    e.eventid,
    e.eventname,
    SUM(s.qtysold) AS total_qtysold
FROM
    {full_sales_table_name} s
JOIN
   {full_event_table_name} e ON s.eventid = e.eventid
GROUP BY
    e.eventid,
    e.eventname

"""

# Run the query using spark.sql
spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC Run one final check to validate the view was successfully populated.

# COMMAND ----------

table_name = "venue_sales_view"
full_table_name = f"{uc_schema_name}.{table_name}"
print(full_table_name)
print("reading parquet table")
# Construct the SQL query using f-string
sql_query = f"""
select * from  {full_table_name}
"""

# Run the query using spark.sql
results_df = spark.sql(sql_query)
results_df.show()
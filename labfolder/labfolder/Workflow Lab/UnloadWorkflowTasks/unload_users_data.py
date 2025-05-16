# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Extraction Notebook
# MAGIC This notebook is designed to connect to the Redshift cluster and extract users table data in parquet format and store the data in an S3 bucket.
# MAGIC
# MAGIC Once the the data has been loaded a Delta table will be created in the students scehma under the classroom catalog in Unity Catalog
# MAGIC
# MAGIC The user and password for the RedShift table is stored and retrived from a secret in AWS Secrets Manager.
# MAGIC
# MAGIC This lab will use the JDBC driver for Redshift.  The Redshift cluster is accessible via a public internet address via an AWS Elastic IP address.  Alternatively we could have connected via Private link from the Databricks private subnet to the Redshift private subnet.
# MAGIC
# MAGIC Federated access would have also been an alternative to JDBC.
# MAGIC
# MAGIC This is all to illustrate that there are other options available according to your design decisions.

# COMMAND ----------

# MAGIC %md
# MAGIC Install the required python libraries and restart the kernel for the packages to be available.

# COMMAND ----------

!pip install jaydebeapi
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
dbutils.library.restartPython()

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
# MAGIC Further setting environment varables for this LAB for the AWS configuaryion of our sample Redshift cluster

# COMMAND ----------

region = "us-east-1"
clusterid = "redshift-cluster-2"
hostname = "redshift-cluster-2.cytrkrfafkrq.us-east-1.redshift.amazonaws.com"
port = "5439"
database = "dev"
iam_role = "arn:aws:iam::633690268896:role/service-role/AmazonRedshift-CommandsAccessRole-20250505T135309"
tempdir = "s3://redshiftprocessing/tmp"
iam_role = "arn:aws:iam::633690268896:role/service-role/AmazonRedshift-CommandsAccessRole-20250505T135309"

# COMMAND ----------

#dbutils.widgets.text("schema_name", "classroom_schema_fredgraichen_outlook_com")
#dbutils.widgets.text("table_name", "users_delta_51")
#schema_name = dbutils.widgets.get("schema_name")
#table_name = dbutils.widgets.get("table_name")
#full_table_name = f"{uc_schema_name}.{table_name}"
#print(full_table_name)




# COMMAND ----------

# MAGIC %md
# MAGIC Dropping our LAB table if it exists for lab consistency.  In a production scenario you would likely not drop the table each time.  

# COMMAND ----------

table_name = "users_parquet_worklow"
full_table_name = f"{uc_schema_name}.{table_name}"
print(full_table_name)

# Clean up the table if it exists
sql_query = f"""
DROP TABLE IF EXISTS {full_table_name}
"""

# Run the query using spark.sql
spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC For LAB purposes the following cell is verifying that this notebook and associated compute have the correct IAM permissions to access the S3 bucket that will be used for the UNLOAD operation.

# COMMAND ----------

# Check this cluster can see the tempdir bucket.
# The tempdir itself may not exist, so just list the bucket.
tempdir = "s3://redshiftprocessing/"
import re
_l = tempdir
x = re.search('(s3[an]?://[^/]+)', _l)
print(x.groups()[0])
dbutils.fs.ls(x.groups()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Check your Redshift connection
# MAGIC
# MAGIC Netcat your Redshift cluster to ensure that you have correctly configured your network settings using the public endpoint and port number. The default port is `5439`
# MAGIC
# MAGIC You should return:
# MAGIC ** `Connection to redshift-cluster-2.cytrkrfafkrq.us-east-1.redshift.amazonaws.com (98.83.246.212) 5439 port [tcp/*] succeeded!\n`**

# COMMAND ----------

import subprocess
result = subprocess.run(['nc', '-zv', hostname, port], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
print(result.stdout)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Add your Redshift credentials (using secrets)
# MAGIC
# MAGIC - 
# MAGIC - Use [JDBC secrets](https://docs.databricks.com/user-guide/secrets/example-secret-workflow.html#example-secret-workflow) to set up your Redshift credentials (`redshift-password`, and `redshift-iam_role`), which will be used in the following `get_table()` function (e.g. `databricks secrets put --scope rd-jdbc --key redshift-password`)
# MAGIC
# MAGIC We have the username and password stored as AWS secrets which we well extract below.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Use boto3 (Python SDK for AWS) to extract the username and secret.

# COMMAND ----------

import boto3
from botocore.exceptions import ClientError
import json


def get_secret():

    secret_name = "rsprofiler"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        secret_dict = json.loads(get_secret_value_response['SecretString'])
        
        secret_password = secret_dict['password']
        secret_username = secret_dict['username']
        print(f"Secret username: {secret_username}")
        return secret_username, secret_password
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    

rs_username, rs_password = get_secret()


# COMMAND ----------

# MAGIC %md
# MAGIC # Data Extract from Redshift using JDBC

# COMMAND ----------

# MAGIC %md
# MAGIC The {safe_user} variable was derived from the username of the current user. It is used to create a unique directory in the S3 bucket for this user's data.
# MAGIC
# MAGIC You can ignore the message "SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder"" and "SLF4J: Defaulting to no-operation (NOP) logger implementation" if you see them. They are related to the logging library used by the JDBC driver and can be safely ignored.

# COMMAND ----------

import jaydebeapi

# JDBC connection properties
jdbc_url = "jdbc:redshift://redshift-cluster-2.cytrkrfafkrq.us-east-1.redshift.amazonaws.com:5439/dev"
username = rs_username
password = rs_password
driver_class = "com.amazon.redshift.jdbc42.Driver"
jdbc_jar_path = "/Volumes/classroom/classroom/jdbc/redshift-jdbc42-2.1.0.32.jar"

# Connect using jaydebeapi
conn = jaydebeapi.connect(
    driver_class,
    jdbc_url,
    [username, password],
    jdbc_jar_path
)

cursor = conn.cursor()

try:
    # UNLOAD command
    unload_sql = f"""
        UNLOAD ('SELECT * FROM users')
        TO 's3://redshiftprocessing/{safe_user}/users/parquet/'
        IAM_ROLE 'arn:aws:iam::633690268896:role/service-role/AmazonRedshift-CommandsAccessRole-20250505T135309'
        ALLOWOVERWRITE 
        FORMAT AS PARQUET;
    """
    print(unload_sql)
    cursor.execute(unload_sql)
    print("UNLOAD completed successfully.")

except Exception as e:
    print("Error executing UNLOAD:", e)

finally:
    cursor.close()
    conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC Verify that the data was unloaded to S3 bucket for this user as parquet files

# COMMAND ----------

import boto3

# Initialize the S3 client
region = "us-east-1"  # Replace with your region
s3 = boto3.client('s3', region_name=region)

bucket_name = 'redshiftprocessing'
prefix = safe_user +'/users/parquet'  # Make sure to end with '/' for folder
print("s3 prefix ", prefix)
response = s3.list_objects_v2(
    Bucket=bucket_name,
    Prefix=prefix
)

# Print contents
if 'Contents' in response:
    for obj in response['Contents']:
        print(obj['Key'])
else:
    print("No objects found in this prefix.")

# COMMAND ----------

# MAGIC %md
# MAGIC The following cell is used to determine what the existing schema is for the Redshift table.  This information than is used to define the schema for the target Delta table in Databricks Unity Catalog

# COMMAND ----------

import jaydebeapi

# JDBC connection properties
jdbc_url = "jdbc:redshift://redshift-cluster-2.cytrkrfafkrq.us-east-1.redshift.amazonaws.com:5439/dev"
username = rs_username
password = rs_password
driver_class = "com.amazon.redshift.jdbc42.Driver"
jdbc_jar_path = "/Volumes/classroom/classroom/jdbc/redshift-jdbc42-2.1.0.32.jar"

# Connect using jaydebeapi
conn = jaydebeapi.connect(
    driver_class,
    jdbc_url,
    [username, password],
    jdbc_jar_path
)

cursor = conn.cursor()
table_name = 'users'
schema_name = 'public'

schema_sql = f"""
    SELECT 
        column_name, 
        data_type, 
        character_maximum_length,
        numeric_precision,
        is_nullable
    FROM information_schema.columns
    WHERE table_schema = '{schema_name}'
      AND table_name = '{table_name}'
    ORDER BY ordinal_position
"""

try:
    # UNLOAD command
  
  cursor.execute(schema_sql)
  columns = cursor.fetchall()
  print(f"Schema for table {schema_name}.{table_name}:")
  for col in columns:
   print(f"Column: {col[0]}, Type: {col[1]}, Nullable: {col[4]}")

except Exception as e:
    print("Error executing UNLOAD:", e)

finally:
    cursor.close()
    conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Datbricks and Redshift support many of the same datatypes but have different syntax for defining each.  In the case where there may not be supported data types in Databricks either ETL or SQL processing (cast) maybe be required before or during data loading.
# MAGIC
# MAGIC https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-datatypes

# COMMAND ----------

# MAGIC %md
# MAGIC You can ignore the "filedescriptor out of range in select()" message as this is likely an artifcact from the compute instance type and the number of files being processed.

# COMMAND ----------

table_name = "users_parquet_worklow"
full_table_name_parquet = f"{uc_schema_name}.{table_name}"
print(full_table_name_parquet)
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType

# Define the schema
schema = StructType([
    StructField("userid", IntegerType(), True),
    StructField("username", StringType(), True),
    StructField("firstname", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
     StructField("email", StringType(), True),
    StructField("likesports", BooleanType(), True),
    StructField("liketheater", BooleanType(), True),
    StructField("likeconcerts", BooleanType(), True),
    StructField("likejazz", BooleanType(), True),
    StructField("likeopera", BooleanType(), True),
    StructField("likerock", BooleanType(), True),
    StructField("likevegas", BooleanType(), True),
    StructField("likebroadway", BooleanType(), True),
    StructField("likemusicals", BooleanType(), True),
   
])

# Create an empty DataFrame with the schema
try:
   df = spark.createDataFrame([], schema)
except Exception as e:
    print(f"Error creating dataframe: {e}")
try:
# Save it as a Delta table if it doesn't exist
   if not spark.catalog.tableExists(full_table_name_parquet):
       df.write.format("delta").saveAsTable(full_table_name_parquet)
except Exception as e:
    print(f"Error creating users_parquet_worflow: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC Here we are using the Databricks Copy INTO command to load the data from S3 into the Delta table.
# MAGIC
# MAGIC Other data formats are supported but a comman datatype for this scenario is parquet.
# MAGIC
# MAGIC Read more about this command here:
# MAGIC
# MAGIC https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/copy-into/
# MAGIC
# MAGIC Notice that spark jobs are created to run processing in parrallel.

# COMMAND ----------

table_name = "users_parquet_worklow"
full_table_name = f"{uc_schema_name}.{table_name}"
print(full_table_name)

# Construct the SQL query using f-string
sql_query = f"""
COPY INTO {full_table_name}
FROM 's3://redshiftprocessing/{safe_user}/users/parquet/'
FILEFORMAT = PARQUET
COPY_OPTIONS ('mergeSchema' = 'True')
"""

# Run the query using spark.sql
spark.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC Run one final check to validate the Detla table was successfully populated.

# COMMAND ----------

table_name = "users_parquet_worklow"
full_table_name = f"{uc_schema_name}.{table_name}"
print(full_table_name)
print("reading parquet table")
# Construct the SQL query using f-string
sql_query = f"""
select userid, username, city, state from  {full_table_name}
"""

# Run the query using spark.sql
results_df = spark.sql(sql_query)
results_df.show()
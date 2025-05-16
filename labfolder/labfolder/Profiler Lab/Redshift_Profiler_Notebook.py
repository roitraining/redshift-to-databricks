# Databricks notebook source
# MAGIC %md
# MAGIC #Redshift Profiler Notebook (v2.2.1)
# MAGIC
# MAGIC This notebook profiles and visualizes your Redshift warehouse usage by pulling metrics from [AWS system tables](https://docs.aws.amazon.com/redshift/latest/dg/cm_chap_system-tables.html). 
# MAGIC
# MAGIC
# MAGIC
# MAGIC You will see the following metrics:
# MAGIC - Warehouse statement type breakdown
# MAGIC - ETL vs. BI queries by hour of day
# MAGIC - ETL vs. BI queries by day of month
# MAGIC - Top 20 queries by CPU time
# MAGIC - Top 20 queries by run time
# MAGIC - Average/max query wait time by hour
# MAGIC - Average concurrent users by hour
# MAGIC - Database idle time percentage (overall, BI and non-BI only)
# MAGIC
# MAGIC Enter your Redshift credentials using secrets and Run the entire notebook!
# MAGIC
# MAGIC > **Note:** <br>
# MAGIC > By default, Redshift system tables save the past 5-7 days' worth of data. To get a more accurate report of your Redshift usage, run a periodic data dump of your system tables to S3.
# MAGIC > Need Read Access to the following tables:
# MAGIC - stl_query
# MAGIC - stl_query_metrics
# MAGIC - stl_wlm_query
# MAGIC - stv_node_storage_capacity
# MAGIC - stv_partitions
# MAGIC - stv_mv_info
# MAGIC - svv_table_info
# MAGIC - svv_external_tables
# MAGIC - pg_catalog.pg_views
# MAGIC - stv_mv_info
# MAGIC - pg_catalog.pg_namespace 
# MAGIC - pg_catalog.pg_proc
# MAGIC - pg_catalog.pg_user
# MAGIC
# MAGIC Redshift attached IAM role needs to have read/write access to the S3 bucket where the temp directory is located for unload data from Redshift.
# MAGIC
# MAGIC Spark cluster attached instance profile needs to have read access to the S3 bucket where the temp directory is located and cloudwatch:GetMetricStatistics permission.

# COMMAND ----------

# MAGIC %md
# MAGIC The following cells are designed to make a different schema under the classroom catalog for each student.   Since the student id is likely to be an email address the following will replace expected special characters in email addresses with underscores. 
# MAGIC
# MAGIC This is for LAB purposes only.

# COMMAND ----------

import re

user_email = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
safe_user = user_email.replace('@', '_')
safe_user = safe_user.replace('.', '_')

print(f"Original user: {user_email}")
print(f"Safe user ID: {safe_user}")

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
# MAGIC The following cells sets variables for the remainder of this notebook pertaining to the Redshift and AWS services used.
# MAGIC
# MAGIC This is for LAB purposes only.

# COMMAND ----------

region = "us-east-1"
clusterid = "redshift-cluster-2"
hostname = "redshift-cluster-2.cytrkrfafkrq.us-east-1.redshift.amazonaws.com"
port = "5439"
database = "dev"
tempdir  = f"s3://redshiftprocessing/tmp/{safe_user}"
iam_role = "arn:aws:iam::633690268896:role/service-role/AmazonRedshift-CommandsAccessRole-20250505T135309"

# COMMAND ----------

# MAGIC %md
# MAGIC For LAB purposes the following cell is verifying that this notebook and associated compute have the correct IAM permissions to access the S3 bucket that will be used for the profile processing steps.
# MAGIC
# MAGIC The prefix is different for each user based on your user id.

# COMMAND ----------

# Check this cluster can see the tempdir bucket.
# The tempdir itself may not exist, so just list the bucket.
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
# MAGIC ** `Connection to redshift-cluster-2.cytrkrfafkrq.us-east-1.redshift.amazonaws.com (98.83.246.212) 5439 port [tcp/*] succeeded!\n`**`

# COMMAND ----------

import subprocess
result = subprocess.run(['nc', '-zv', hostname, port], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
print(result.stdout)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Retrieve Redshift credentials using Boto3 and AWS Secrets Manager 
# MAGIC
# MAGIC - 
# MAGIC - Use [JDBC secrets](https://docs.databricks.com/user-guide/secrets/example-secret-workflow.html#example-secret-workflow) to set up your Redshift credentials (`redshift-password`, and `redshift-iam_role`), which will be used in the following `get_table()` function (e.g. `databricks secrets put --scope rd-jdbc --key redshift-password`)
# MAGIC
# MAGIC We have the username and password stored as AWS secrets which we well extract below.

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
# MAGIC
# MAGIC #### Create schema to store tables created by the profiler tasks
# MAGIC
# MAGIC - For LAB purposes the following cell is verifying that this notebook and associated compute have the correct IAM permissions to access the S3 bucket that will be used for the profile processing steps.
# MAGIC
# MAGIC The schema is different for each user based on your user id.
# MAGIC
# MAGIC Example:  redshift_profiler_run_someuser_outlook_com_1747151082
# MAGIC
# MAGIC Note: Since a timestamp is appended at the end, running this notebook will create a new schema and new table
# MAGIC

# COMMAND ----------

import time
# Create a new schema for the profiler run and use it
redshift_profiler_schema = f"redshift_profiler_run_{safe_user}_{int(time.time())}"
print(redshift_profiler_schema)
spark.sql(f"create database if not exists {redshift_profiler_schema}")
spark.sql(f"use {redshift_profiler_schema}")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Define JDBC functions for system table queries.

# COMMAND ----------

#get the Reshift metrics tables with this function
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

def get_table(table_name):
    jdbcUrl = f"jdbc:redshift://{hostname}:{port}/{database}"
    #print("tempdir", tempdir)
  #read in the Redshift table
    table = (spark.read 
       .format("com.databricks.spark.redshift") 
       .option("url", jdbcUrl) 
       .option("dbtable", table_name) 
       .option("user",rs_username)
       .option("password", rs_password)
       .option("aws_iam_role", iam_role) 
       .option("tempdir", tempdir.replace("s3n", "s3a")) 
       .load())
  
  #convert all timestamp columns to strings
    for column in table.dtypes:
       if column[1] == 'timestamp':
          table = table.withColumn(column[0], col(column[0]).cast(StringType()))

    # create a table in Databricks workspace
  #  table.take(10)
    table.write.mode("overwrite").saveAsTable(table_name)
    
    
def get_table_query(query, table_name):
  
    #jdbcUrl = f"jdbc:redshift://{hostname}:{port}/{database}?user={username}&password={password}"
    jdbcUrl = f"jdbc:redshift://{hostname}:{port}/{database}"

  
  #read in the Redshift table
    table = (spark.read 
       .format("com.databricks.spark.redshift") 
       .option("url", jdbcUrl) 
       .option("query", query)
       .option("user", rs_username)
       .option("password", rs_password)
       .option("aws_iam_role", iam_role) 
       .option("tempdir", tempdir.replace("s3n", "s3a")) 
       .load())
    
  #convert all timestamp columns to strings
    for column in table.dtypes:
       if column[1] == 'timestamp':
          table = table.withColumn(column[0], col(column[0]).cast(StringType()))
  
   # create a table in Databricks workspace
  #  table.take(10)
    table.write.mode("overwrite").saveAsTable(table_name)    

# COMMAND ----------

# MAGIC %md
# MAGIC Define AWS client functions to retrieve CPU metrics.

# COMMAND ----------

# DBTITLE 1,Function to get CPU utilization
def get_cpu_util(clusterid, node_type, startdate, enddate):
  print("get_cpu_util")
  print("clusterid: " + clusterid)
  print("node_type: " + node_type)
  print("startdate: " + startdate)
  print("startdate: " + enddate)
  response = client.get_metric_statistics(
            Namespace="AWS/Redshift",
            MetricName="CPUUtilization",
            Dimensions=[
                {
                    "Name": "ClusterIdentifier",
                    "Value": clusterid
                },
                {
                    "Name": "NodeID",
                    "Value": node_type
                },
            ],
            StartTime=datetime(int(startdate[:4]),int(startdate[4:6]),int(startdate[6:])),
            EndTime=datetime(int(enddate[:4]),int(enddate[4:6]),int(enddate[6:])),
            Period=300, # 5mins
            Statistics=[
                "Average",
            ],
            Unit='Percent'
    )
  print("response: " + str(response))
  new_response = sorted(response['Datapoints'], key=itemgetter('Timestamp'))
  new_response = [{k: v for k, v in d.items() if k != 'Unit'} for d in new_response]
  d = {'node_type' : node_type}
  upd_response = [ i | d for i in new_response]
  return upd_response

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Extract
# MAGIC Extract data from Redshift systems tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Information

# COMMAND ----------

# MAGIC %md
# MAGIC Generic query to retrieve system table

# COMMAND ----------

# MAGIC %md
# MAGIC stl_query
# MAGIC
# MAGIC Tracks metadata for all submitted queries, including duration, user, and status.

# COMMAND ----------

# DBTITLE 1,1. Load STL_Query Table

"""
Modified to remove the firehose user
"""
try:
    get_table_query("""
                SELECT A.*
                FROM STL_QUERY AS A
                INNER JOIN PG_USER AS B
                ON A.USERID = B.USESYSID and A.USERID  > 1
                WHERE UPPER(B.USENAME) != 'FIREHOSE'
                """,
                'stl_query')
except:
    print("Firehose user not found")

# COMMAND ----------

# MAGIC %md
# MAGIC stl_query_metrics
# MAGIC
# MAGIC Provides query-level performance metrics like CPU time, memory, and block I/O.

# COMMAND ----------

# DBTITLE 1,2. Load stl_query_metrics
#Note: Queries that run < 1 sec might not be recorded
get_table("stl_query_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC stl_wlm_query 
# MAGIC
# MAGIC Shows how queries are managed by the Workload Management (WLM) queues, including queue time and execution time.
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,3. Load stl_wlm_query
get_table("stl_wlm_query")

# COMMAND ----------

# MAGIC %md
# MAGIC stv_node_storage_capacity 
# MAGIC
# MAGIC Displays current disk storage usage and capacity for each Redshift node.

# COMMAND ----------

# DBTITLE 1,Load stv_node_storage_capacity
# Table: stv_node_storage_capacity
# is visible only to superusers as AWS docs
# https://docs.aws.amazon.com/redshift/latest/dg/r_STV_NODE_STORAGE_CAPACITY.html
get_table("stv_node_storage_capacity");

# COMMAND ----------

# MAGIC %md
# MAGIC stv_partitions
# MAGIC
# MAGIC Shows how table data is partitioned across slices and nodes.

# COMMAND ----------

# DBTITLE 1,Load stv_partitions
get_table("stv_partitions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Warehouse Objects - System Views

# COMMAND ----------

# MAGIC %md
# MAGIC svv_table_info
# MAGIC
# MAGIC Summarizes storage and distribution details for user tables, including size, skew, and compression.

# COMMAND ----------

# DBTITLE 1,query tables 
get_table("svv_table_info")

# COMMAND ----------

# MAGIC %md
# MAGIC Generic function to query external tables
# MAGIC
# MAGIC svv_external_tables
# MAGIC
# MAGIC Lists all external tables available via Redshift Spectrum or Amazon Glue Data Catalog.

# COMMAND ----------

# DBTITLE 1,query external tables
get_table_query("""
select distinct location, input_format from SVV_EXTERNAL_TABLES
""", "rs_external_tables")


# COMMAND ----------

# MAGIC %md
# MAGIC pg_catalog.pg_views
# MAGIC
# MAGIC Lists all views in the database, including definitions and owners.

# COMMAND ----------

# DBTITLE 1,query views
get_table_query("""
select
  schemaname as schema_name
 , viewname as view_name
from pg_catalog.pg_views  
where schemaname not in ('information_schema', 'pg_catalog')
""", "rs_views")

# COMMAND ----------

# MAGIC %md
# MAGIC stv_mv_info
# MAGIC
# MAGIC Contains metadata and status information about materialized views, such as refresh state.

# COMMAND ----------

# DBTITLE 1,query materialized views
get_table ("stv_mv_info")

# COMMAND ----------

# MAGIC %md
# MAGIC Generic function querying pg_catalog
# MAGIC
# MAGIC pg_catalog.pg_namespace
# MAGIC
# MAGIC Represents all schemas in the current database with metadata like names and IDs.
# MAGIC
# MAGIC pg_catalog.pg_proc
# MAGIC
# MAGIC Contains definitions for stored procedures and functions, including argument types and return types.

# COMMAND ----------

# DBTITLE 1,query procs
get_table_query("""
SELECT
    n.nspname,
    b.usename,
    p.proname,
    p.prolang,
    p.prosrc
FROM
    pg_catalog.pg_namespace n
JOIN pg_catalog.pg_proc p ON
    pronamespace = n.oid
join pg_catalog.pg_user b on
    b.usesysid = p.proowner and  b.usesysid > 1
where
    nspname not in ('information_schema', 'pg_catalog')
  
""",
  "rs_procs"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## AWS System Information

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Use Boto3 (python sdk) to gather systems metrics from AWS service API's
# MAGIC
# MAGIC Create client connection to AWS cloudwatch service

# COMMAND ----------

# DBTITLE 1,AWS API Client
import boto3
from operator import itemgetter
from datetime import datetime
client = boto3.client('cloudwatch', region)

# COMMAND ----------

# MAGIC %md
# MAGIC Set metric windows for current date - 5 days

# COMMAND ----------

# DBTITLE 1,CPU Utilization Data Extract
from datetime import datetime, timedelta
# get last 5 days for metrics
cpu_eddate = datetime.today().strftime("%Y%m%d")
cpu_stdate = (datetime.today() - timedelta(days=5)).strftime("%Y%m%d")
print("cpu_stdate: {}".format(cpu_stdate))
print("cpu_eddate: {}".format(cpu_eddate))

# COMMAND ----------

# DBTITLE 1,Leader node  (CPU Utilization)
#leadernode_cpu = get_cpu_util(clusterid, 'Leader', cpu_stdate, cpu_eddate)

# COMMAND ----------

#node_count_df = spark.sql("select count(*) node_count from stv_node_storage_capacity")
#print(node_count_df.head())

# COMMAND ----------

# MAGIC %md
# MAGIC Retrieve CPU Utilization for the metric window for Redshift Compute Nodes

# COMMAND ----------

# DBTITLE 1,Compute nodes - CPU Utilization
from pyspark.sql.functions import col
# get number of nodes
node_count_df = spark.sql("select count(*) node_count from stv_node_storage_capacity")
node_count = node_count_df.head()[0]

# send api query one per each node
all_compute = []
computenodes_cpu = {}
for i in range(node_count):
  node = 'Compute-'+str(i)
  ##node = 'shared'
  #print(node)
  computenodes_cpu[i] = get_cpu_util(clusterid, node, cpu_stdate, cpu_eddate)
  #print( "computenodes_cpu[i]", computenodes_cpu[i])
  all_compute = all_compute + computenodes_cpu[i] 

# COMMAND ----------

# DBTITLE 1,Cluster CPU Utilization
#all_cpu_df = spark.createDataFrame(all_compute + leadernode_cpu)
all_cpu_df = spark.createDataFrame(all_compute)
all_cpu_df.write.mode("overwrite").saveAsTable("cluster_cpu_utlization")   

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spectrum Utilization
# MAGIC
# MAGIC Redshift Spectrum is a feature of Amazon Redshift, a cloud data warehouse service, that allows you to query data stored in Amazon S3 (simple storage service) using SQL. It enables you to analyze large datasets in S3 without the need to load the data into the Redshift cluster. 

# COMMAND ----------

# DBTITLE 1,s3_scanned_tbs
get_table_query("""
SELECT
   trunc(starttime) as date,
   round(1.0*sum(s3_scanned_bytes/1024/1024/1024/1024),4) s3_scanned_tb
FROM SVL_S3QUERY_SUMMARY
where starttime >= current_date-10
group by 1
""", "s3_scanned_tbs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concurrency Scaling Utilization
# MAGIC
# MAGIC In Amazon Redshift, Concurrency Scaling is a feature that automatically and elastically scales query processing power to handle increases in concurrent queries

# COMMAND ----------

# DBTITLE 1,concurrency scaling usage table
get_table("svcs_concurrency_scaling_usage")

# COMMAND ----------

# MAGIC %md
# MAGIC stv_mv_info
# MAGIC
# MAGIC Contains metadata and status information about materialized views, such as refresh state.
# MAGIC
# MAGIC

# COMMAND ----------

get_table("stv_mv_info")

# COMMAND ----------

# MAGIC %md
# MAGIC stl_query
# MAGIC
# MAGIC Tracks metadata for all submitted queries, including duration, user, and status.

# COMMAND ----------

get_table("stl_query")

# COMMAND ----------

# MAGIC %md
# MAGIC # Warehouse Information

# COMMAND ----------

# DBTITLE 1,1. Storage Info  - Node level
# MAGIC %sql
# MAGIC
# MAGIC --select * from stv_node_storage_capacity
# MAGIC select
# MAGIC    node,
# MAGIC    round((capacity / 1024), 2) as capacity_gb,
# MAGIC    round((used / 1024), 2) as  used_gb
# MAGIC  from
# MAGIC    stv_node_storage_capacity

# COMMAND ----------

# DBTITLE 1,2. Storage Info  - Cluster level
# MAGIC %sql
# MAGIC select
# MAGIC   round((sum(capacity) / 1024), 2) as capacity_gb,
# MAGIC   round((sum(used) / 1024), 2) as  used_gb
# MAGIC from
# MAGIC   stv_node_storage_capacity

# COMMAND ----------

# DBTITLE 1,3. Cluster Instance Types
# MAGIC %sql
# MAGIC -- Modify the capacity for RA3 node type
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN capacity = 190633  AND NOT is_nvme    THEN 'dc1.large'
# MAGIC         WHEN capacity = 380319                     THEN 'dc1.8xlarge'
# MAGIC         WHEN capacity = 190633  AND is_nvme        THEN 'dc2.large'
# MAGIC         WHEN capacity = 760956                     THEN 'dc2.8xlarge'
# MAGIC         WHEN capacity = 726296                     THEN 'dc2.8xlarge'
# MAGIC         WHEN capacity = 952455                     THEN 'ds2.xlarge' 
# MAGIC         WHEN capacity = 945026                     THEN 'ds2.8xlarge' 
# MAGIC         WHEN capacity = 2002943  AND part_count = 1 THEN 'ra3.xlplus'
# MAGIC         WHEN capacity = 6772561 AND part_count = 1 THEN 'ra3.4xlarge' 
# MAGIC         WHEN capacity = 6772561 AND part_count = 4 THEN 'ra3.16xlarge' 
# MAGIC         ELSE 'unknown' 
# MAGIC     END AS InstanceType,
# MAGIC     (select count(distinct host) from stv_partitions) as `Number Of Nodes`
# MAGIC     
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         capacity,
# MAGIC         mount LIKE '/dev/nvme%' AS is_nvme,
# MAGIC         count(1) AS part_count
# MAGIC     FROM stv_partitions
# MAGIC     WHERE host = 0 AND owner = 0 
# MAGIC     GROUP BY 1, 2
# MAGIC     ORDER BY 1 DESC
# MAGIC     LIMIT 1
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC Read more about instance type comparisons here -  Not something you would consider for migration from RedShift
# MAGIC
# MAGIC https://aws.amazon.com/blogs/apn/amazon-redshift-benchmarking-comparison-of-ra3-vs-ds2-instance-types/

# COMMAND ----------

# DBTITLE 1,4. Cluster Instance Type DS2/DC2 to RA3 Conversion
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     concat(instancetype, ' ', cast(numbernodes as string)) AS `DC2/DS2NodeTypeNumber`,
# MAGIC     CASE
# MAGIC         WHEN instancetype = 'dc2.large' and numbernodes <= 4 THEN concat('ra3.xlplus ', cast(numbernodes as STRING) )
# MAGIC         WHEN instancetype = 'dc2.large' and numbernodes between 5 and 15 THEN concat('ra3.xlplus ', cast(ceiling(numbernodes*3/8) as STRING))
# MAGIC         WHEN instancetype = 'dc2.large' and numbernodes between 16 and 32 THEN 'ra3.4xlarge 4'
# MAGIC         WHEN instancetype = 'dc2.8xlarge' and numbernodes between 2 and 15 THEN concat('ra3.4xlarge ', cast((numbernodes*2) as STRING))
# MAGIC         WHEN instancetype = 'dc2.8xlarge' and numbernodes between 16 and 128 THEN concat('ra3.16xlarge ', cast(ceiling(numbernodes*1/2) as STRING))
# MAGIC         WHEN instancetype = 'ds2.xlarge' and numbernodes = 1 THEN 'ra3.xlplus 1'
# MAGIC         WHEN instancetype = 'ds2.xlarge' and numbernodes between 2 and 3 THEN 'ra3.xlplus 2'
# MAGIC         WHEN instancetype = 'ds2.xlarge' and numbernodes = 4 THEN 'ra3.xlplus 3'
# MAGIC         WHEN instancetype = 'ds2.xlarge' and numbernodes between 5 and 6 THEN  'ra3.xlplus 4'
# MAGIC         WHEN instancetype = 'ds2.xlarge' and numbernodes = 7 THEN  'ra3.xlplus 5'
# MAGIC         WHEN instancetype = 'ds2.xlarge' and numbernodes = 8 THEN  'ra3.4xlarge 2'
# MAGIC         WHEN instancetype = 'ds2.xlarge' and numbernodes between 9 and 10 THEN  'ra3.4xlarge 3'
# MAGIC         WHEN instancetype = 'ds2.xlarge' and numbernodes between 11 and 128 THEN concat('ra3.4xlarge ', cast(ceiling(numbernodes*1/4) as STRING))
# MAGIC         WHEN instancetype = 'ds2.8xlarge' and numbernodes between 2 and 15 THEN concat('ra3.4xlarge ', cast((numbernodes*2) as STRING)) 
# MAGIC         WHEN instancetype = 'ds2.8xlarge' and numbernodes between 16 and 128 THEN concat('ra3.16xlarge ', cast(ceiling(numbernodes*1/2) as STRING))   
# MAGIC         ELSE 'No Match'
# MAGIC     END AS `RA3NodeTypeNumber`
# MAGIC
# MAGIC FROM (
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN capacity = 190633  AND NOT is_nvme    THEN 'dc1.large'
# MAGIC         WHEN capacity = 380319                     THEN 'dc1.8xlarge'
# MAGIC         WHEN capacity = 190633  AND is_nvme        THEN 'dc2.large'
# MAGIC         WHEN capacity = 760956                     THEN 'dc2.8xlarge'
# MAGIC         WHEN capacity = 726296                     THEN 'dc2.8xlarge'
# MAGIC         WHEN capacity = 952455                     THEN 'ds2.xlarge' 
# MAGIC         WHEN capacity = 945026                     THEN 'ds2.8xlarge' 
# MAGIC         WHEN part_count = 2                        THEN 'ra3.xlplus'
# MAGIC         WHEN part_count = 4                        THEN 'ra3.4xlarge' 
# MAGIC         WHEN part_count = 16                       THEN 'ra3.16xlarge' 
# MAGIC         ELSE 'unknown' 
# MAGIC     END AS instancetype,
# MAGIC     (select count(distinct host) from stv_partitions) as numbernodes
# MAGIC     
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         capacity,
# MAGIC         mount LIKE '/dev/nvme%' AS is_nvme,
# MAGIC         count(1) AS part_count
# MAGIC     FROM stv_partitions
# MAGIC     WHERE owner = 0 
# MAGIC     GROUP BY 1, 2
# MAGIC     ORDER BY 1 DESC
# MAGIC     LIMIT 1
# MAGIC )) 
# MAGIC WHERE instancetype like 'dc2%' or instancetype like 'ds2%'

# COMMAND ----------

# MAGIC %md
# MAGIC Gather information from the tables created above.

# COMMAND ----------

# DBTITLE 1,Database Objects
# MAGIC %sql
# MAGIC select "Schemas" as otype, count(distinct schema) as count from svv_table_info
# MAGIC union all
# MAGIC select "Tables" as otype, count(distinct table_id) as count from svv_table_info
# MAGIC union all
# MAGIC select "Views" as otype,  count(distinct *) as count from rs_views 
# MAGIC union all
# MAGIC select "Materialized Views" as otype,  count(distinct (schema ||'.' || name)) as count from stv_mv_info
# MAGIC union all
# MAGIC select "Procs" as otype,  count(distinct (nspname ||'.' || proname)) as count from rs_procs 
# MAGIC union all
# MAGIC select "External Tables" as otype, count(distinct location) as count from rs_external_tables
# MAGIC union all
# MAGIC select "S3 Buckets" as otype, count(distinct parse_url(location,"HOST")) from rs_external_tables where location like 's3%';

# COMMAND ----------

# MAGIC %md
# MAGIC # Workload Insights

# COMMAND ----------

# MAGIC %md
# MAGIC Analyze the different workload types during the metric window

# COMMAND ----------

# DBTITLE 1,Create query_view
# MAGIC %sql
# MAGIC -- Added space after create/load/unload
# MAGIC -- Added endtime
# MAGIC -- Included fetch in BI query
# MAGIC
# MAGIC create or replace temporary view query_view as
# MAGIC (select userid, query, querytxt, cast(starttime as timestamp), cast(endtime as timestamp), case
# MAGIC   when label like 'stmt%' or label like 'statement%' then 'other'
# MAGIC   else label
# MAGIC   end as query_group,
# MAGIC  case
# MAGIC    when lower(querytxt) like '%create % table % as %' then 'Transform'
# MAGIC    when lower(querytxt) like '%create % temp % table %' then 'Transform'
# MAGIC    when lower(querytxt) like '%load %' then 'Extract and Load'
# MAGIC    when lower(querytxt) like '%unload %' then 'Extract and Load'
# MAGIC    when lower(querytxt) like 'copy % to %' then 'Extract and Load'
# MAGIC    when lower(querytxt) like 'copy % from %' then 'Ingestion'
# MAGIC    when lower(querytxt) like '%insert %' OR lower(querytxt) like '%update %' OR lower(querytxt) like '%delete %' then 'Transform'
# MAGIC    when lower(querytxt) like '%stv_recents%' or lower(querytxt) like '%padb_fetch_sample%'  or lower(querytxt) like '%copy%analyze%' or lower(querytxt) like '%analyze%compression%' then 'System'
# MAGIC    when lower(querytxt) like '%select %' or lower(querytxt) like '%with %' or lower(querytxt) like 'fetch %' then 'BI Query'
# MAGIC   else 'Unknown'
# MAGIC  end as query_type
# MAGIC from stl_query 
# MAGIC where label not in('metrics', 'other', 'health', 'cmstats') and (label not like 'statement%' and label not like 'stmt%')
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Create query_metrics_view
# MAGIC %sql
# MAGIC drop view if exists query_metrics_view;
# MAGIC create temporary view query_metrics_view as(
# MAGIC   select userid, query, 
# MAGIC   case
# MAGIC     when cpu_time = -1 then 0
# MAGIC     else cpu_time/1000000
# MAGIC     end 
# MAGIC   as cpu_time, 
# MAGIC    case
# MAGIC     when run_time = -1 then 0
# MAGIC     else run_time/1000000
# MAGIC     end 
# MAGIC   as run_time
# MAGIC   from stl_query_metrics
# MAGIC   where segment = -1 and step_type = -1
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Create query_overview
# MAGIC %sql
# MAGIC drop view if exists query_overview;
# MAGIC create temporary view query_overview as(
# MAGIC   select * from query_view
# MAGIC   natural join query_metrics_view
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Query type metrics by Hour
# MAGIC %sql
# MAGIC select query_type, hour(starttime) as hour, count(*) as count
# MAGIC from query_view
# MAGIC group by 1, 2
# MAGIC order by hour;

# COMMAND ----------

# DBTITLE 1,Query type metrics by Day
# MAGIC %sql
# MAGIC select query_type, day(starttime) as day, count(*) as count
# MAGIC from query_view
# MAGIC group by 1, 2
# MAGIC order by day;

# COMMAND ----------

# DBTITLE 1,Concurrent users by Hour
# MAGIC %sql
# MAGIC select count(distinct userid) as distinct_users, hour(starttime) as hour
# MAGIC from query_view 
# MAGIC group by 2
# MAGIC order by 2;

# COMMAND ----------

# DBTITLE 1,Average query wait time (sec) by hour
# MAGIC %sql
# MAGIC -- total_queue_time: the wait time
# MAGIC select 
# MAGIC   avg(total_queue_time)/1000000 as avg_query_wait_time, 
# MAGIC   max(total_queue_time)/1000000 as max_query_wait_time,
# MAGIC   hour(cast(queue_start_time as timestamp)) as hour
# MAGIC from stl_wlm_query
# MAGIC where service_class > 4
# MAGIC group by 3
# MAGIC order by 3;
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,BI Queries per Minute (QPM) - Distribution
# MAGIC %sql
# MAGIC -- BI queries per minute(QPM) min/max/avg 
# MAGIC select  
# MAGIC make_timestamp(
# MAGIC   date_part('YEAR', starttime),
# MAGIC   date_part('MONTH', starttime),
# MAGIC   date_part('DAY', starttime),
# MAGIC   date_part('HOUR', starttime),
# MAGIC   date_part('MINUTE', starttime), 0) start_time_minute,
# MAGIC count(query) as query_cnt
# MAGIC from query_view
# MAGIC where 
# MAGIC query_type='BI Query'
# MAGIC group by start_time_minute;

# COMMAND ----------

# DBTITLE 1,BI Queries per Minute (QPM) - Min/Max/Avg
# MAGIC %sql
# MAGIC select min(query_cnt) qpm_min, avg(query_cnt) qpm_avg, max(query_cnt) qpm_max
# MAGIC from (
# MAGIC select  make_timestamp(
# MAGIC   date_part('YEAR', starttime),
# MAGIC   date_part('MONTH', starttime),
# MAGIC   date_part('DAY', starttime),
# MAGIC   date_part('HOUR', starttime),
# MAGIC   date_part('MINUTE', starttime), 0) start_time_minute,
# MAGIC count(query) as query_cnt
# MAGIC from query_view
# MAGIC where 
# MAGIC query_type='BI Query'
# MAGIC group by start_time_minute  
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Database Idle Time Percentage
# MAGIC %sql
# MAGIC -- Query Idle Time Pct
# MAGIC select round(idle_time_seconds_total/time_seconds_total, 2)*100 idle_time_pct,
# MAGIC   idle_time_seconds_total,
# MAGIC   time_seconds_total
# MAGIC from 
# MAGIC (select NVL(sum(DATEDIFF(second, endtime, NVL(next_starttime, last_endtime))), 0) as idle_time_seconds_total
# MAGIC from (
# MAGIC SELECT starttime, endtime, last_endtime,
# MAGIC LEAD(starttime) OVER (ORDER BY starttime) AS next_starttime
# MAGIC FROM query_view, (select max(endtime) as last_endtime from query_view)
# MAGIC )
# MAGIC where DATEDIFF(second, endtime, NVL(next_starttime, last_endtime)) > 0),
# MAGIC (select DATEDIFF(second, min(starttime), max(endtime)) time_seconds_total from query_view);
# MAGIC

# COMMAND ----------

# DBTITLE 1,Database Idle Time Percentage using BI queries only
# MAGIC %sql
# MAGIC -- Query Idle Time Pct for BI queries
# MAGIC with bi_query_view as (
# MAGIC   select * from query_view where query_type='BI Query'
# MAGIC )
# MAGIC select round(idle_time_seconds_total/time_seconds_total, 2)*100 idle_time_pct,
# MAGIC   idle_time_seconds_total,
# MAGIC   time_seconds_total
# MAGIC from 
# MAGIC (select NVL(sum(DATEDIFF(second, endtime, NVL(next_starttime, last_endtime))), 0) as idle_time_seconds_total
# MAGIC from (
# MAGIC SELECT starttime, endtime, last_endtime,
# MAGIC LEAD(starttime) OVER (ORDER BY starttime) AS next_starttime
# MAGIC FROM bi_query_view, (select max(endtime) as last_endtime from bi_query_view)
# MAGIC )
# MAGIC where DATEDIFF(second, endtime, NVL(next_starttime, last_endtime)) > 0),
# MAGIC (select DATEDIFF(second, min(starttime), max(endtime)) time_seconds_total from bi_query_view);
# MAGIC

# COMMAND ----------

# DBTITLE 1,Database Idle Time Percentage using non-BI queries only
# MAGIC %sql
# MAGIC -- Query Idle Time Pct for NON BI queries
# MAGIC with non_bi_query_view as (
# MAGIC   select * from query_view where query_type!='BI Query'
# MAGIC )
# MAGIC select round(idle_time_seconds_total/time_seconds_total, 2)*100 idle_time_pct,
# MAGIC   idle_time_seconds_total,
# MAGIC   time_seconds_total
# MAGIC from 
# MAGIC (select NVL(sum(DATEDIFF(second, endtime, NVL(next_starttime, last_endtime))), 0) as idle_time_seconds_total
# MAGIC from (
# MAGIC SELECT starttime, endtime, last_endtime,
# MAGIC LEAD(starttime) OVER (ORDER BY starttime) AS next_starttime
# MAGIC FROM non_bi_query_view, (select max(endtime) as last_endtime from non_bi_query_view)
# MAGIC )
# MAGIC where DATEDIFF(second, endtime, NVL(next_starttime, last_endtime)) > 0),
# MAGIC (select DATEDIFF(second, min(starttime), max(endtime)) time_seconds_total from non_bi_query_view);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Workload Attribution
# MAGIC
# MAGIC The following two queries break down total Redshift usage by workload type.

# COMMAND ----------

# DBTITLE 1,CPU consumption % by query type
# MAGIC %sql
# MAGIC select query_type, sum(cpu_time) as sum_cpu_time
# MAGIC from query_overview
# MAGIC where cpu_time > 0
# MAGIC group by query_type

# COMMAND ----------

# DBTITLE 1,CPU consumption by hour and query type
# MAGIC %sql
# MAGIC select q.query_type, sum(q.cpu_time) as sum_cpu_time, hour(q.starttime) as hour
# MAGIC from query_overview q
# MAGIC where q.cpu_time > 0
# MAGIC group by 1, 3
# MAGIC order by 3 asc, sum_cpu_time asc

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # System Utilization
# MAGIC
# MAGIC The following two queries break down total Redshift CPU usage 

# COMMAND ----------

# DBTITLE 1,Daily Average Utilization By Node
# MAGIC %sql
# MAGIC select node_type, date_format(Timestamp, "HH:mm:ss") as time, round(avg(average), 1) as `cpu_utilization_%` 
# MAGIC from cluster_cpu_utlization
# MAGIC group by 1, 2
# MAGIC order by 1, 2

# COMMAND ----------

# DBTITLE 1,Cluster CPU Utilization 
# MAGIC %sql
# MAGIC select date_format(Timestamp, "HH:mm:ss") as time, round(avg(average), 1) as `cpu_utilization_%` 
# MAGIC from cluster_cpu_utlization
# MAGIC where lower(node_type) <> "leader" 
# MAGIC group by 1
# MAGIC order by 1

# COMMAND ----------

# DBTITLE 1,Cluster CPU Utilization Distribution
# MAGIC %sql
# MAGIC SELECT 'Compute Nodes' as node_type, 
# MAGIC percentile_cont(array(0.25, 0.5, 0.75, 0.9)) WITHIN GROUP (ORDER BY cpu_utilization_pct) as 25_50_75_90_percentile,
# MAGIC min(cpu_utilization_pct) min_cpu_utilization,
# MAGIC max(cpu_utilization_pct) max_cpu_utilization,
# MAGIC avg(cpu_utilization_pct) avg_cpu_utilization
# MAGIC FROM (
# MAGIC select date_format(Timestamp, "HH:mm:ss") as time, round(avg(average), 1) as cpu_utilization_pct
# MAGIC from cluster_cpu_utlization
# MAGIC where lower(node_type) <> "leader" 
# MAGIC group by 1
# MAGIC )
# MAGIC UNION
# MAGIC SELECT 'Leader Node' as node_type, 
# MAGIC percentile_cont(array(0.25, 0.5, 0.75, 0.9)) WITHIN GROUP (ORDER BY cpu_utilization_pct) as 25_50_75_90_percentile,
# MAGIC min(cpu_utilization_pct) min_cpu_utilization,
# MAGIC max(cpu_utilization_pct) max_cpu_utilization,
# MAGIC avg(cpu_utilization_pct) avg_cpu_utilization
# MAGIC FROM (
# MAGIC select date_format(Timestamp, "HH:mm:ss") as time, round(avg(average), 1) as cpu_utilization_pct
# MAGIC from cluster_cpu_utlization
# MAGIC where lower(node_type) = "leader" 
# MAGIC group by 1
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC # Spectrum Utilization

# COMMAND ----------

# DBTITLE 1,Daily S3 Scanned Volume (TB)
# MAGIC %sql
# MAGIC select *, round(avg(s3_scanned_tb) over (), 4 ) avg_daily_scanned_tb from s3_scanned_tbs
# MAGIC order by 1

# COMMAND ----------

# MAGIC %md
# MAGIC # Concurrency Scaling Utilization

# COMMAND ----------

# DBTITLE 1,Daily Concurrecny Scaling Usage (seconds)
# MAGIC %sql
# MAGIC select *, round(avg(daily_usage_in_seconds) over (), 0) as avg_daily_usage_in_seconds
# MAGIC from (
# MAGIC select distinct to_date(cast(start_time as timestamp)) as day, 
# MAGIC sum(usage_in_seconds) OVER (PARTITION BY to_date(cast(start_time as timestamp))) daily_usage_in_seconds,
# MAGIC sum(usage_in_seconds) OVER () AS total_usage_in_seconds
# MAGIC from svcs_concurrency_scaling_usage);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Miscellaneous Query Metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top Transform queries by cpu_time

# COMMAND ----------

# DBTITLE 1,Profile of top 20 Transform queries by cpu_time
# MAGIC %sql
# MAGIC select query_type, max(cpu_time) as max_cpu_time, avg(cpu_time) as avg_cpu_time, querytxt, count(*)
# MAGIC from query_overview
# MAGIC where cpu_time > 0 and query_type = 'Transform'
# MAGIC group by querytxt, query_type
# MAGIC order by max_cpu_time desc, avg_cpu_time limit 20;

# COMMAND ----------

# DBTITLE 1,Profile of top 20 Transform queries by run_time
# MAGIC %sql
# MAGIC -- run time: diff of endtime and begintime
# MAGIC select query_type, max(run_time) as max_run_time, avg(run_time) as avg_run_time, 
# MAGIC querytxt, count(*) as count
# MAGIC from
# MAGIC (select query_type, querytxt, datediff(SECOND, starttime, endtime) as run_time
# MAGIC from query_view
# MAGIC where 
# MAGIC datediff(SECOND, starttime, endtime) >0 and query_type = 'Transform')
# MAGIC group by querytxt, query_type
# MAGIC order by max_run_time desc, avg_run_time limit 20; 
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top BI queries

# COMMAND ----------

# DBTITLE 1,Profile of top 20 BI queries by cpu_time
# MAGIC %sql
# MAGIC select query_type, max(cpu_time) as max_cpu_time, avg(cpu_time) as avg_cpu_time, querytxt, count(*) as count
# MAGIC from query_overview
# MAGIC where cpu_time > 0 and query_type = 'BI Query'
# MAGIC group by querytxt, query_type
# MAGIC order by max_cpu_time desc, avg_cpu_time limit 20;

# COMMAND ----------

# DBTITLE 1,Profile of1 top 20 BI queries by run_time
# MAGIC %sql
# MAGIC -- Run time: diff of endtime and begintime
# MAGIC
# MAGIC select query_type, max(run_time) as max_run_time, avg(run_time) as avg_run_time, 
# MAGIC querytxt, count(*) as count
# MAGIC from
# MAGIC (select query_type, querytxt, datediff(SECOND, starttime, endtime) as run_time
# MAGIC from query_view
# MAGIC where datediff(SECOND, starttime, endtime) >0 and query_type = 'BI Query')
# MAGIC group by querytxt, query_type
# MAGIC order by max_run_time desc, avg_run_time limit 20; 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top Unknown Type Queries

# COMMAND ----------

# DBTITLE 1,Profile of top 20 Unknown type queries by cpu_time
# MAGIC %sql
# MAGIC select query_type, max(cpu_time) as max_cpu_time, avg(cpu_time) as avg_cpu_time, querytxt, count(*) as count
# MAGIC from query_overview
# MAGIC where cpu_time > 0 and lower(query_type) = 'unknown'
# MAGIC group by querytxt, query_type
# MAGIC order by max_cpu_time desc, avg_cpu_time limit 20;

# COMMAND ----------

# DBTITLE 1,Profile of top 20 Unknown type queries by run_time
# MAGIC %sql
# MAGIC -- run time: diff of endtime and begintime
# MAGIC select query_type, max(run_time) as max_run_time, avg(run_time) as avg_run_time, 
# MAGIC querytxt, count(*) as count
# MAGIC from
# MAGIC (select query_type, querytxt, datediff(SECOND, starttime, endtime) as run_time
# MAGIC from query_view
# MAGIC where datediff(SECOND, starttime, endtime) >0 and query_type = 'unknown')
# MAGIC group by querytxt, query_type
# MAGIC order by max_run_time desc, avg_run_time limit 20; 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top System Type Queries

# COMMAND ----------

# DBTITLE 1,Profile of top 20 System type queries by cpu_time
# MAGIC %sql
# MAGIC select query_type, max(cpu_time) as max_cpu_time, avg(cpu_time) as avg_cpu_time, querytxt, count(*) as count
# MAGIC from query_overview
# MAGIC where cpu_time > 0 and lower(query_type) = 'system'
# MAGIC group by querytxt, query_type
# MAGIC order by max_cpu_time desc, avg_cpu_time limit 20;

# COMMAND ----------

# DBTITLE 1,Profile of top 20 System type queries by run_time
# MAGIC %sql
# MAGIC -- -- run time: diff of endtime and begintime
# MAGIC select query_type, max(run_time) as max_run_time, avg(run_time) as avg_run_time, 
# MAGIC querytxt, count(*) as count
# MAGIC from
# MAGIC (select query_type, querytxt, datediff(SECOND, starttime, endtime) as run_time
# MAGIC from query_view
# MAGIC where datediff(SECOND, starttime, endtime) >0 and query_type = 'system')
# MAGIC group by querytxt, query_type
# MAGIC order by max_run_time desc, avg_run_time limit 20; 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Function Stats

# COMMAND ----------

# DBTITLE 1,Load Spark Functions into a DataFrame

"""
Compare spark functions with postgres
"""

spark_functions = spark.sql("SHOW ALL FUNCTIONS")
spark_functions.createOrReplaceTempView("spark_functions")

# COMMAND ----------

# DBTITLE 1,Function Usage
# MAGIC %sql
# MAGIC
# MAGIC -- get the count of functions from run queries
# MAGIC
# MAGIC   WITH
# MAGIC     t1 as (
# MAGIC       select 
# MAGIC         explode(split(querytxt, " ")) as token
# MAGIC       from stl_query
# MAGIC     ),
# MAGIC     t1_1 as (
# MAGIC       select
# MAGIC         explode(split(token, ",")) as token
# MAGIC       from t1
# MAGIC     ),
# MAGIC     t1_2 as (
# MAGIC       select
# MAGIC         explode(split(token, "--")) as token
# MAGIC       from t1_1
# MAGIC     ),
# MAGIC     t1_3 as (
# MAGIC       select
# MAGIC         explode(split(token, "=")) as token
# MAGIC       from t1_2
# MAGIC     ),
# MAGIC     t1_4 as (
# MAGIC       select
# MAGIC         explode(split(token, '::')) as token
# MAGIC       from t1_3
# MAGIC     ),
# MAGIC     t1_5 as (
# MAGIC       select
# MAGIC         explode(split(token, '\\)')) as token
# MAGIC       from t1_4
# MAGIC     ),
# MAGIC     t2 as (
# MAGIC       select 
# MAGIC         lower(split(regexp_extract(token, '^.*[\\(].*$', 0), '\\(')[0]) as func
# MAGIC       from t1_5
# MAGIC       where regexp_extract(token, '^.*[\\(].*$', 0) is not null
# MAGIC     ),
# MAGIC     t2_1 as (
# MAGIC       select lower(regexp_replace(func, '[-+*\/%\'\\\\\>\<]', "")) as func
# MAGIC       from t2
# MAGIC     ),
# MAGIC     t3 as (
# MAGIC       select
# MAGIC         func,
# MAGIC         count(*) AS COUNTS
# MAGIC       from t2_1
# MAGIC       group by 1
# MAGIC       order by 2
# MAGIC     )
# MAGIC     select t3.*,
# MAGIC            case
# MAGIC              when B.function is null
# MAGIC                then false
# MAGIC              else true
# MAGIC            end AS AVAILABLE_IN_SPARK_SQL
# MAGIC     from t3
# MAGIC     left join spark_functions as B
# MAGIC       on lower(B.function) = lower(t3.func)
# MAGIC     where UPPER(t3.func) not like '%SYSTEM$%' and UPPER(t3.func) not like 'VOLT_TT%'
# MAGIC       and UPPER(t3.func) not in ('IN', 'AS')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up the tables created by the profiler
# MAGIC # 

# COMMAND ----------

from databricks.sdk import WorkspaceClient

# Initialize WorkspaceClient (assumes env vars: DATABRICKS_HOST, DATABRICKS_TOKEN)
w = WorkspaceClient()

# Set your target catalog and schema
catalog_name = "classroom"
schema_name = redshift_profiler_schema

# List and delete all tables
for table in w.tables.list(catalog_name=catalog_name, schema_name=schema_name):
    try:
        print(f"🗑️ Ceated table: {table.full_name}")
      
    except Exception as e:
        print(f"⚠️ Failed to delete {table.full_name}: {e}")

print("✅ Table list complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Purposely failing the notebook in case the students want to review the tables that were created by the profiler

# COMMAND ----------

Purposely stop notebook

# COMMAND ----------

# MAGIC %md
# MAGIC # After revieing the tables in the catalog, we can delete the tables

# COMMAND ----------

from databricks.sdk import WorkspaceClient

# Initialize WorkspaceClient (assumes env vars: DATABRICKS_HOST, DATABRICKS_TOKEN)
w = WorkspaceClient()

# Set your target catalog and schema
catalog_name = "classroom"
schema_name = redshift_profiler_schema

# List and delete all tables
for table in w.tables.list(catalog_name=catalog_name, schema_name=schema_name):
    try:
        full_table_name = f"{catalog_name}.{schema_name}.{table.name}"
        print(f"🗑️ Deleting table: {full_table_name}")
        w.tables.delete(full_table_name)
    except Exception as e:
        print(f"⚠️ Could not delete {full_table_name}: {e}")

print("✅ All tables deleted.")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

# Initialize WorkspaceClient (assumes env vars: DATABRICKS_HOST, DATABRICKS_TOKEN)
w = WorkspaceClient()

# Set your target catalog and schema
catalog_name = "classroom"
schema_name = redshift_profiler_schema
try:
    print(f"🗑️ Deleting schema: {catalog_name}.{schema_name}")
    w.schemas.delete(full_name=f"{catalog_name}.{schema_name}")
    print("✅ Schema deleted.")
except Exception as e:
    print(f"⚠️ Failed to delete schema: {e}")
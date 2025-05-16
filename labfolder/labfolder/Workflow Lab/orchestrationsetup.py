# Databricks notebook source
import re



# Get the user's email from the Databricks context
user_email = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

# Extract the portion before the @
safe_user = user_email.split('@')[0]

# Optionally sanitize the username (e.g., replace dots)
safe_user = safe_user.replace('.', '_')

print(f"Original user: {user_email}")
print(f"Safe user ID: {safe_user}")


uc_schema_name = f"{safe_user}"
# Optional: specify catalog if using Unity Catalog
catalog_name = "classroom"  # replace with your catalog
full_schema_name = f"{catalog_name}.{uc_schema_name}"

spark.sql("USE CATALOG classroom")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {uc_schema_name}")
spark.catalog.setCurrentDatabase(uc_schema_name)

# Create the schema
#spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema_name}")
#print(f"Schema created: {full_schema_name}")


#spark.sql(f"USE  {full_schema_name}")

# COMMAND ----------

catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
schema = spark.sql("SELECT current_schema()").collect()[0][0]

print(f"Current catalog: {catalog}")
print(f"Current schema: {schema}")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

cluster_name = safe_user

# List all clusters and find the one that matches the name
cluster_id = None
for cluster in w.clusters.list():
   cluster_id = cluster.cluster_id
   break

if cluster_id:
    print(f"Cluster ID for '{cluster_name}': {cluster_id}")
else:
    print(f"Cluster '{cluster_name}' not found.")

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from databricks.sdk.errors import NotFound
from datetime import datetime

class LessonWorkflow:
    def __init__(self, schema_name, jobcluster):
        """
        Initialize the LessonWorkflow with schema name and workspace client.
        """
        self.schema_name = schema_name
        self.workspace = WorkspaceClient()

        # Get current user email (for notebook paths)
        self.user_email = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        self.user_folder = self.user_email.replace('@', '_').replace('.', '_')


        user_email = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

        safe_user = user_email.replace('@', '_')
        safe_user = safe_user.replace('.', '_')

        print(f"Original user: {user_email}")
        print(f"Safe user ID: {safe_user}")

        
   
        notebook_base_path = f"/Workspace/Users/{user_email}/labfolder/Workflow Lab/UnloadWorkflowTasks"
       
                             

        job_1 = f"venue_{self.schema_name}_extract_load_venue"
        job_2 = f"users_{self.schema_name}_extract_load_users"
        job_3 = f"sales_{self.schema_name}_extract_load_sales"

        job_names = [job_1, job_2, job_3]
        existing_jobs = list(self.workspace.jobs.list())

        # Delete existing jobs with the same names
        for job in existing_jobs:
          if job.settings.name in job_names:
           print(f"🗑️ Deleting existing job: {job.settings.name} (ID: {job.job_id})")
           self.workspace.jobs.delete(job_id=job.job_id)


        # Create new jobs
        for job in self.workspace.jobs.list():
           if job.settings.name in [job_1,job_2,job_3]:
               assert_false = False
               assert assert_false, f'You already have job named created{job.setting.name}. Please go to the Jobs page and manually delete the job. Then rerun this program to recreate it from the start of this demonstration.'

        # Define tasks
        ingest_venue_task = jobs.Task(
            existing_cluster_id = cluster_id,
            task_key="Ingest_Venue_Data",
            notebook_task=jobs.NotebookTask(notebook_path=f"{notebook_base_path}/unload_venue_data")
        )

        ingest_users_task = jobs.Task(
            existing_cluster_id = cluster_id,
            task_key="Ingest_User_Data",
            notebook_task=jobs.NotebookTask(notebook_path=f"{notebook_base_path}/unload_users_data")
        )

        ingest_sales_task = jobs.Task(
            existing_cluster_id = cluster_id,
            task_key="Ingest_Sales_Data",
            notebook_task=jobs.NotebookTask(notebook_path=f"{notebook_base_path}/unload_sales_data")
        
        )

        # Create Jobs
        created_job_1 = self.workspace.jobs.create(name=job_1, tasks=[ingest_venue_task])
        print(f"✅ Created job: {job_1}, ID: {created_job_1.job_id}")

        created_job_2 = self.workspace.jobs.create(name=job_2, tasks=[ingest_users_task])
        print(f"✅ Created job: {job_2}, ID: {created_job_2.job_id}")

        created_job_3 = self.workspace.jobs.create(name=job_3, tasks=[ingest_sales_task])
        print(f"✅ Created job: {job_3}, ID: {created_job_3.job_id}")


# COMMAND ----------

print("before call")
workflow = LessonWorkflow(uc_schema_name, cluster_id)
##workflow.create_job_lesson05()
# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab: Modular Orchestration
# MAGIC
# MAGIC In this lab, you'll be configuring a multi-task job comprising of three notebooks.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you should be able to:
# MAGIC * Schedule a Master Job consists of SubJobs (RunJobs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
# MAGIC
# MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
# MAGIC
# MAGIC Follow these steps to select the classic compute cluster:
# MAGIC
# MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
# MAGIC
# MAGIC 1. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
# MAGIC
# MAGIC   - In the drop-down, select **More**.
# MAGIC
# MAGIC   - In the **Attach to an existing compute resource** pop-up, select the first drop-down. You will see a unique cluster name in that drop-down. Please select that cluster.
# MAGIC
# MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
# MAGIC
# MAGIC 1. Right-click **Compute** in the left navigation pane and select *Open in new tab*.
# MAGIC **Your cluster will match your user name with underscores for any special characters in your email address**
# MAGIC
# MAGIC 1. Find the triangle icon to the right of your compute cluster name and click it.
# MAGIC
# MAGIC 1. Wait a few minutes for the cluster to start.
# MAGIC
# MAGIC 1. Once the cluster is running, complete the steps above to select your cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workflow setup
# MAGIC Run the following cell to configure three worflow jobs. Each job will have one task to unload a Redshift table to Amazon S3 and then create and load a corresponding Delta table in Unity Catalog.
# MAGIC
# MAGIC In subsequent steps, you will create a job that combines each individual job into one master job.
# MAGIC

# COMMAND ----------

# MAGIC %run ./orchestrationsetup

# COMMAND ----------

# MAGIC %md
# MAGIC Review one of the individual jobs
# MAGIC 1. Select Worklows from the left navigation panel
# MAGIC 1. Select a job ending in extract_load_sales
# MAGIC 1. Select Tasks

# COMMAND ----------

# MAGIC %md
# MAGIC ![Distribution](./images/jobtask.png)

# COMMAND ----------

# MAGIC %md
# MAGIC This job has a single task that runs the notebook /Workspace/labfolder/Workflow Lab/UnloadWorkflowTasks/unload_sales_data. In workflows, you can create more tasks and link them together to create a workflow. We will keep things simple for this lab and each of our jobs will have a single task.

# COMMAND ----------

# MAGIC %md
# MAGIC To test this singular job, click the **Run Now** button.
# MAGIC
# MAGIC
# MAGIC
# MAGIC ![Distribution](./images/runnow.png)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC To review your running job, select **Runs** next to the Tasks link.
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC ![Distribution](./images/showruns.png)

# COMMAND ----------

# MAGIC %md
# MAGIC Select a value from the "Start time" column to review the running notebook.
# MAGIC
# MAGIC
# MAGIC ![Distribution](./images/joboutput.png)

# COMMAND ----------

# MAGIC %md
# MAGIC Three similar jobs were created. In the following section, you will combine them all into one master job.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating a Master Job and adding Run Job as a task
# MAGIC 1. Right-click **Workflows** in the left navigation bar, and open the link in a new tab.
# MAGIC 2. Click **Create job**, and give it the name of **your-schema - Master export load Orchestration Job**
# MAGIC 3. Change the name by changing the text **"New Job ...."**
# MAGIC 4. For the first task, complete the fields as follows:
# MAGIC
# MAGIC Configure the task:
# MAGIC
# MAGIC | Setting | Instructions |
# MAGIC |--|--|
# MAGIC | Task name | Enter **Ingest_From_Source_Venue** |
# MAGIC | Type | Choose **Run Job** |
# MAGIC | Job | Start typing "your shema name". You should see a job that is named -> **[your-schema]_extract_load_venue** Select this job.|
# MAGIC Advanced Settings| Maximum concurrent runs - 2
# MAGIC
# MAGIC 4. Click **Create task**
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add another Run Job as task
# MAGIC Now, configure the second task similar to first task. The second task is already a job being created as **[your_schema]__extract_load_users**
# MAGIC 1. Click **+Add Task**
# MAGIC 2. Complete the fields as follows:
# MAGIC Configure the task:
# MAGIC
# MAGIC | Setting | Instructions |
# MAGIC |--|--|
# MAGIC | Task name | Enter **Ingest_From_Source_Users** |
# MAGIC | Type | Choose **Run Job** |
# MAGIC | Job | Start typing "your schema name". You should see a job that is named -> **users_[your-schema]_extract_load_users** Select this job.|
# MAGIC Depends On | Remove  **X [your-schema]_extract_load_venue** Select this job.|
# MAGIC Advanced Settings| Maximum concurrent runs - 2
# MAGIC
# MAGIC 3. Click **Create task**
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 4. Click **Create task**
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC C2. Add another Run Job as task
# MAGIC Now, configure the second task similar to first task. The second task is already a job being created as [your_schema]__extract_load_users
# MAGIC
# MAGIC Complete the fields as follows: Configure the task:
# MAGIC Setting	Instructions
# MAGIC Task name	Enter Ingest_From_Source_Users
# MAGIC Type	Choose Run Job
# MAGIC Job	Start typing "your schema name". You should see a job that is named -> [your-schema]_extract_load_users Select this job.
# MAGIC Click **Create task**
# MAGIC
# MAGIC Click **Create task**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add another Run Job as task
# MAGIC Now, configure the third task similar to first task. The second task is already a job being created as **[your_schema]__extract_load_sales**
# MAGIC 1. Click **+Add Task**
# MAGIC 2. Complete the fields as follows:
# MAGIC Configure the task:
# MAGIC
# MAGIC | Setting | Instructions |
# MAGIC |--|--|
# MAGIC | Task name | Enter **Ingest_From_Source_Sales** |
# MAGIC | Type | Choose **Run Job** |
# MAGIC | Job | Start typing "your shema name". You should see a job that is named -> **sales_[your-schema]_extract_load_sales** Select this job.|
# MAGIC Depends On | Remove  **X [your-schema]_extract_load_Users** Select this job.|
# MAGIC Advanced Settings| Maximum concurrent runs - 2|
# MAGIC
# MAGIC
# MAGIC 3. Click **Create task**
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Notebook as task
# MAGIC Now, configure the another task similar to first task. The second task is already a job being created as **create_sales_view**
# MAGIC 1. Click **+Add Task**
# MAGIC 2. Complete the fields as follows:
# MAGIC Configure the task:
# MAGIC
# MAGIC | Setting | Instructions |
# MAGIC |--|--|
# MAGIC | Task name | Enter **create_sales_view** |
# MAGIC | Type | Choose **Notebook** |
# MAGIC | Notebook| Start typing "your shema name". You should see a job that is named -> **[/Workspace/users/YOURUSERID/labfolder/Worklfow Lab/UnloadWorkflowTasks/create_materialized_view** Select this notebook.|
# MAGIC | Cluster| Choose schema -> **[your_schema]** Select this cluster.|
# MAGIC Depends On |  **venue_[your-schema]_extract_load_venue and sales_[your-schema]_extract_load_sales**.|
# MAGIC
# MAGIC 3. Click **Create task**
# MAGIC
# MAGIC <br>

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC The job view should now look similar to this:
# MAGIC
# MAGIC
# MAGIC ![Distribution](./images/final_view.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Run the Job
# MAGIC Click **`Run now`** to run the job

# COMMAND ----------

# MAGIC %md
# MAGIC To review your running job, select **Runs** next to the Tasks link.
# MAGIC
# MAGIC Click the **Logs and Metrics** links.
# MAGIC
# MAGIC If your run fails, use the logs or output from each job to determine the problem and then use the Repair Run option to restart the job.
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC ![Distribution](./images/maxconcurrent.png)

# COMMAND ----------


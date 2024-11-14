# Databricks notebook source
# MAGIC %md
# MAGIC # Supply Chain Analysis for Order History in a Fashion Retail Store: Data Management with <img src="https://miro.medium.com/v2/resize:fit:1400/format:webp/1*bWrto2YAmeGjEW9sZIkdcg.png" width = 300> and <img src="https://docs.delta.io/latest/_static/delta-lake-logo.png" width=200/>

# COMMAND ----------

# MAGIC %md
# MAGIC # **Project Scenario**
# MAGIC
# MAGIC As a **_Data Engineer_** for an online clothing brand offering a variety of **_fashion brands_**, I am developing a **_Supply Chain Dashboard_** to analyze **_Order History_**. This dashboard will support **_Purchasing Decisions_** and help maintain **_Sufficient Inventory Levels_** for the upcoming **_Holiday Season_**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Data Ingestion Step**
# MAGIC
# MAGIC I am **creating and ingesting fictional JSON file data** into the **Databricks file system**. This dataset will be used to support analysis for the **Supply Chain Dashboard** project. 
# MAGIC
# MAGIC Using **Databricks**, I will load the data into a **Delta Table** to enable efficient processing and transformations, ultimately building a **scalable data pipeline** for actionable insights.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Project Steps**
# MAGIC
# MAGIC 1. **Create and Ingest Data** into a **Delta Table**.
# MAGIC 2. Use **Databricks Notebooks** in **Python** and **SQL** to **Process and Transform** the data.
# MAGIC 3. Develop a **Supply Chain Dashboard** for real-time insights.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Delta Lake Features**
# MAGIC
# MAGIC Leverage **Delta Lake** functionalities, including:
# MAGIC - **Merge Operations** for efficient data updates.
# MAGIC - **Time Travel** to manage historical data views.
# MAGIC
# MAGIC This approach will build a **Scalable Data Pipeline** that supports robust analytics and data integrity.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Uploaded fictional JSON file data to the Databricks file system and Git Repo

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. Upload ORDERS Json files in Databricks File System & Git Repo.

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. Check loaded files

# COMMAND ----------

# Use Databricks Utilities (dbutils). Documentation : https://docs.databricks.com/dev-tools/databricks-utils.html#ls-command-dbutilsfsls 

dbutils.fs.ls("/Volumes/foraproject/default/filestore/SupplyChain/ORDERS_RAW/")





# COMMAND ----------

# MAGIC %md
# MAGIC # Loading ORDERS_RAW Data Into Notepad and Creating Delta Table

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. Read multiline json files using spark dataframe:

# COMMAND ----------

# Read multiple line json files using spark dataframeAPI

orders_raw_df = spark.read.option("multiline", "true").json("/Volumes/foraproject/default/filestore/SupplyChain/ORDERS_RAW/ORDERS_RAW_PART_*.json")


## Show the datafarme
orders_raw_df.show(n=5, truncate=False) 


# COMMAND ----------

#Validate loaded files Count Number of Rows in the DataFrame

orders_raw_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![b.](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) b. Create Delta Table ORDERS_RAW

# COMMAND ----------

# MAGIC %md
# MAGIC Delta Lake is 100% compatible with Apache Spark&trade;, which makes it easy to get started with if you already use Spark for your big data workflows.
# MAGIC Delta Lake features APIs for **SQL**, **Python**, and **Scala**, so that you can use it in whatever language you feel most comfortable in.
# MAGIC
# MAGIC
# MAGIC    <img src="https://databricks.com/wp-content/uploads/2020/12/simplysaydelta.png" width=400/>

# COMMAND ----------

# First, Create Database SupplyChainDB if it doesn't exist
db = "SupplyChainDB"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
spark.sql(f"USE {db}")

# COMMAND ----------

## Create DelaTable ORDERS_RAW in the metastore using DataFrame's schema and write data to it
## Documentation : https://docs.delta.io/latest/quick-start.html#create-a-table

orders_raw_df.write.format("delta").mode("overwrite").saveAsTable("ORDERS_RAW")

# COMMAND ----------

# MAGIC %md
# MAGIC ### C. Show Created Delta Table:

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC **d. Validate data loaded successfully to Delta Table ORDERS_RAW**:

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) FROM ORDERS_RAW

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE TABLE ORDERS_RAW

# COMMAND ----------



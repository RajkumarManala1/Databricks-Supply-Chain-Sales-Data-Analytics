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
# MAGIC # Create Delta Table : ORDERS_RAW

# COMMAND ----------



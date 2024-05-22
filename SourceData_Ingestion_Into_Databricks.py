# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC This notebook shows you how to create and query a table or DataFrame loaded from data stored in Azure Blob storage.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 1: Set the data location and type
# MAGIC
# MAGIC There are two ways to access Azure Blob storage: account keys and shared access signatures (SAS).
# MAGIC
# MAGIC To get started, we need to set the location and type of the file.

# COMMAND ----------

storage_account_name = "haridatacontainer"
storage_account_access_key = "3***w=="

# COMMAND ----------

file_location = "wasbs://sourcedata@haridatacontainer.blob.core.windows.net/products.csv"
file_type = "csv"

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 2: Read the data
# MAGIC
# MAGIC Now that we have specified our file metadata, we can create a DataFrame. Notice that we use an *option* to specify that we want to infer the schema from the file. We can also explicitly set this to a particular schema if we have one already.
# MAGIC
# MAGIC First, let's create a DataFrame in Python.

# COMMAND ----------

df = spark.read.format(file_type).option("inferSchema", "true").option("header","true").load(file_location)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 3: Query the data
# MAGIC
# MAGIC Now that we have created our DataFrame, we can query it. For instance, you can identify particular columns to select and display.

# COMMAND ----------

display(df.head(3))

# COMMAND ----------

df = df.withColumnRenamed("Category","BikeCategory")

# COMMAND ----------

display(df.head(3))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 4: (Optional) Create a view or table
# MAGIC
# MAGIC If you want to query this data as a table, you can simply register it as a *view* or a table.

# COMMAND ----------

df.createOrReplaceTempView("src_products")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We can query this view using Spark SQL. For instance, we can perform a simple aggregation. Notice how we can use `%sql` to query the view from SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT ProductName, AVG(ListPrice) FROM src_products GROUP BY ProductName ORDER BY AVG(ListPrice) DESC LIMIT 3

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Since this table is registered as a temp view, it will be available only to this notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.

# COMMAND ----------

_sqldf.write.format("parquet").saveAsTable("products_agg")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC This table will persist across cluster restarts and allow various users across different notebooks to query this data.

# COMMAND ----------

# MAGIC %sql 
# MAGIC SHOW CREATE TABLE default.products_agg

# COMMAND ----------

# MAGIC %md
# MAGIC Write the transformed data back to Azure Data Lake Storage Gen2 for further processing(say via Azure Data Factory or Azure Synapse Analytics)

# COMMAND ----------

df.write.format("parquet").save("wasbs://sourcedata@haridatacontainer.blob.core.windows.net/transformed_products_delta")

# COMMAND ----------

df.write.format("delta").save("wasbs://sourcedata@haridatacontainer.blob.core.windows.net/transformed_products_delta")

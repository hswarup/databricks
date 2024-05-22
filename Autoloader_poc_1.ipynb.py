# Databricks notebook source
from pyspark.sql.functions import col,current_timestamp
display(dbutils.fs.ls("/databricks-datasets/structured-streaming"))
filepath="/databricks-datasets/structured-streaming/events/"
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
tablename=f"{username}_etl_quickstart"
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"


# COMMAND ----------

# Clear previous run data
spark.sql(f"DROP TABLE IF EXISTS {tablename}")
dbutils.fs.rm(checkpoint_path,True)

# COMMAND ----------

# Configure Auto Loader to ingest JSON data to a Delta table
(spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format","json") \
    .option("cloudFiles.schemaLocation",checkpoint_path) \
    .load(filepath) \
    .select("*",col("_metadata.file_path").alias("sourcefile"),current_timestamp().alias("processingtime"))
    .writeStream \
    .option("checkpointLocation",checkpoint_path) \
    .trigger(availableNow=True) \
    .toTable(tablename)
)

# COMMAND ----------

tablename

# COMMAND ----------

# Check the table contents
display(spark.read.table(tablename))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1),processingtime,MAX(sourcefile) FROM swaruphariprasad79_gmail_com_etl_quickstart GROUP BY processingtime ORDER BY processingtime LIMIT 3

# COMMAND ----------

hive_metastore.default.swaruphariprasad79_gmail_com_etl_quickstart

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `hive_metastore`; select * from `default`.`swaruphariprasad79_gmail_com_etl_quickstart` limit 100;

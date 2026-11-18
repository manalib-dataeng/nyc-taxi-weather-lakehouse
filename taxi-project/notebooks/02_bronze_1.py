# Databricks notebook source
from pyspark.sql import functions as F
base = "/Volumes/workspace/taxi_lakehouse/raw"

(spark.readStream.format("cloudFiles")
  .option("cloudFiles.format", "parquet")
  .option("cloudFiles.schemaLocation", f"{base}/_schemas/trips")
  .load(f"{base}/taxi")
  .withColumn("_ingested_at", F.current_timestamp())
  .withColumn("_source_file", F.col("_metadata.file_path"))
  .writeStream
  .option("checkpointLocation", f"{base}/_checkpoints/trips")
  .trigger(availableNow=True)
  .toTable("workspace.taxi_lakehouse.bronze_trips"))

# COMMAND ----------

spark.read.parquet(f"{base}/taxi").count()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) FROM taxi_lakehouse.bronze_trips;

# COMMAND ----------

(spark.read.option("multiline","true").json(f"{base}/weather")
  .withColumn("_ingested_at", F.current_timestamp())
  .write.mode("overwrite").saveAsTable("workspace.taxi_lakehouse.bronze_weather"))

(spark.read.option("header","true").csv(f"{base}/zones")
  .write.mode("overwrite").saveAsTable("workspace.taxi_lakehouse.bronze_zones"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM taxi_lakehouse.bronze_weather;
# MAGIC SELECT COUNT(*) FROM taxi_lakehouse.bronze_zones;
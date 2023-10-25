# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists jobsdemo;
# MAGIC use jobsdemo;

# COMMAND ----------

input_file_path="dbfs:/mnt/sanly/input/csv_files/"
output_file_path="dbfs:/mnt/sanly/input/jobs/"

# COMMAND ----------

(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.schemaLocation",f"{output_file_path}naval/logs/first/schema")
.option("cloudFiles.inferColumnTypes","True")
.load(f"{input_file_path}")     
.writeStream
.option("checkpointLocation", f"{output_file_path}naval/logs/first/checkpointlocation")
.option("path",f"{output_file_path}naval/first")
.option("mergeSchema",True)
.trigger(once=True)
.table("jobsdemo.bronze")
)

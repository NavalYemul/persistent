# Databricks notebook source
input_file_path_csv="dbfs:/mnt/sanly/input/task/csv"

input_file_path_json="dbfs:/mnt/sanly/input/task/json"

# COMMAND ----------

output_file_path="dbfs:/mnt/sanly/input/taskouput"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists task;
# MAGIC
# MAGIC use task;

# COMMAND ----------

(

spark.readStream

 .format("cloudFiles")

 .option("cloudFiles.format","csv")

 .option("cloudFiles.schemaLocation",f"{output_file_path}/Samir/logs/csv")

 .option("cloudFiles.inferColumnTypes","True")

 .load(f"{input_file_path_csv}")

 .createOrReplaceTempView("empdatatempview")

 )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Create or replace temp view bronze_temp as
# MAGIC
# MAGIC (select *, current_timestamp() as ingestiondate, input_file_name() as path from empdatatempview)

# COMMAND ----------

(spark.table("bronze_temp")

.writeStream

.option("checkpointLocation",f"{output_file_path}/Samir/logs/checkpoint/csv/")

.option("mergeSchema",True)

.trigger(processingTime="2 minutes")

.table("task.emp_bronze")

)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp

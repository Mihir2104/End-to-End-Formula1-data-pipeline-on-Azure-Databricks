# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times folder

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Reading the CSV file using the spark dataframe reader API

# COMMAND ----------

dbutils.widgets.text("p_data_src", "")
v_data_src=dbutils.widgets.get("p_data_src")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Renaming columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id")\
.withColumn("data_source", lit(v_data_src))\
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

final_df1= add_ingestion_date(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Writing to output to processed container in parquet format

# COMMAND ----------

#overwrite_partition(final_df1, 'f1_processed', 'lap_times', 'race_id')

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
merge_delta_data(final_df1, 'f1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_id' )

# COMMAND ----------

#display(spark.read.parquet(f"{processed_folder_path}/lap_times"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.lap_times;

# COMMAND ----------

dbutils.notebook.exit("Success")
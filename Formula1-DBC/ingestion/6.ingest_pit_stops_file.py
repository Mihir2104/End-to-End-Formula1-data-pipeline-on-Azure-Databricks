# Databricks notebook source
# MAGIC %md
# MAGIC ### ingest Pitstops.json file (multiline json)

# COMMAND ----------

dbutils.widgets.text("p_data_src", "")
v_data_src=dbutils.widgets.get("p_data_src")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run "../includes/common_functions"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("stop", StringType(), True),
                                     StructField("lap", IntegerType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("duration", StringType(), True),
                                     StructField("milliseconds", IntegerType(), True)])

# COMMAND ----------

pit_stops_df= spark.read\
    .schema(pit_stops_schema)\
    .option("multiline", True)\
    .json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id")\
.withColumnRenamed("raceId", "race_id")\
.withColumn("data_source", lit(v_data_src))\
.withColumn("file_date", lit(v_file_date))



# COMMAND ----------

final_df1 = add_ingestion_date(final_df)

# COMMAND ----------

#overwrite_partition(final_df1, 'f1_processed', 'pit_stops', 'race_id')

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(final_df1, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id' )

# COMMAND ----------

#display(spark.read.parquet(f"{processed_folder_path}/pit_stops"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.pit_stops;

# COMMAND ----------

dbutils.notebook.exit("Success")
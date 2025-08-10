# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - reading the csv file using the spark dataframe reader API

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                   StructField("year", IntegerType(), True),
                                   StructField("round", IntegerType(), True),
                                   StructField("circuitId", IntegerType(), True),
                                   StructField("name", StringType(), True),
                                   StructField("date", DateType(), True),
                                   StructField("time", StringType(), True),
                                   StructField("url", StringType(), True)])

# COMMAND ----------

races_df = spark.read\
    .option("header", True)\
    .schema(races_schema)\
    .csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2-  Adding ingestion date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, to_timestamp, col, lit

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("data_source", lit(v_data_src))\
.withColumn("ingestion_date", current_timestamp())\
.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))\
.withColumn("file_date",lit(v_file_date))
display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 3- selecting only the required columns & rename as required

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), col('round'), col('circuitId').alias('circuit_id'), col('name'), col('ingestion_date'), col('race_timestamp'),col('data_source'),col('file_date'))
display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing the output to processed container in parquet format

# COMMAND ----------

#races_selected_df.write.mode("overwrite").parquet(f"{processed_folder_path}/races")

# COMMAND ----------

#display(spark.read.parquet(f"{processed_folder_path}/races"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - partition data by race year

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

#display(spark.read.parquet(f"{processed_folder_path}/races"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.races;

# COMMAND ----------

dbutils.notebook.exit("Success")
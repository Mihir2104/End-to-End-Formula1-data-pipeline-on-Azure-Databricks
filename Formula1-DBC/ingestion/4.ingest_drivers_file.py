# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest drivers.json file

# COMMAND ----------

dbutils.widgets.text("p_data_src", "")
v_data_src=dbutils.widgets.get("p_data_src")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1 - reading the json file using dfreader api
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DataType

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, col,concat

# COMMAND ----------

name_schema= StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)])
drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                 StructField("driverRef", StringType(), True),
                                 StructField("number", IntegerType(), True),
                                 StructField("code", StringType(), True),
                                 StructField("name", name_schema),
                                 StructField("dob", StringType(), True),
                                 StructField("nationality", StringType(), True),
                                 StructField("url", StringType(), True)])

# COMMAND ----------

drivers_df = spark.read\
.schema(drivers_schema)\
.json(f"{raw_folder_path}/{v_file_date}/drivers.json")
display(drivers_df)


# COMMAND ----------

# MAGIC %md
# MAGIC Step-2 - Renaming columns and add new columns
# MAGIC 1. driverId renamed to driver_id
# MAGIC 2. driverRef renamed to driver_ref
# MAGIC 3. name added with concatenated forename and surname

# COMMAND ----------

drivers_renamed_df = drivers_df.withColumnRenamed("driverId", "driver_id")\
.withColumnRenamed("driverRef", "driver_ref")\
.withColumn("name",concat(col("name.forename"), lit(" "), col("name.surname")))\
.withColumn("data_source",lit(v_data_src))\
.withColumn("file_date",lit(v_file_date))


# COMMAND ----------

drivers_final_df1= add_ingestion_date(drivers_renamed_df)

# COMMAND ----------

display(drivers_final_df1)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3 - Dropping the unwanted columns
# MAGIC 1. name.forename
# MAGIC 2. name.surname
# MAGIC 3. url

# COMMAND ----------

drivers_final_df= drivers_final_df1.drop(col("url"))
drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("Success")
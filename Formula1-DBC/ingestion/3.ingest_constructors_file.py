# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 1- Reading the json file using dfreader api

# COMMAND ----------

dbutils.widgets.text("p_data_src", "")
v_data_src=dbutils.widgets.get("p_data_src")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

Constructors_schema= "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read\
    .schema(Constructors_schema)\
    .json(f"{raw_folder_path}/{v_file_date}/constructors.json")


# COMMAND ----------

# MAGIC %md
# MAGIC ### step 2 - Dropping unwanted columns from the df

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp,lit

# COMMAND ----------

constructor_dropped_df= constructor_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3- Renaming column and add ingestion date

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId","constructor_id")\
    .withColumnRenamed("constructorRef","constructor_ref")\
    .withColumn("data_source",lit(v_data_src))\
    .withColumn("file_date",lit(v_file_date))
    


# COMMAND ----------

constructor_final_df1= add_ingestion_date(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 4 - writing output to parquet file

# COMMAND ----------

display(constructor_final_df1)
constructor_final_df1.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.constructors; 

# COMMAND ----------

dbutils.notebook.exit("Success")
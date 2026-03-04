# Databricks notebook source
print("Hello world")

# COMMAND ----------

# required functions import
from pyspark.sql.functions import current_timestamp, input_file_name

# Configuration
src_path = "/Volumes/accenture/manishgautam/manishvolume/structuredStreaming/src"
checkpoint_path_bronze = "/Volumes/accenture/manishgautam/manishvolume/structuredStreaming/checkpoints/bronze"
#checkpoint_path_silver = "/Volumes/accenture/manishgautam/manishvolume/structuredStreaming/checkpoints/silver/"
#checkpoint_path_gold   = "/Volumes/accenture/manishgautam/manishvolume/structuredStreaming/checkpoints/gold/"
schema_path_bronze = "/Volumes/accenture/manishgautam/manishvolume/structuredStreaming/schema/bronze/"
#schema_path_silver = "/Volumes/accenture/manishgautam/manishvolume/structuredStreaming/schema/silver/"
#schema_path_gold = "/Volumes/accenture/manishgautam/manishvolume/structuredStreaming/schema/gold/"

# COMMAND ----------

# STEP 1: streaming dataframe create (Auto Loader)
df_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("cloudFiles.useNotifications", "false")  # yeh add karo
    .option("cloudFiles.schemaLocation", schema_path_bronze
)
    .load(src_path)
)

# COMMAND ----------

# STEP 2: Bronze layer metadata columns
bronze_df = (
    df_stream
    .withColumn("ingestion_time", current_timestamp())
    .withColumn("source_file", input_file_name())
)

# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Cell 6
# STEP 3: Write to Bronze Delta table (Unity Catalog)
query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path_bronze)
    .toTable("accenture.manishgautam.bronze_table")
)

# COMMAND ----------

# DBTITLE 1,Cell 7
spark.sql("select count
(*) from accenture.manishgautam.bronze_table").display()

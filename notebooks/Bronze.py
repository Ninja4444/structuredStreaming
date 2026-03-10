# Databricks notebook source
# DBTITLE 1,Cell 2
from pyspark.sql.functions import current_timestamp, input_file_name, from_utc_timestamp

src_path = "/Volumes/accenture/manishgautam/manishvolume/structuredStreaming/src"
checkpoint_path_bronze = "/Volumes/accenture/manishgautam/manishvolume/structuredStreaming/checkpoints/bronze"
#checkpoint_path_silver = "/Volumes/accenture/manishgautam/manishvolume/structuredStreaming/checkpoints/silver/"
#checkpoint_path_gold   = "/Volumes/accenture/manishgautam/manishvolume/structuredStreaming/checkpoints/gold/"
schema_path_bronze = "/Volumes/accenture/manishgautam/manishvolume/structuredStreaming/schema/bronze/"
#schema_path_silver = "/Volumes/accenture/manishgautam/manishvolume/structuredStreaming/schema/silver/"
#schema_path_gold = "/Volumes/accenture/manishgautam/manishvolume/structuredStreaming/schema/gold/"
bronze_table= 'accenture.manishgautam.bronze_table'

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 64)

# COMMAND ----------

# DBTITLE 1,Untitled
df_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("cloudFiles.useNotifications", "false")  
    .option("cloudFiles.schemaLocation", schema_path_bronze
    )
    .load(src_path))

# COMMAND ----------

# DBTITLE 1,Cell 4
bronze_df = (
    df_stream
    .withColumn("ingestion_time", current_timestamp()) 
    .withColumn("source_file", input_file_name())
)

# COMMAND ----------

#bronze_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Cell 6

query = (

    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path_bronze)
    .trigger(availableNow=True)   # process all available files and then stop and also the stream 
    .toTable(bronze_table)
)

# COMMAND ----------

#display(spark.sql("Select count(*) from accenture.manishgautam.bronze_table"))

# COMMAND ----------

from datetime import datetime, timezone, timedelta
ist = timezone(timedelta(hours=5, minutes=30))
print(f"notebook completed sucessfully at: {datetime.now(ist)}")

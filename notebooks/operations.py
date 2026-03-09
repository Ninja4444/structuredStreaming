# Databricks notebook source
# dbutils.fs.rm("/Volumes/accenture/manishgautam/manishvolume/structuredStreaming/checkpoints/gold_stream", recurse=True)
dbutils.fs.rm("/Volumes/accenture/manishgautam/manishvolume/structuredStreaming/checkpoints", recurse=True)
dbutils.fs.rm("/Volumes/accenture/manishgautam/manishvolume/structuredStreaming/schema", recurse=True)

# COMMAND ----------

##to performe operations:-

# COMMAND ----------

spark.sql("TRUNCATE TABLE accenture.manishgautam.bronze_table")
spark.sql("TRUNCATE TABLE accenture.manishgautam.silver_table")
spark.sql("TRUNCATE TABLE accenture.manishgautam.silver_table_stream")
spark.sql("TRUNCATE TABLE accenture.manishgautam.gold_table_stream")


# COMMAND ----------

display(spark.table("accenture.manishgautam.bronze_table").count())
print("\n---------------------")
display(spark.table("accenture.manishgautam.silver_table").count())
print("\n---------------------")
display(spark.table("accenture.manishgautam.silver_table_stream").count())
print("\n---------------------")
display(spark.table("accenture.manishgautam.gold_table_stream").count())

# COMMAND ----------

print(spark.table("accenture.manishgautam.silver_table").columns)

# COMMAND ----------

spark.sql("select silver_ingestion_time from accenture.manishgautam.silver_table_stream").display()

# COMMAND ----------

from datetime import datetime, timezone, timedelta
ist = timezone(timedelta(hours=5, minutes=30))
print(f"notebook completed sucessfully at: {datetime.now(ist)}")

# COMMAND ----------

print(spark.table("accenture.manishgautam.silver_table").columns)

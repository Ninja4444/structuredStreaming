# Databricks notebook source
##to performe operations:-

# COMMAND ----------

# display(spark.table("accenture.manishgautam.bronze_table").count())
# print("\n---------------------")
# display(spark.table("accenture.manishgautam.silver_table").count())
display(spark.table("accenture.manishgautam.silver_table_stream").count())

# COMMAND ----------

# spark.sql("TRUNCATE TABLE accenture.manishgautam.bronze_table")
# spark.sql("TRUNCATE TABLE accenture.manishgautam.silver_table")
spark.sql("TRUNCATE TABLE accenture.manishgautam.silver_table_stream")


# COMMAND ----------

print(spark.table("accenture.manishgautam.bronze_table").columns)

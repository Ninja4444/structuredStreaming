# Databricks notebook source
# Databricks notebook source

# 1. Imports
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

# 2. Table names
bronze_table = "accenture.manishgautam.bronze_table"
silver_table_stream = "accenture.manishgautam.silver_table_stream"

# COMMAND ----------

# 3. Read Bronze as stream
df_bronze_stream = spark.readStream.table(bronze_table)

# COMMAND ----------


# 4. Apply same transformations
df_silver_stream = (
    df_bronze_stream

    # Remove duplicates & nulls
    .dropDuplicates(["TransactionID"])
    .dropna(subset=["TransactionID", "PatientID", "Amount"])

    # Fix data types
    .withColumn("VisitDate", to_date("VisitDate","M/d/yyyy"))
    .withColumn("ServiceDate", to_date("ServiceDate","M/d/yyyy"))
    .withColumn("PaidDate", to_date("PaidDate","M/d/yyyy"))

    .withColumn("Amount", round(col("Amount"),2))
    .withColumn("PaidAmount", round(col("PaidAmount"),2))

    # New columns
    .withColumn("processing_days", datediff(col("PaidDate"),col("ServiceDate")))
    .withColumn("pending_amount", round(col("Amount") - col("PaidAmount"),2))
    .withColumn("payment_percentage", round((col("PaidAmount")/col("Amount"))*100,2))

    # Payment status
    .withColumn("payment_status",
        when(col("PaidAmount") >= col("Amount"),"FULLY_PAID")
        .when(col("PaidAmount") == 0,"UNPAID")
        .otherwise("PARTIALLY_PAID")
    )

    # Payor category
    .withColumn("payor_category",
        when(col("LineOfBusiness")=="MEDICARE","GOVERNMENT")
        .when(col("LineOfBusiness")=="MEDICAID","GOVERNMENT")
        .when(col("LineOfBusiness")=="COMMERCIAL","PRIVATE")
        .otherwise("OTHER")
    )

    # Same day flag
    .withColumn("is_same_day",
        when(col("VisitDate")==col("ServiceDate"),True).otherwise(False)
    )

    .withColumn("silver_ingestion_time", current_timestamp())

    # Filters
    .filter(col("Amount") > 0)
    .filter(col("PaidDate") >= col("ServiceDate"))
)


# COMMAND ----------


# 5. Merge logic for streaming
def upsert_to_silver(microBatchDF, batchId):

    if not spark.catalog.tableExists(silver_table_stream):
        (microBatchDF.write
         .format("delta")
         .mode("overwrite")
         .saveAsTable(silver_table_stream))
    else:
        delta_table = DeltaTable.forName(spark, silver_table_stream)

        (delta_table.alias("target")
         .merge(
            microBatchDF.alias("updates"),
            "target.TransactionID = updates.TransactionID"
         )
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())


# COMMAND ----------

# 6. Start stream
query = (
    df_silver_stream.writeStream
    .foreachBatch(upsert_to_silver)
    .option("checkpointLocation",
        "/Volumes/accenture/manishgautam/manishvolume/structuredStreaming/checkpoints/silver_stream")
    .outputMode("update")
    .start()
)

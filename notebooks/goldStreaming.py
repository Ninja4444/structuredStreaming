# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import *



spark.conf.set("spark.sql.shuffle.partitions", 64)

#silver_table= "accenture.manishgautam.silver_table"  #to run with batch
silver_table= "accenture.manishgautam.silver_table_stream" #to run with streaming
gold_table= "accenture.manishgautam.gold_table_stream"


df_silver = spark.readStream.table(silver_table)
checkpoint_path_gold="/Volumes/accenture/manishgautam/manishvolume/structuredStreaming/checkpoints/gold_stream"



# COMMAND ----------

# DBTITLE 1,Cell 2
df_gold_stream = (
    df_silver
    .groupBy("ServiceDate","DeptID","payor_category")
    .agg(
        approx_count_distinct("TransactionID").alias("total_transactions"),
        approx_count_distinct("PatientID").alias("unique_patients"),
        sum("Amount").alias("total_billed_amount"),
        sum("PaidAmount").alias("total_paid_amount"),
        sum("pending_amount").alias("total_pending_amount"),
        avg("payment_percentage").alias("avg_payment_percentage"),
        avg("processing_days").alias("avg_processing_days"),
        sum(when(col("payment_status")=="FULLY_PAID",1).otherwise(0)).alias("fully_paid_claims"),
        sum(when(col("payment_status")=="PARTIALLY_PAID",1).otherwise(0)).alias("partial_paid_claims"),
        sum(when(col("payment_status")=="UNPAID",1).otherwise(0)).alias("unpaid_claims")
    )
)

# COMMAND ----------

def upsert_to_gold(microBatchDF, batchId):

    row_count = microBatchDF.count()
    print(f"Batch ID: {batchId} | Rows processed: {row_count}")

    if not spark.catalog.tableExists(gold_table):
        (microBatchDF.write
         .format("delta")
         .mode("overwrite")
         .saveAsTable(gold_table))

    else:

        delta_table = DeltaTable.forName(spark, gold_table)

        (delta_table.alias("target")
         .merge(
             microBatchDF.alias("updates"),
             """
             target.ServiceDate = updates.ServiceDate
             AND target.DeptID = updates.DeptID
             AND target.payor_category = updates.payor_category
             """
         )
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())

# COMMAND ----------

query = (
    df_gold_stream.writeStream
    .foreachBatch(upsert_to_gold)
    .option("checkpointLocation", checkpoint_path_gold)
    .outputMode("update")
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

# COMMAND ----------

#display(spark.table("accenture.manishgautam.gold_table_stream").count())

# COMMAND ----------

display(spark.sql("select count(*) from accenture.manishgautam.gold_table_stream"))

# COMMAND ----------

from datetime import datetime, timezone, timedelta
ist = timezone(timedelta(hours=5, minutes=30))
print(f"notebook completed sucessfully at: {datetime.now(ist)}")

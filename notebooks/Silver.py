# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

bronze_table = "accenture.manishgautam.bronze_table"
silver_table = "accenture.manishgautam.silver_table"

# COMMAND ----------

df_bronze = spark.read.table(bronze_table)

# COMMAND ----------

# df_bronze.limit(2).display()

# COMMAND ----------

# DBTITLE 1,Cell 5
df_silver = (
    df_bronze

    #Remove duplicates & nulls
    .dropDuplicates(["TransactionID"])
    .dropna(subset=["TransactionID", "PatientID", "Amount"])

    # modufy datatype
    .withColumn("VisitDate",    to_date("VisitDate",    "M/d/yyyy"))
    .withColumn("ServiceDate",  to_date("ServiceDate",  "M/d/yyyy"))
    .withColumn("PaidDate",     to_date("PaidDate",     "M/d/yyyy"))
    .withColumn("Amount",       round(col("Amount"),    2))
    .withColumn("PaidAmount",   round(col("PaidAmount"),2))

    # adding few columns
    .withColumn("processing_days",datediff(col("PaidDate"), col("ServiceDate")))
    .withColumn("pending_amount",round(col("Amount") - col("PaidAmount"), 2))
    .withColumn("payment_percentage",round((col("PaidAmount") / col("Amount")) * 100, 2))

    # Payment Status
    .withColumn("payment_status",
    when(col("PaidAmount") >= col("Amount"),"FULLY_PAID") 
    .when(col("PaidAmount") == 0,"UNPAID")
    .otherwise("PARTIALLY_PAID"))

    # payor category chek
    .withColumn("payor_category",
        when(col("LineOfBusiness") == "MEDICARE",   "GOVERNMENT")
        .when(col("LineOfBusiness") == "MEDICAID",  "GOVERNMENT")
        .when(col("LineOfBusiness") == "COMMERCIAL","PRIVATE")
        .otherwise("OTHER"))

    # Same Day treatment
    .withColumn("is_same_day",
        when(col("VisitDate") == col("ServiceDate"), True)
        .otherwise(False))

    
    .withColumn("silver_ingestion_time", current_timestamp())

    .filter(col("Amount") > 0)
    .filter(col("PaidDate") >= col("ServiceDate"))

#chnge the order of columns
    .select(
        "TransactionID", "PatientID", "ProviderID", "DeptID",
        "VisitDate", "ServiceDate", "PaidDate", "VisitType",
        "Amount", "PaidAmount", "pending_amount",
        "payment_status", "payment_percentage", "processing_days",
        "payor_category", "is_same_day",
        "ClaimID", "PayorID", "LineOfBusiness",
        "silver_ingestion_time"
    )

#dtaatype change
    .withColumn("Amount",     col("Amount").cast("double"))
    .withColumn("PaidAmount", col("PaidAmount").cast("double"))
)

# Display result
#df_silver.limit(5).display()

# COMMAND ----------

# DBTITLE 1,Cell 6
# Drop table if it exists (safer than TRUNCATE for tables that may not exist yet)
#spark.sql(f"DROP TABLE IF EXISTS {silver_table}")

# COMMAND ----------

# DBTITLE 1,Cell 7
if not spark.catalog.tableExists(silver_table):
    (df_silver.write
     .format("delta")
     .mode("overwrite")
     .saveAsTable(silver_table))
    print(f"Success: {silver_table} created and loaded for the first time.")
else:
    target_table = DeltaTable.forName(spark, silver_table)

    (target_table.alias("target").merge(df_silver.alias("updates"), "target.TransactionID = updates.TransactionID")
     
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())
    print(f"Success: Data merged into {silver_table}.")

# COMMAND ----------

# display(spark.table("accenture.manishgautam.silver_table_stream").count())

# COMMAND ----------

from datetime import datetime, timezone, timedelta
ist = timezone(timedelta(hours=5, minutes=30))
print(f"data copy to silver: {datetime.now(ist)}")

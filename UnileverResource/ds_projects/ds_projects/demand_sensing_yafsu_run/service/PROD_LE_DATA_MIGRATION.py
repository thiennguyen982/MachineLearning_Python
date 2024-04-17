# Databricks notebook source
from pyspark.sql import functions as F
import pandas as pd
import pyspark.pandas as ps_pd 
import datetime as dt
import numpy as np
# import openpyxl
NUMBER_OF_PARTITIONS = sc.defaultParallelism * 2

# COMMAND ----------

# MAGIC %md
# MAGIC # DATA LOAD

# COMMAND ----------

# MAGIC %md
# MAGIC ## MIGRATION DATA

# COMMAND ----------

migration_link= "dbfs:/mnt/adls/MDL_Prod/Silver/DistributiveTrade/Dimension/dim_vn_historical_outlet_map/"
df_migration = spark.read.format("delta").options(header = True).load(migration_link)

# COMMAND ----------

display(df_migration)

# COMMAND ----------

# MAGIC %md
# MAGIC ## LE SEC SALES RAW

# COMMAND ----------

DF_LE_SEC_SALES = spark.read.format("delta").load(
    "dbfs:/mnt/adls/Prod_CD_BDL/BusinessDataLake/CD/SecSales/Global/Online_Countries/Transactional/OnlineInvoiceSKUSales/OnlineInvoiceSKUSales_hist_parquet/"
)
DF_LE_SEC_SALES = DF_LE_SEC_SALES.where('CountryCode == "VN" AND InvoiceDate > "2019-01-01"')
DF_LE_SEC_SALES = DF_LE_SEC_SALES.withColumn("MATERIAL", F.col("ArticleCode").cast("int"))

# COMMAND ----------

# display(DF_LE_SEC_SALES)

# COMMAND ----------

# MAGIC %md
# MAGIC ## LE OUTLET MASTER

# COMMAND ----------

DF_LE_OUTLET_MASTER = spark.read.format("delta").load("dbfs:/mnt/adls/MDL_Prod/Silver/DistributiveTrade/Dimension/dim_vn_outlet_master/")
DF_LE_OUTLET_MASTER = DF_LE_OUTLET_MASTER.where('country_code == "VN"')

# COMMAND ----------

display(DF_LE_OUTLET_MASTER)

# COMMAND ----------

# MAGIC %md
# MAGIC ## LE DISTRIBUTOR MASTER

# COMMAND ----------

DF_LE_CUSTOMER_MASTER = spark.read.format("delta").load(
    "dbfs:/mnt/adls/MDL_Prod/Silver/DistributiveTrade/Dimension/dim_vn_distributor_master/"
)
DF_LE_CUSTOMER_MASTER = DF_LE_CUSTOMER_MASTER.where("country_code == 'VN'")

# COMMAND ----------

# display(DF_LE_CUSTOMER_MASTER)

# COMMAND ----------

# MAGIC %md
# MAGIC # JOIN SEC SALES WITH DISTRIBUTOR MASTER

# COMMAND ----------

DF_LE_SEC_SALES_CUST_MASTER_MERGE = DF_LE_SEC_SALES.join(DF_LE_CUSTOMER_MASTER.select("cust_shipp_code", "site_code", "dt_type", "sales_org"), DF_LE_SEC_SALES.SiteCode == DF_LE_CUSTOMER_MASTER.site_code, how = "left") \
  .drop(F.col("site_code")) \
  .distinct() 

# perform filters
DF_LE_SEC_SALES_CUST_MASTER_MERGE_FITERED = DF_LE_SEC_SALES_CUST_MASTER_MERGE.where(
    "dt_type == 'C10000' AND sales_org == 'V001' AND GrossSalesValue != 0 "
)

# COMMAND ----------

# display(DF_LE_SEC_SALES_CUST_MASTER_MERGE_FITERED)

# COMMAND ----------

# Update Invoice Date
DF_LE_SEC_SALES_CUST_MASTER_MERGE_FITERED.createOrReplaceTempView("test_dt")
sql_query = """
WITH B AS (
  SELECT DISTINCT InvoiceNumber as B_InvoiceNumber, 
  InvoiceDate as B_InvoiceDate
  FROM test_dt
)
SELECT *,
  CASE WHEN B.B_InvoiceDate IS NULL  
       THEN A.InvoiceDate
       ELSE B.B_InvoiceDate
  END AS UpdatedInvoiceDate
FROM test_dt as A 
LEFT JOIN B
  ON A.SalesReturnReferenceInvoiceNumber = B.B_InvoiceNumber;
"""
# Execute the SQL query and create a DataFrame
DF_LE_SEC_SALES_CUST_MASTER_MERGE_FITERED_INVOICEDATE = spark.sql(sql_query)

# COMMAND ----------

# display(DF_LE_SEC_SALES_CUST_MASTER_MERGE_FITERED_INVOICEDATE)

# COMMAND ----------

# add date time vars
DF_LE_SEC_SALES_CUST_MASTER_MERGE_FITERED_INVOICEDATE = (
    DF_LE_SEC_SALES_CUST_MASTER_MERGE_FITERED_INVOICEDATE.withColumn("YEAR", F.year(F.col("UpdatedInvoiceDate")))
    .withColumn("MONTH", F.month(F.col("UpdatedInvoiceDate")))
    .withColumn("DAY", F.dayofmonth(F.col("UpdatedInvoiceDate")))
    .withColumn("WEEK", F.weekofyear(F.col("UpdatedInvoiceDate")))
    .withColumn("YEARMONTH", F.col("YEAR") * 100 + F.col("MONTH"))
    .withColumn(
        "YEARWEEK", F.concat(F.col("YEAR"), F.lpad(F.col("WEEK"), 2, "0"))
        
    )
)

DF_LE_SEC_SALES_CUST_MASTER_MERGE_FITERED_INVOICEDATE = DF_LE_SEC_SALES_CUST_MASTER_MERGE_FITERED_INVOICEDATE.where("YEARWEEK >= 202001")

# COMMAND ----------

# display(DF_LE_SEC_SALES_CUST_MASTER_MERGE_FITERED_INVOICEDATE)

# COMMAND ----------

#create a subset 
DF_LE_SEC_SALES_CLEAN_FEWER_COLS = DF_LE_SEC_SALES_CUST_MASTER_MERGE_FITERED_INVOICEDATE.groupby(["MATERIAL", "UpdatedInvoiceDate", "YEARWEEK", "OutletCode", "SiteCode"]).agg(F.sum(F.col("SalesPCQuantity")))

# COMMAND ----------

# display(DF_LE_SEC_SALES_CLEAN_FEWER_COLS)

# COMMAND ----------

# MAGIC %md
# MAGIC # OUTLET MIGRATION

# COMMAND ----------

DF_LE_SEC_SALES_CLEAN_FEWER_COLS_migration = DF_LE_SEC_SALES_CLEAN_FEWER_COLS.join(
    df_migration.select("prev_outlet_code", "outlet_code", "site_code"),
    DF_LE_SEC_SALES_CLEAN_FEWER_COLS["OutletCode"] == df_migration["prev_outlet_code"],
    how="left",
)
DF_LE_SEC_SALES_CLEAN_FEWER_COLS_migration = (
    DF_LE_SEC_SALES_CLEAN_FEWER_COLS_migration.withColumn(
        "OutletCode_latest",
        F.when(F.col("prev_outlet_code").isNull(), F.col("OutletCode")).otherwise(
            F.col("outlet_code")
        ),
    )
)
DF_LE_SEC_SALES_CLEAN_FEWER_COLS_migration = (
    DF_LE_SEC_SALES_CLEAN_FEWER_COLS_migration.withColumn(
        "SiteCode_latest",
        F.when(F.col("site_code").isNull(), F.col("SiteCode")).otherwise(
            F.col("site_code")
        ),
    )
)

# COMMAND ----------

# display(DF_LE_SEC_SALES_CLEAN_FEWER_COLS_migration)

# COMMAND ----------

'''
some distributors have not been fully migrated in migration data, need to ask CD for help, below is an example
'''
# df_migration_err = DF_LE_SEC_SALES_CLEAN_FEWER_COLS_migration.where("SiteCode == '3294' AND (SiteCode_latest == '3255' OR SiteCode_latest == '3294')")
# display(df_migration_err.where("prev_outlet_code is null"))

# COMMAND ----------

# MAGIC %md
# MAGIC # RE-AGG AFTER OUTLET AGG

# COMMAND ----------

DF_LE_SEC_SALES_CLEAN_FEWER_COLS_post_migration = (
    DF_LE_SEC_SALES_CLEAN_FEWER_COLS_migration.select(
        "MATERIAL",
        "UpdatedInvoiceDate",
        "YEARWEEK",
        "OutletCode_latest",
        "SiteCode_latest",
        "sum(SalesPCQuantity)",
    )
)
DF_LE_SEC_SALES_re_agg = DF_LE_SEC_SALES_CLEAN_FEWER_COLS_post_migration.groupby(
    [
        "MATERIAL",
        "UpdatedInvoiceDate",
        "YEARWEEK",
        "OutletCode_latest",
        "SiteCode_latest",
    ]
).agg(F.sum(F.col("sum(SalesPCQuantity)")))

# COMMAND ----------

# display(DF_LE_SEC_SALES_re_agg)

# COMMAND ----------

# MAGIC %md
# MAGIC # GET OUTLET CHANNELS
# MAGIC

# COMMAND ----------

DF_LE_SEC_SALES_get_channels = DF_LE_SEC_SALES_re_agg.join(
    DF_LE_OUTLET_MASTER.select("outlet_code", "channel"),
    DF_LE_SEC_SALES_re_agg["OutletCode_latest"] == DF_LE_OUTLET_MASTER["outlet_code"],
    how="left",
)
DF_LE_SEC_SALES_get_channels = (
    DF_LE_SEC_SALES_get_channels.withColumnRenamed("channel", "OUTLET_TYPES")
    .withColumnRenamed("sum(sum(SalesPCQuantity))", "PCS")
    .drop("outlet_code")
    .distinct()
)

# COMMAND ----------

# display(DF_LE_SEC_SALES_get_channels)

# COMMAND ----------

# MAGIC %md
# MAGIC # GET BANNER, SHIPTO

# COMMAND ----------

DF_LE_SEC_SALES_get_banner_and_shipto = (
    DF_LE_SEC_SALES_get_channels.join(
        DF_LE_CUSTOMER_MASTER.select("site_code", "dt_region", "cust_shipp_code"),
        DF_LE_SEC_SALES_get_channels["SiteCode_latest"]
        == DF_LE_CUSTOMER_MASTER["site_code"],
        how="left",
    )
    .drop(F.col("site_code"))
    .distinct()
)

DF_LE_SEC_SALES_get_banner_and_shipto = (
    DF_LE_SEC_SALES_get_banner_and_shipto.withColumnRenamed(
        "dt_region", "BANNER"
    ).withColumnRenamed("cust_shipp_code", "DISTRIBUTOR")
)

# COMMAND ----------

# display(DF_LE_SEC_SALES_get_banner_and_shipto)

# COMMAND ----------

DF_LE_SEC_SALES_get_banner_and_shipto.repartition(NUMBER_OF_PARTITIONS).write.mode("overwrite").parquet(
    "dbfs:/mnt/adls/DT_PREDICTEDORDER/LE_SEC_SALE_DAILY/LE_SEC_SALES_RAW_UPDATED_PARQUET"
)

# COMMAND ----------


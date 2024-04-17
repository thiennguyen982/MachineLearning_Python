# Databricks notebook source
spark.sparkContext.version

# COMMAND ----------

# MAGIC %md
# MAGIC # Customer Information
# MAGIC - Customer Hierachy
# MAGIC - Outlet master
# MAGIC - Online Outlet Master
# MAGIC - Outlet master
# MAGIC - Customer Master
# MAGIC - Migration data
# MAGIC - Local Outlet Channel Mapping - VN_OL_CHANNEL

# COMMAND ----------

customer_hierarchy = spark.read.delta("/mnt/adls/Prod_UDL/TechDebt/InternalSources/U2K2BW/OpenHubFileDestination/CustomerMaster/SouthEastAsiaAustralasia/Processed_Parquet")

# COMMAND ----------

customer_hierarchy_path = "/mnt/adls/Prod_UDL/TechDebt/InternalSources/U2K2BW/OpenHubFileDestination/CustomerMaster/SouthEastAsiaAustralasia/Processed_Parquet"
customer_hierarchy = spark.read.format("delta").load(customer_hierarchy_path)

# COMMAND ----------

display(customer_hierarchy)

# COMMAND ----------

df = customer_hierarchy.pandas_api()

# COMMAND ----------

df["COUNTRY"].nunique()

# COMMAND ----------

print(f"Number of customer sales: {df['CUST_SALES'].nunique()}")

# COMMAND ----------

print("Number of customer breaking down by COUNTRY")
data = df.groupby("COUNTRY")["CUST_SALES"].nunique().reset_index().sort_values(by = "CUST_SALES", ascending = False)
data["%CUST_SALES"] = data["CUST_SALES"]*100/data["CUST_SALES"].sum()
data.head(10)

# COMMAND ----------

df["YYYY"].unique()

# COMMAND ----------

# df.loc[df["COUNTRY"] == "VN"].groupby("CUST_SALES")["PROCESSED_DATE"].rank().reset_index()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Outlet Master
# MAGIC format:  csv
# MAGIC KEY: BIC/ZSD OTL, YYYY, MM, DD

# COMMAND ----------

df =  spark.read.options(delimiter = "|").csv("/mnt/adls/Prod_UDL/TechDebt/InternalSources/U2K2BW/OpenHubFileDestination/OutletMaster/Vietnam/Processed/", header = True).pandas_api()



# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Supply Chain Master
# MAGIC - Master data total cat
# MAGIC - Product Master
# MAGIC - Master ML Calendar

# COMMAND ----------

!pip install openpyxl
import pandas as pd 
import pyspark.pandas as ps
master_ml_calendar = "/mnt/adls/DT_SNOP_TOTALFC_BASELINE/LANDING-MASTER/Master ML Calendar.xlsx"
df = ps.read_excel(master_ml_calendar)

# COMMAND ----------

df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Promotion
# MAGIC - DT SNOP weekly promotion check
# MAGIC - DT SNOP banded promotion
# MAGIC - Product UOM price
# MAGIC - Promotion Header
# MAGIC - SC ESS Promotion
# MAGIC - SC Promotion Library
# MAGIC - SC KA Promotion
# MAGIC - SC Demand Sensing Promotion Timing
# MAGIC - MT SNOP PPM Promotion
# MAGIC - CSM Code
# MAGIC _ Mapping sales BOM vs CSM
# MAGIC - TPR data
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

dt_snop_weekly_promotion_check = "/mnt/adls/DT_SNOP_TOTALFC_BASELINE/LANDING-PROMO/"

df = ps.read_excel(dt_snop_weekly_promotion_check)
df.head()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Calendar
# MAGIC

# COMMAND ----------

df = ps.read_csv("/mnt/adls/BHX_FC/FBFC/DATA/CALENDAR.csv")

# COMMAND ----------

df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## SC Forecast Result
# MAGIC - DT SNOP TOTALFC BASELINE

# COMMAND ----------

df = ps.read_excel("/mnt/adls/DT_SNOP_TOTALFC_BASELINE/OUTPUT_FORECAST/FC_BASELINE_SEC2PRI_HIS/")

# COMMAND ----------

!ls /dbfsmnt/adls/DT_SNOP_TOTALFC_BASELINE/OUTPUT_FORECAST/FC_BASELINE_SEC2PRI_HIS/

# COMMAND ----------


# Databricks notebook source
# Import Neccessary Library
import numpy as np
import pandas as pd
import mlflow

# Spark Function
import pyspark.pandas as ps
from pyspark.sql.functions import expr

# Visualization
import matplotlib.pyplot as plt

import seaborn as sns
import seaborn.objects as so
import plotly
import plotly.express as px

from datetime import date
# from typing import AnyType

today = date.today()

mlflow.log_param("today", str(today))
mlflow.log_param("spark_version", spark.sparkContext.version)


# COMMAND ----------

# Databrick Function Utils
def read_data(path: str, format: str = "csv", header: bool = True):
  """
  Read data from any format
  """
  # df = spark.read.options(delimiter = "|", header = header, format = format).load(path)
  if format == "parquet":
    df = spark.read.options(delimiter = "|", header = True).parquet(path)
  elif format == "csv":
    df = spark.read.options(delimiter = "|", header = True).csv(path)
  elif format == "delta":
    df = spark.read.options(delimiter = "|", header = True).format("delta").load(path)
  else:
    df = spark.read.options(delimiter = "|", header = header, format = format).load(path)
  print(f"Schema: \n{df.printSchema()}")
  return df


# COMMAND ----------

primary_sales_banner_weekly_path = "/mnt/adls/SAP_HANA_DATASET/RAW_DATA/PRI_SALES_BANNER_DAILY_PARQUET"

primary_sales_banner_weekly = read_data(primary_sales_banner_weekly_path, format = "parquet")
primary_sales_banner_weekly_df = primary_sales_banner_weekly.pandas_api().to_pandas()
product_master = spark.read.format('csv').options(delimiter = ",", header = "True").load('/mnt/adls/BHX_FC/FBFC/DATA/PRODUCT_MASTER.csv')
product_master_df = product_master.pandas_api().to_pandas()
primary_sales_banner_weekly_df["DATE"] = pd.to_datetime(primary_sales_banner_weekly_df["DATE"])
primary_sales_banner_weekly_df["MATERIAL"] = primary_sales_banner_weekly_df["MATERIAL"].astype("int").astype("str")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Descritive Analysis

# COMMAND ----------

primary_sales_banner_weekly_df.head()

# COMMAND ----------



display(product_master_df)

# COMMAND ----------

primary_sales_banner_weekly_hierarchy_df = primary_sales_banner_weekly_df.merge(product_master_df, how = "left", left_on = "MATERIAL", right_on = "Material")

primary_sales_banner_weekly_hierarchy_df.head()

# COMMAND ----------

product_master_df.Material.unique()

# COMMAND ----------

primary_sales_banner_weekly_df.describe(include = "all")

# COMMAND ----------

print(f"""
BANNER: {primary_sales_banner_weekly_df["BANNER"].unique()}
REGION: {primary_sales_banner_weekly_df["REGION"].unique()}
      """)

# COMMAND ----------

psbw_coop = primary_sales_banner_weekly_df.loc[(primary_sales_banner_weekly_df["BANNER"].isin(['SAIGON COOP', 'BIG_C']))&(primary_sales_banner_weekly_df["REGION"] == "HO CHI MINH - EAST")&(primary_sales_banner_weekly_df["DATE"] >= '2022-01-01')].groupby(["DATE", "BANNER", "REGION"])["PCS"].sum().reset_index()
# sns.lineplot(data = psbw_coop, x = "DATE", y = "PCS")
psbw_coop.head()


# COMMAND ----------

psbw_coop.head()

# COMMAND ----------

px.line(data_frame = psbw_coop.groupby(["BANNER"]).resample("W-Mon", on = "DATE").sum().reset_index(), x = "DATE", y = "PCS", color = "BANNER")


# COMMAND ----------

product_hierarchy_path = "/mnt/adls/Prod_UDL/TechDebt/InternalSources/U2K2BW/OpenHubFileDestination/ProductMaster/SouthEastAsiaAustralasia/Processed/"

product_hierarchy = read_data(product_hierarchy_path, format = "csv")
product_hierarchy_df = product_hierarchy.pandas_api().to_pandas()

# COMMAND ----------

product_hierarchy_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC # Promotion

# COMMAND ----------

uom_price_df = pd.read_csv("/dbfs/mnt/adls/NMHDAT_SNOP/DT/Promotion/promotion_uom_price.csv")


# COMMAND ----------

uom_price_df.head()

# COMMAND ----------

sc_promotion_library = "/dbfs/mnt/adls/BHX_FC/FBFC/DATA/2022 Promotion (for upload).xlsx"
sc_promotion_library_df = pd.read_csv("/dbfs/mnt/adls/BHX_FC/FBFC/DATA/promotion_data.csv")

# COMMAND ----------

sc_promotion_library_df.head()

# COMMAND ----------

primary_sales_banner = spark.read.options(delimiter = "|", header = True).parquet("/mnt/adls/SAP_HANA_DATASET/RAW_DATA/PRI_SALES_BANNER_DAILY_PARQUET").pandas_api().to_pandas()

# COMMAND ----------

mt_snop_ppm_promotion_path = "/mnt/adls/MDL_Prod/Bronze/PPM/PIDReport/VN/Processed"
# mt_snop_ppm_promotion_df = read_data(mt_snop_ppm_promotion_path, format = "csv")
mt_snop_ppm_promotion = read_data(mt_snop_ppm_promotion_path, format = "delta")
mt_snop_ppm_promotion_df = mt_snop_ppm_promotion.pandas_api().to_pandas()
mt_snop_ppm_promotion_df.head()



# COMMAND ----------

mt_snop_ppm_promotion_df.head()

# COMMAND ----------

primary_sales_banner["DATE"] = pd.to_datetime(primary_sales_banner["DATE"])

# COMMAND ----------

primary_sales_banner.head()

# COMMAND ----------

data = primary_sales_banner.groupby(["DATE", "BANNER"])["GSV"].mean().reset_index()
px.line(data_frame = data.loc[data["BANNER"].isin(["BIG_C", "SAIGON COOP"])], x = "DATE", y = "GSV", color = "BANNER")



# COMMAND ----------

data.head()

# COMMAND ----------



fig, axes = plt.subplots(1, 1, figsize = (12, 8))
data = primary_sales_banner.loc[(primary_sales_banner["BANNER"] == "AEON")&(primary_sales_banner["DATE"] >= '2023-06-01')].groupby(["DATE", "BANNER"])["GSV"].mean().reset_index()
# sns.lineplot(data = data, x = "DATE", y = "GSV", hue = "REGION", ax = axes)
px.line(data_frame = data, x = "DATE", y = "GSV", color = "REGION")

# data.head()


# COMMAND ----------

primary_sales_banner.loc[primary_sales_banner["BANNER"] == "BIG_C"].plot.line(x = "DATE", y = "GSV")

# COMMAND ----------

hana_secondary_sale_daily_banner = spark.read.options(delimiter = "|").parquet("/mnt/adls/SAP_HANA_DATASET/RAW_DATA/SEC_SALES_BANNER_DAILY_PARQUET")

# COMMAND ----------

display(hana_secondary_sale_daily_banner)

# COMMAND ----------

df = hana_secondary_sale_daily_banner.pandas_api().to_pandas()

# COMMAND ----------

df["BANNER"].unique()

# COMMAND ----------


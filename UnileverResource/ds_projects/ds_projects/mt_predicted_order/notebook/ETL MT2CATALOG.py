# Databricks notebook source
import numpy as np
import pandas as pd
import pyspark.pandas as ps
import re
from pathlib import Path
import pyarrow 
from typing import Union, List, Optional
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count

import polars as pl
spark.sparkContext.setLogLevel("DEBUG")


# COMMAND ----------

# Function Utils

class IOSpark:
    def __init__(self):
        pass
    
    def read_excel_batch(
        self, files_path, header: bool = True, sheet_name: str = None, dtypes: dict = None
    ) -> pd.DataFrame:
        dfs = []
        for file_path in files_path:
            # df = pd.read_excel(file_path, header = header, sheet_name = sheet_name)
            df = pl.read_excel(file_path, engine = "calamine", read_options={"header_row": header}, sheet_name = sheet_name)
            # df = df.to_pandas()
            if isinstance(df, dict):
                print(f"list of sheet_name: {df.keys()}")
                df = df.items()[0][1]
            dfs.append(df)
        data = pl.concat(dfs, how = "vertical_relaxed")
        # data = data.apply(lambda x: x.astype(str))
        return data.to_pandas()
    def read_excel_spark(
        self, pdf, header: bool = True, sheet_name: str = None
    ) -> ps.DataFrame:
        df = self.read_excel_batch(pdf["file_path"].tolist(), header, sheet_name)
        return df

def read_excel_spark(
    file_path: Union[List, str, Path], header: bool = True, sheet_name: str = None
):
    if isinstance(file_path, str):
        return ps.read_excel(file_path, header=header, sheet_name=sheet_name)
    elif isinstance(file_path, Path):
        file_path = [str(fp) for fp in file_path]
    df_path:ps.DataFrame = ps.DataFrame({"file_path": file_path})
    io_spark = IOSpark()

    def read_excel_spark_io(
        pdf, header: bool = True, sheet_name: str = None
    ):
        return io_spark.read_excel_batch(
            pdf["file_path"].tolist(), header=header, sheet_name=sheet_name
        )
    df_spark = df_path.spark.repartition(32).pandas_on_spark.apply_batch(
        read_excel_spark_io, header=header, sheet_name=sheet_name
    )
    
    return df_spark

# COMMAND ----------

def clean_column_name(column_name: str):
  # strip text
  column_name = column_name.strip()
  column_name = column_name.lower()

  # remove special symbols from string
  # column_name = re.sub("[^A-Za-z0-9\space]+", '', column_name)
  # Replace or remove invalid characters as needed
  # This regex replaces any character not a letter, number, or underscore with an underscore
  column_name = re.sub(r'[^\w]', '_', column_name)
  return column_name
clean_column_name(" df ne()")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- create mt_predicted_order dataset 
# MAGIC CREATE SCHEMA IF NOT EXISTS mt_predicted_order LOCATION '/mnt/adls/mt_predicted_order'
# MAGIC

# COMMAND ----------

root_path = "/mnt/adls"
promotion_lib_path = f"{root_path}/MT_POST_MODEL/PROMOTION_LIB/"
remove_repeated_path = f"{root_path}/MT_POST_MODEL/REMOVE REPEATED/"
master_path = f"{root_path}/MT_POST_MODEL/MASTER"

# COMMAND ----------

# Create table product master
PATH_PRODUCT_MASTER = f"/mnt/adls/MT_POST_MODEL/MASTER/PRODUCT MASTER.csv"
df = ps.read_csv(PATH_PRODUCT_MASTER)
# Clean column
df.columns = [clean_column_name(c) for c in df.columns]

# Write data
(
  df.to_spark()
  .write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .saveAsTable("mt_predicted_order.product_master")
)


# COMMAND ----------

df_header = pd.read_excel(f"/dbfs/{promotion_lib_path}/2023 Promotion (for upload).xlsx", header = [0, 1, 2, 3, 4, 5, 6])
# df_header = df_header.dropna(axis = "columns", how = "all")
df_header.columns = df_header.columns.get_level_values(1)
usecols = []
for idx, col in enumerate(df_header.columns):
  if pd.notnull(col) and df_header.iloc[:, idx].nunique() > 1 and df_header.iloc[:, idx].isnull().sum() == 0:
    usecols.append([idx, col])

df_promotion = ps.read_excel(promotion_lib_path, 
                             header = None,
                             skiprows=7,
                             usecols = [col[0] for col in usecols],
                             dtype = 'str',
                             engine='openpyxl',
                             sheet_name = 0)
df_promotion.columns = [col[1] for col in usecols]    
df_promotion.columns = [clean_column_name(c) for c in df_promotion.columns]
display(df_promotion)

# COMMAND ----------


# Write data
(
  df_promotion.to_spark()
  .write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .saveAsTable("mt_predicted_order.mt_promotion")
)


# COMMAND ----------

# def executor_func(tasks: List, func, **args):
#   with ProcessPoolExecutor(cpu_count()) as executor:
#     future_to_task = {executor.submit(func, task, **args): task for task in tasks}
#     results = []
#     for idx, future in enumerate(as_completed(future_to_task)):
#       task = future_to_df[future]
#       print(f"task {idx}: {task}")
#       try:
#         data = future.result()
#         results.append(data)
#       except Exception as exc:
#         print(f"{data} generated an exception: {exc}")
#     return results



# file_names = [] 
# for file_name in Path("/dbfs"+remove_repeated_path).glob("*.xlsx"):
#   file_name = str(file_name)
#   if 'BIG_C' in file_name or 'COOP' in file_name:
#     file_names.append(file_name)
# write_file_path = Path("/dbfs/mnt/adls/elt/mt_predicted_order/sale")
# write_file_path.mkdir(parents = True, exist_ok = True)
# def read_excel(file_path):
#   file_name = file_path.split("/")[-1].split(".")[-2]
#   df: pl.DataFrame = pl.read_excel(file_path, engine = "calamine", read_options={"header_row": 1}, sheet_name = "Raw data")
#   df.write_parquet(write_file_path.joinpath(f"{file_name}.parquet"))
#   return None

# # executor_func(file_names[:10], read_excel)
# for file_path in file_names:
  
#   file_name = file_path.split("/")[-1].split(".")[-2]
#   write_file_name = write_file_path.joinpath(f"{file_name}.parquet")
#   if write_file_name.exists() == False:
#     df: pl.DataFrame = pl.read_excel(file_path, engine = "calamine", read_options={"header_row": 1}, sheet_name = "Raw data")
#     print(f"write to: {write_file_name}")
#     df.write_parquet(write_file_path.joinpath(f"{file_name}.parquet"))
#   else:
#     print(f"file_path: {file_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write excel to parquet

# COMMAND ----------

file_paths = [] 
for file_path in Path("/dbfs"+remove_repeated_path).glob("*.xlsx"):
  file_name = str(file_path)
  if 'BIG_C' in file_name or 'COOP' in file_name:
    file_paths.append(file_name)

print("file_paths: ", file_paths)
file_path_sale = Path("/dbfs/mnt/adls/elt/mt_predicted_order/sale")
for file_path in file_paths:
  
  file_name = file_path.split("/")[-1].split(".")[-2]
  file_path_sale_parquet = file_path_sale.joinpath(f"{file_name}.parquet")
  if file_path_sale_parquet.exists() == False:
    df: pl.DataFrame = pl.read_excel(file_path, engine = "calamine", read_options={"header_row": 1}, sheet_name = "Raw data")
    for col in df.columns:
      df = df.with_columns(pl.col(col).cast(pl.Utf8))
    print(f"write to: {file_path_sale_parquet}")
    df.write_parquet(file_path_sale_parquet)
  else:
    print(f"pass file_path: {file_path}")



# COMMAND ----------

df_sale = ps.read_parquet("/mnt/adls/elt/mt_predicted_order/sale")
df_sale.head()

# COMMAND ----------

# usecols = [
#   "Banner",
# "Region",
# "Sold To",
# "Ship To",
# "Ship To Name",
# "PO No.",
# "Week/Month",
# "PO Date",
# "PO Received Date",
# "Keying Date",
# "Billing Date",
# "U RDD",
# "PO RDD",
# "PO End Date",
# "Division",
# "Category",
# "Brand",
# "ULV Code",
# "ULV Description",
# "Order (CS)",
# "Key In After(CS)",
# "Key In After(Value)",
# "Loss at Keying (CS)",
# "Loss at Keying (Value)",
# "Delivery Plan (CS)",
# "Loss at Delivery Plan (CS)",
# "Billing (CS)",
# "Loss at Billing (CS)",
# # "Loss at Keying",
# "Loss at Delivery Plan",
# # "Customer Price (cs)",
# "U Price (cs)",
# "Promotion Name",
# "DP Name",
# "Activity ID",
# "Original Promotion Name",
# "Promotion Check",
# "Est. Demand (cs)",
# "Total Loss (CS)",
# "INFO",
# ]
# df_sale = ps.read_excel(remove_repeated_path, 
#                          header=1, 
#                          sheet_name="Raw data",
#                          dtype = "str",
#                          engine='openpyxl',
#                          usecols = usecols)
df_sale.columns = [clean_column_name(c) for c in df_sale.columns]


# Write data
# df_sale.to_delta("mt_predicted_order.mt_sale", mode = "w" )
(
  df_sale.to_spark()
  .write
  # .partitionBy("billing_date")
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .saveAsTable("mt_predicted_order.mt_sale")
  
)


# COMMAND ----------

from datetime import datetime

file_name = str(Path("/dbfs"+remove_repeated_path).glob("*.xlsx").__next__())
start = datetime.now()
df = pl.read_excel(file_name, engine = "calamine", read_options={"header_row": 1}, sheet_name = "Raw data")
print("[pl]Time: ", datetime.now() - start)

start = datetime.now()
df = pd.read_excel(file_name, header = 1, sheet_name = "Raw data")
print("[pd]Time: ", datetime.now() - start)

# COMMAND ----------


file_names = [] 
for file_name in Path("/dbfs"+remove_repeated_path).glob("*.xlsx"):
  file_name = str(file_name)
  if 'BIG_C' in file_name or 'COOP' in file_name:
    file_names.append(file_name)
print("Len of file_names: ", len(file_names))

def read_excel(file_name: str, header: Union[int, str], sheet_name: str):
  # df = pl.read_excel(file_name, engine = "calamine", read_options={"header_row": header}, sheet_name = sheet_name)
  df = pd.read_excel(file_name, engine = "openpyxl", header = 1, sheet_name = "Raw data")
  df.columns = [clean_column_name(col) for col in df.columns]
  return df
dfs = []
for file_name in file_names[:10]:
  print("file_path: ", file_name)
  df = read_excel(file_name, header = 1, sheet_name="Raw data")
  dfs.append(df)
with ProcessPoolExecutor(cpu_count()) as executor:
  future_to_df = {executor.submit(read_excel, file_name, header = 1, sheet_name = 'Raw data'): file_name for file_name in file_names[:100]}
  print("Done future to df")
  for future in as_completed(future_to_df):
    file_name = future_to_df[future]
    print("future: ", file_name)
    try:
      df = future.result()
      dfs.append(df)
    except Exception as exc:
      print(f"{df} generated an exception: {exc}")


# COMMAND ----------



# COMMAND ----------

# file_path = Path("/dbfs"+remove_repeated_path).glob("*.xlsx").__next__()
# df = pl.read_excel(file_path, engine = "calamine", read_options={"header_row": 1}, sheet_name = "Raw data")
# # df.columns = [clean_column_name(col) for col in df.columns]
# df_pandas = df.to_pandas()
# df_pandas = df_pandas.dropna(axis = "columns", how = "all")
# for col in df_pandas.columns:
#   print(f'"{col}",')
# #   if df_pandas[col].dtype == "datetime64[ns]":
# #     try:
# #       df_pandas[col] = pd.to_datetime(df_pandas[col])
# #     except:
# #       df_pandas[col] = df_pandas[col].astype("str")
# # for col in df_pandas.columns:
# #   # print(f"col: {col}, type: {df_pandas[col].dtype}")
# #   if df_pandas[col].dtype== "object":
# #     print(f'StructField("{col}", StringType(), True),')
  
# #   elif df_pandas[col].dtype== "int64":
# #     print(f'StructField("{col}", StringType(), True),')
# #   elif df_pandas[col].dtype== "float64":
# #     print(f'StructField("{col}", StringType(), True),')
# #   elif df_pandas[col].dtype == "datetime64[ns]":
# #     print(f'StructField("{col}", StringType(), True),')


# COMMAND ----------

# df_sale = pl.concat(dfs, how = "vertical_relaxed")
# df_sale.head()

# COMMAND ----------

# from pyspark.sql.functions import col

# df_sale = spark.createDataFrame(df_sale.to_pandas(), schema = sale_schema)

# COMMAND ----------

# df_sale = df_sale.withColumn("billing_date", col("billing_date").cast("date"))

# COMMAND ----------

# # Write data
# (
#   df_sale.to_spark()
#   .write
#   # .partitionBy("billing_date")
#   .format("delta")
#   .mode("overwrite")
#   .option("overwriteSchema", "true")
#   .saveAsTable("mt_predicted_order.mt_sale")
# )

# COMMAND ----------

df_calendar = ps.read_excel(f"{master_path}/CALENDAR_MASTER.xlsx", dtype = "str")

df_calendar = df_calendar.dropna(axis = "columns", how = "all")
df_calendar.columns = [clean_column_name(c) for c in df_calendar.columns]
# Write data
(
  df_calendar.to_spark()
  .write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .saveAsTable("mt_predicted_order.mt_calendar")
)

# COMMAND ----------

df_price_change = ps.read_excel(f"{master_path}/PRICE_CHANGE_MASTER.xlsx", dtype = "str")

df_price_change = df_price_change.dropna(axis = "columns", how = "all")
df_price_change.columns = [clean_column_name(c) for c in df_price_change.columns]
# Write data
(
  df_price_change.to_spark()
  .write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .saveAsTable("mt_predicted_order.mt_price_change")
)

# COMMAND ----------

# from delta.tables import *

# df_table = spark.read.load("/mnt/adls/mt_predicted_order/product_master")

# display(df_table)
# deltaTableProductMaster = DeltaTable.forPath(spark, '/mnt/adls/mt_predicted_order/product_master')

# deltaTableProductMaster.alias("pm_hdfs")\
#   .merge(
#     df_table.alias("pm_table")
    
#   )


# COMMAND ----------

import pyspark.pandas as ps
df_sale = ps.read_parquet("/mnt/adls/elt/mt_predicted_order/sale")

# COMMAND ----------

df_sale.columns

# COMMAND ----------

import os
from datetime import datetime, timedelta

def duration_day_file(file_path):
  modify_time = os.path.getmtime(file_path)
  modify_date = datetime.fromtimestamp(modify_time)
  return (datetime.today() - modify_date).days
file_path = "/dbfs/mnt/adls/MT_POST_MODEL/REMOVE REPEATED/2021.19_SL_VIN PLUS.xlsx"
print(duration_day_file(file_path))

# COMMAND ----------

import pandas as pd
pd.to_datetime(datetime.today()).day_of_week 

# COMMAND ----------


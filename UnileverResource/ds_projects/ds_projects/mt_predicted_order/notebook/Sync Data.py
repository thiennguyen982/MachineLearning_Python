# Databricks notebook source
import numpy as np 
import pandas as pd
import subprocess

# COMMAND ----------

!azcopy copy ./BIG_C_202331.xlsx https://dbstorageda16d902308adls.blob.core.windows.net/unilever/YAFSU/BIG_C-202331/INPUT/EXCEL_XLSX/BIG_C_202331.xlsx

# COMMAND ----------

for idx in range(31, 45):
    idx = str(idx)
    cmd = f"azcopy copy C:\\Users\\Dao-Minh.Toan\\Downloads\\LINHDATA\\BIG_C-2023{idx}.xlsx https://dbstorageda16d902308adls.blob.core.windows.net/unilever/YAFSU/BIG_C-2023{idx}/INPUT/EXCEL_XLSX/BIG_C-2023{idx}.xlsx"
    print(cmd)
    subprocess.run(cmd.split(" "))
    

# COMMAND ----------

for MT in ["BIG_C", "SGC"]:
    for idx in range(31, 45):
        idx = str(idx)
        cmd = f"azcopy copy C:\\Users\\Dao-Minh.Toan\\Downloads\\LINHDATA\\{MT}-2023{idx} https://dbstorageda16d902308adls.blob.core.windows.net/unilever/YAFSU --recursive=true"
        print(cmd)
        subprocess.run(cmd.split(" "))

# COMMAND ----------

for MT in ["BIG_C", "SGC"]:
    for idx in range(31, 45):
        idx = str(idx)
        cmd = f"azcopy copy https://dbstorageda16d902308adls.blob.core.windows.net/unilever/YAFSU/{MT}-2023{idx} C:\\Users\\Dao-Minh.Toan\\Downloads\\OUTPUT --recursive=true"
        print(cmd)
        subprocess.run(cmd.split(" "))

# COMMAND ----------

import pickle as pkl

with open("C:\\Users\Dao-Minh.Toan\\Downloads\\FC_EST_DEMAND.pkl", "rb") as f:
    data = pkl.load(f)

# COMMAND ----------

data

# COMMAND ----------

!pip3 install rich

# COMMAND ----------

!pip3 install databricks-sdk

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------

from pathlib import Path
import shutil

root_path = "/mnt/adls"
promotion_lib_path = f"{root_path}/MT_POST_MODEL/PROMOTION_LIB/"
remove_repeated_path = f"{root_path}/MT_POST_MODEL/REMOVE REPEATED/"

file_names = [str(file_name) for file_name in Path("/dbfs"+remove_repeated_path).glob("*.xlsx")]
new_folder = Path("/dbfs/mnt/adls/test/MT_POST_MODEL/REMOVE REPEATED/")
new_folder.mkdir(parents = True, exist_ok = True)
for file_name in file_names[:2]:
  name = file_name.split("/")[-1]
  print(name)
  shutil.copy(file_name, new_folder.joinpath(name))
  

# COMMAND ----------

for file_name in new_folder.glob("*.xlsx"):
  print(file_name)

# COMMAND ----------

path_data = "/mnt/adls/MT_POST_MODEL/"

# COMMAND ----------

import pandas as pd
df = pd.read_excel("/dbfs/mnt/adls/MT_POST_MODEL/MASTER/Machine learning Factors.xlsx", header = [0, 1, 2, 3, 4, 5, 6], sheet_name = 0)

# COMMAND ----------

df.head()

# COMMAND ----------

import pandas as pd
root_path = "/mnt/adls"
promotion_lib_path = f"{root_path}/MT_POST_MODEL/PROMOTION_LIB/"

df_header = pd.read_excel(
    f"/dbfs/{promotion_lib_path}/2023 Promotion (for upload).xlsx", header=[0, 1, 2, 3, 4, 5], nrows = 1000)
df_header.columns = df_header.columns.get_level_values(1)

# COMMAND ----------

usecols = []
for idx, col in enumerate(df_header.columns):
    if pd.notnull(col) and df_header.iloc[:, idx].nunique() > 1:
      print(col)
      usecols.append([idx, col])

# COMMAND ----------

df_header.head()

# COMMAND ----------

len(usecols)

# COMMAND ----------

from pathlib import Path
Path(f"/dbfs{promotion_lib_path}").glob("*.xlsx").__next__()

# COMMAND ----------

import pandas as pd

root_path = "/mnt/adls"
promotion_lib_path = f"{root_path}/MT_POST_MODEL/PROMOTION_LIB/"

df_header = pd.read_excel(
    f"/dbfs/{promotion_lib_path}/2023 Promotion (for upload).xlsx", header=[0, 1, 2, 3, 4, 5, 6])
df_header.columns = df_header.columns.get_level_values(1)
usecols = []
for idx, col in enumerate(df_header.columns):
    if pd.notnull(col) and df_header.iloc[:, idx].nunique() > 1 and (df_header.iloc[:, idx].isnull().sum()/df_header.shape[0])<0.9:
        usecols.append([idx, col])

# COMMAND ----------

import pyspark.pandas as ps

df_promotion = ps.read_excel(promotion_lib_path,
                             header=None,
                             skiprows=7,
                             usecols=[col[0] for col in usecols],
                             dtype='str',
                             engine='openpyxl',
                             sheet_name=0)
                             


# COMMAND ----------



# COMMAND ----------

import os
import polars as pl
import pandas as pd
import pyspark.pandas as ps
from typing import List, Union
from pathlib import Path
from mt_predicted_order.de.etl.utils import clean_column_name
from mt_predicted_order.common.logger import logger
from datetime import datetime, timedelta

spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

root_path = "/mnt/adls"
promotion_lib_path = f"{root_path}/MT_POST_MODEL/PROMOTION_LIB/"
remove_repeated_path = f"{root_path}/MT_POST_MODEL/REMOVE REPEATED/"

file_path_sale: Path = Path("/dbfs/mnt/adls/elt/mt_predicted_order/sale")

file_paths: List[str] = [str(file_path) for file_path in Path(
    "/dbfs"+remove_repeated_path).glob("*.xlsx")]


def duration_day_file(file_path):
    modify_time = os.path.getmtime(file_path)
    modify_date = datetime.fromtimestamp(modify_time)
    return (datetime.today() - modify_date).days


def read_excel(file_path, header: int, sheet_name: str) -> Union[pl.DataFrame, pd.DataFrame]:
    try:
        df: pl.DataFrame = pl.read_excel(file_path, engine="calamine", read_options={
                                         "header_row": 1}, sheet_name="Raw data")
        for col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Utf8))
        return df
    except Exception as e:
        logger.error(f"Error when read file: {file_path} by polars: {e}")
        try:
            df: pd.DataFrame = pd.read_excel(file_path,
                                             header=header, sheet_name=sheet_name)
            for col in df.columns:
                df[col] = df[col].astype("str")
            return df
        except Exception as e:
            logger.error(f"Error when read file: {file_path} by pandas: {e}")

    return None


def read_parquet(file_path, engine="polars") -> Union[pl.DataFrame, pd.DataFrame]:
    if engine == "polars":
        return pl.read_parquet(file_path)
    else:
        return pd.read_parquet(file_path)


for file_path in file_paths:
    file_name = file_path.split("/")[-1].split(".")[-2]
    file_path_sale_parquet = file_path_sale.joinpath(f"{file_name}.parquet")
    duration_of_file = duration_day_file(file_path)
    if pd.to_datetime(datetime.today()).day_of_week >= 0 or duration_of_file < 30:
        df_raw = read_excel(file_path=file_path, header=1,
                            sheet_name="Raw data")
        if file_path_sale_parquet.exists():
            df_parquet = read_parquet(file_path_sale_parquet, "polars" if isinstance(
                df_raw, pl.DataFrame) else "pandas")
            if df_parquet.shape[0] == df_raw.shape[0]:
                df_raw = None
            else:
                logger.info(f"NEED TO UPDATE FILE: {file_path}")
        else:
            logger.info(f"pass file_path: {file_path}")

        if df_raw is not None:
            logger.info(f"write to: {file_path_sale_parquet}")
            if isinstance(df_raw, pl.DataFrame):
                df_raw.write_parquet(file_path_sale_parquet)
            elif isinstance(df_raw, pd.DataFrame):
                df_raw.to_parquet(file_path_sale_parquet)
            else:
                logger.info(
                    f"file_path: {file_path} is not in pl.DataFrame and pd.DataFrame")
                pass
    else:
        logger.info(f"DONT NEED TO UPDATE BECAUSE DURATION OF FILE")

df_sale = ps.read_parquet("/mnt/adls/elt/mt_predicted_order/sale")
df_sale.columns = [clean_column_name(col) for col in df_sale.columns]
df_sale["billing_date"] = ps.to_datetime(df_sale["billing_date"])

(
    df_sale.to_spark()
    .write
    .partitionBy("billing_date")
    .format("delta")
    .mode("overwrite")
    .saveAsTable("mt_predicted_order.mt_sale")

)

# COMMAND ----------

import pandas as pd
import polars as pl

def read_excel(file_path, header: int, sheet_name: str):
    try:
        df: pl.DataFrame = pl.read_excel(file_path, engine="calamine", read_options={
                                         "header_row": 1}, sheet_name="Raw data")
        for col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Utf8))
        return df
    except Exception as e:
        logger.error(f"Error when read file: {file_path} by polars: {e}")
        try:
            df: pd.DataFrame = pd.read_excel(file_path,
                                             header=header, sheet_name=sheet_name)
            for col in df.columns:
                df[col] = df[col].astype("str")
            return df
        except Exception as e:
            logger.error(f"Error when read file: {file_path} by pandas: {e}")

    return None
df = read_excel("/dbfs/mnt/adls/MT_POST_MODEL/REMOVE REPEATED/2023.47_SL_BACH HOA XANH.xlsx",header = 1, sheet_name = 0)

# COMMAND ----------

import pyspark.pandas as ps
import zipfile
def is_valid_zip(file_path):
    try:
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            # Check if the zip file is readable
            zip_ref.testzip()
        return True
    except zipfile.BadZipFile:
        return False

is_valid_zip("/dbfs/mnt/adls/MT_POST_MODEL/REMOVE REPEATED/2023.47_SL_BACH HOA XANH.xlsx")
# data = ps.read_excel("/mnt/adls/MT_POST_MODEL/REMOVE REPEATED/2023.47_SL_BACH HOA XANH.xlsx", engine = "openpyxl")

# COMMAND ----------

# Set Spark configuration to dynamically overwrite partitions
spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

# Define paths and exclusion criteria
base_path = Path("/dbfs/mnt/adls/MT_POST_MODEL/REMOVE REPEATED/")
chunk_size = 100

file_paths = []
for file_path in base_path.glob("*.xlsx"):
  file_path = str(file_path).replace('/dbfs/', '/')
  file_paths.append(file_path)
dfs = []
for i in range(0, len(file_paths), chunk_size):
  print(f"Process chunk from {i} -> {i+chunk_size}")
  try:
    df_spark = (spark.read.format("excel").option("dataAddress", "'Raw data'!A2")
                                          .option("header", "true")
                                          .option("maxRowsInMemory", 10000)
                                          .option("maxByteArraySize", 2147483647)
                                          .option("tempFileThreshold", 10000000)
                                          .load(file_paths[i:i+chunk_size])
                                        )
    dfs.append(df_spark)
  except Exception as e:
    print(f"Error read chunk: {i}->{i+chunk_size}, e: {e}")
    for idx in range(i, i+chunk_size):
      file_path = file_paths[idx]
      try:
        df_spark = (spark.read.format("excel").option("dataAddress", "'Raw data'!A2")
                                              .option("header", "true")
                                              .option("maxRowsInMemory", 10000)
                                              .option("maxByteArraySize", 2147483647)
                                              .option("tempFileThreshold", 10000000)
                                              .load(file_path)
                                              )

        dfs.append(df_spark)
      except Exception as e:
        print(f"Error read file: {file_path} and skip it")
df_sale = functools.reduce(lambda first, second: first.union(second), dfs)
df_sale = df_sale.pandas_api()

# COMMAND ----------

from openpyxl import load_workbook
def validate_excel_file(file_path):
    try:
        # Attempt to open the Excel file
        wb = load_workbook(filename=file_path)
        return True
    except Exception as e:
        # If an error occurs, the file is considered invalid
        print(f"Invalid file: {file_path}, error: {e}")
        return False
      
# validate_excel_file("/dbfs/mnt/adls/MT_POST_MODEL/REMOVE REPEATED/2023.47_SL_BACH HOA XANH.xlsx")
for file_path in file_paths[3700:3800]:
    if validate_excel_file(f"/dbfs{}") == False:
        print(file_path)


# COMMAND ----------

validate_excel_file("/dbfs/mnt/adls/MT_POST_MODEL/REMOVE REPEATED/Validation Order_WINMART.xlsx")

# COMMAND ----------

len(dfs)

# COMMAND ----------

import functools 
import re
import pyspark.pandas as ps

def clean_column_name(column_name):
    # Replace any sequence of characters that is not alphanumeric or underscore with an underscore
    cleaned_name = re.sub(r'[^a-zA-Z0-9_]', '_', column_name)
    cleaned_name = re.sub(r'_{2,}', '_', cleaned_name)
    # Optionally, you might want to remove leading or trailing underscores
    cleaned_name = re.sub(r'^_+|_+$', '', cleaned_name)
    # Convert to lowercase
    cleaned_name = cleaned_name.lower()
    return cleaned_name

df_sale = functools.reduce(lambda first, second: first.unionByName(second), dfs)
df_sale = df_sale.pandas_api()
df_sale.columns = [clean_column_name(col) for col in df_sale.columns]
df_sale["billing_date"] = ps.to_datetime(df_sale["billing_date"])

(
    df_sale.to_spark()
    .write
    .partitionBy("billing_date")
    .format("delta")
    .mode("overwrite")
    .saveAsTable("mt_predicted_order.mt_sale")

)

# COMMAND ----------

display(final_df)

# COMMAND ----------

import re
import pandas as pd
import os
from pathlib import Path
from pyspark.sql import SparkSession
import pyspark.pandas as ps
# from mt_predicted_order.de.etl.utils import clean_column_name

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("ETL_MT_PROMOTION") \
    .getOrCreate()

# list_files = Path("./").glob('**/*')
# for file_name in list_files:
#     print(str(file_name))
def clean_column_name(column_name):
    # Replace any sequence of characters that is not alphanumeric or underscore with an underscore
    cleaned_name = re.sub(r'[^a-zA-Z0-9_]', '_', column_name)
    cleaned_name = re.sub(r'_{2,}', '_', cleaned_name)
    # Optionally, you might want to remove leading or trailing underscores
    cleaned_name = re.sub(r'^_+|_+$', '', cleaned_name)
    # Convert to lowercase
    cleaned_name = cleaned_name.lower()
    return cleaned_name

root_path = "/mnt/adls"
promotion_lib_path = f"{root_path}/MT_POST_MODEL/PROMOTION_LIB/"

# df_header = pd.read_excel(
#     f"/dbfs/{promotion_lib_path}/2023 Promotion (for upload).xlsx", header=[0, 1, 2, 3, 4, 5, 6])
# df_header.columns = df_header.columns.get_level_values(1)
# usecols = []
# for idx, col in enumerate(df_header.columns):
#     if pd.notnull(col) and df_header.iloc[:, idx].nunique() > 1 and df_header.iloc[:, idx].isnull().sum() == 0:
#         usecols.append([idx, col])

# df_promotion = ps.read_excel(promotion_lib_path,
#                              header=None,
#                              skiprows=7,
#                              usecols=[col[0] for col in usecols],
#                              dtype='str',
#                              engine='openpyxl',
#                              sheet_name=0)

dataAddress = "'Promotion Library'!A1"
df_promotion_header = (spark.read.format("excel").option("dataAddress", dataAddress)
                                            .option("header", "false")
                                            .option("maxRowsInMemory", 10000)
                                            .option("maxByteArraySize", 2147483647)
                                            .option("tempFileThreshold", 10000000)
                                            .load(promotion_lib_path).limit(7)
                                          )
header_row = df_promotion_header.collect()[1]
new_column_names = [col_name for col_name in header_row.asDict().values()]


dataAddress = "'Promotion Library'!A7"
df_promotion = (spark.read.format("excel").option("dataAddress", dataAddress)
                                            .option("header", "false")
                                            .option("maxRowsInMemory", 10000)
                                            .option("maxByteArraySize", 2147483647)
                                            .option("tempFileThreshold", 10000000)
                                            .load(promotion_lib_path)
                                          )

for i, new_col_name in enumerate(new_column_names):
  if new_col_name is not None:
    df_promotion = df_promotion.withColumnRenamed(f"_c{i}", clean_column_name(new_col_name))

# Write data
(
    df_promotion
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("mt_predicted_order.mt_promotion")
)
display(df_promotion)

# COMMAND ----------

header_row = df_promotion_header.collect()[1]

# COMMAND ----------

display(df_promotion_header)

# COMMAND ----------

spark: SparkSession = SparkSession.builder \
    .appName("ETL_MT_PRICE_CHANGE") \
    .getOrCreate()
root_path = "/mnt/adls"
master_path = f"{root_path}/MT_POST_MODEL/MASTER"
dataAddress = "'PRICE_CHANGE_MASTER'!A1"
df_price_change = (spark.read.format("excel").option("dataAddress", dataAddress)
                                            .option("header", "true")
                                            .option("maxRowsInMemory", 10000)
                                            .option("maxByteArraySize", 2147483647)
                                            .option("tempFileThreshold", 10000000)
                                            .load([f"{master_path}/PRICE_CHANGE_MASTER.xlsx"])
                                          )
display(df_price_change)


# COMMAND ----------

new_column_names = [col_name for col_name in header_row.asDict().values()]
new_column_names

# COMMAND ----------

for i, new_col_name in enumerate(new_column_names):
  if new_col_name is not None:
    df_promotion = df_promotion.withColumnRenamed(f"_c{i}", new_col_name)

# COMMAND ----------


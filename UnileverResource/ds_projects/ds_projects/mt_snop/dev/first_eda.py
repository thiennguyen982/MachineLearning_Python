# Databricks notebook source
# MAGIC %pip install openpyxl ray

# COMMAND ----------

# MAGIC %run ./Modules/Utils

# COMMAND ----------

# Runtime-related libraries
import os
import logging
import warnings

from tqdm.auto import tqdm

# General libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from pandas.tseries.offsets import DateOffset

# Parallel-related libraries
import ray
import pyspark.sql.types as types
import pyspark.sql.functions as F

from pyspark.sql.window import Window

# COMMAND ----------

# First-step configurations

# Disable warnings
logging.getLogger("prophet").setLevel(logging.ERROR)
logging.getLogger("cmdstanpy").disabled = True
logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)
logging.getLogger("py4j.clientserver").setLevel(logging.ERROR)
warnings.filterwarnings("ignore")

# Config matplotlib's graphs
plt.rcParams['figure.figsize'] = [40, 20]
plt.rcParams['font.size'] = 20

%matplotlib inline

# Config Pandas's float to .xx
pd.options.display.float_format = "{:,.2f}".format

# Turn on Ray
if not ray.is_initialized():
  ray.init()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Paths to data

# COMMAND ----------

# MAGIC %fs ls /mnt/adls/MT_POST_MODEL/

# COMMAND ----------

print_structure('/dbfs/mnt/adls/MT_POST_MODEL/DATA_PROMOTION/')

# COMMAND ----------

print_structure('/dbfs/mnt/adls/MT_POST_MODEL/DATA_RAW_CONVERT/')

# COMMAND ----------

print_structure('/dbfs/mnt/adls/MT_POST_MODEL/DEMAND_POST_MODEL/')

# COMMAND ----------

print_structure('/dbfs/mnt/adls/MT_POST_MODEL/MASTER/')

# COMMAND ----------

print_structure('/dbfs/mnt/adls/MT_POST_MODEL/PROMOTION_LIB/')

# COMMAND ----------

print_structure('/dbfs/mnt/adls/MT_POST_MODEL/SHIP_TO/')

# COMMAND ----------


# Remove repeated (SAlE) path 
PATH_RAW_SALE = f"/dbfs/mnt/adls/MT_POST_MODEL/REMOVE REPEATED/"    

# PROMOTION INPUT FROM BIZ (FC PROMOTION) path 
PATH_PROMOTION = f"/dbfs/mnt/adls/MT_POST_MODEL/DATA_PROMOTION/UPDATE"   

# HISTORY PROMOTION - STORE IN ADLS
PATH_PROMOTION_HISTORY = f"/dbfs/mnt/adls/MT_POST_MODEL/DATA_PROMOTION/HISTORY/"

######## MASTER DATA PATH ######

PATH_RAW_CALENDAR = (
    f"/dbfs/mnt/adls/MT_POST_MODEL/MASTER/CALENDAR_MASTER.xlsx"
)
PATH_PRICE_CHANGE = (
    f"/dbfs/mnt/adls/MT_POST_MODEL/MASTER/PRICE_CHANGE_MASTER.xlsx"
)
FACTOR = f"/dbfs/mnt/adls/MT_POST_MODEL/MASTER/Machine learning Factors.xlsx"

    
PATH_PRODUCT_MASTER = f"/dbfs/mnt/adls/MT_POST_MODEL/MASTER/PRODUCT MASTER.csv"

# COMMAND ----------

def read_file(file_path, header):
  return pd.read_excel(file_path, header = header)

@ray.remote
def REMOTE_read_file(file_path, header):
  return read_file(file_path, header)

def read_folder(folder_path, header):
  file_list = [folder_path + file_name for file_name in os.listdir(folder_path)]
  return pd.concat(ray.get([REMOTE_read_file.remote(file_path, header) for file_path in tqdm(file_list)]))

# COMMAND ----------

df_sales = read_folder('/dbfs/mnt/adls/MT_POST_MODEL/REMOVE REPEATED/', 1)

# COMMAND ----------

df_sales

# COMMAND ----------

spark.createDataFrame(df_sales).write.parquet('/mnt/adls/MT_FC/DATA/231115_SALES_DATA.parquet')

# COMMAND ----------


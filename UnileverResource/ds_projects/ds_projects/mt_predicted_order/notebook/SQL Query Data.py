# Databricks notebook source
# MAGIC %sql
# MAGIC with sale as (
# MAGIC select 
# MAGIC billing_date as ds,
# MAGIC banner,
# MAGIC brand,
# MAGIC region,
# MAGIC
# MAGIC dp_name,
# MAGIC est__demand__cs_ as est_demand,
# MAGIC category,
# MAGIC U_Price__cs_ as u_price,
# MAGIC
# MAGIC
# MAGIC -- Promotion Features
# MAGIC promotion_type
# MAGIC
# MAGIC
# MAGIC from mt_predicted_order.mt_sale
# MAGIC )
# MAGIC select *
# MAGIC from sale

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Sale analysis
# MAGIC with sale as (
# MAGIC select 
# MAGIC to_date(billing_date) as ds,
# MAGIC banner,
# MAGIC brand,
# MAGIC region,
# MAGIC
# MAGIC dp_name,
# MAGIC est__demand__cs_ as est_demand,
# MAGIC category,
# MAGIC U_Price__cs_ as u_price,
# MAGIC -- Promotion Features
# MAGIC promotion_type
# MAGIC from mt_predicted_order.mt_sale
# MAGIC )
# MAGIC
# MAGIC , sale_final as (
# MAGIC select *
# MAGIC from sale
# MAGIC )
# MAGIC
# MAGIC -- Promotion analysis
# MAGIC , promotion as (
# MAGIC select 
# MAGIC banner,
# MAGIC dp_name,
# MAGIC
# MAGIC on_off_post,
# MAGIC order_start_date,
# MAGIC order_end_date
# MAGIC
# MAGIC
# MAGIC from mt_predicted_order.mt_promotion
# MAGIC )
# MAGIC , promotion_explode as (
# MAGIC select 
# MAGIC explode(sequence(to_date(order_start_date), to_date(order_end_date), interval 1 day)) AS ds,
# MAGIC banner,
# MAGIC dp_name,
# MAGIC
# MAGIC on_off_post,
# MAGIC order_start_date,
# MAGIC order_end_date
# MAGIC from promotion
# MAGIC )
# MAGIC , promotion_final as (
# MAGIC select *
# MAGIC from promotion_explode
# MAGIC )
# MAGIC
# MAGIC select *
# MAGIC from sale_final
# MAGIC -- left join promotion_final using(ds, banner, dp_name)
# MAGIC where 1=1
# MAGIC -- and banner = "SAIGON COOP"
# MAGIC
# MAGIC

# COMMAND ----------

root_path = "/mnt/adls"
promotion_lib_path = f"{root_path}/MT_POST_MODEL/PROMOTION_LIB/"
remove_repeated_path = f"{root_path}/MT_POST_MODEL/'REMOVE REPEATED'/"
master_path = f"{root_path}/MT_POST_MODEL/MASTER"

df = (spark.read
      .format("excel")
      .option("dataAddress", "0!A6")
      .option("inferSchema", "true")
      .option("maxRowsInMemory", 20) 
      .option("maxByteArraySize", 2147483647)
      .option("tempFileThreshold", 10000000) 
      .load(promotion_lib_path)
)

# COMMAND ----------

root_path = "/mnt/adls/test"
remove_repeated_path = f"{root_path}/MT_POST_MODEL/REMOVE REPEATED/"
df_sale = (spark.read
          .format("excel")
          .option("header", "true")
          .option("dataAddress", "'Raw data'!A2")
          .option("maxRowsInMemory", 20) 
          .option("maxByteArraySize", 2147483647)
          .option("tempFileThreshold", 10000000)
          .load(remove_repeated_path)
           
)


# COMMAND ----------

import re
def clean_column_name(column_name: str):
  # strip text
  column_name = column_name.strip()

  # remove special symbols from string
  # column_name = re.sub("[^A-Za-z0-9\space]+", '', column_name)
  # Replace or remove invalid characters as needed
  # This regex replaces any character not a letter, number, or underscore with an underscore
  column_name = re.sub(r'[^\w]', '_', column_name)
  return column_name


# COMMAND ----------

# df_sale = df_sale.pandas_api()
for col in df_sale.columns:
  df_sale = df_sale.withColumnRenamed(col, clean_column_name(col))


# COMMAND ----------

# MAGIC %fs ls /mnt/adls/MT_POST_MODEL/PROMOTION_LIB/

# COMMAND ----------

# df_sale.pandas_api().head()

# COMMAND ----------

# Write data
(
  df_sale
  .write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .saveAsTable("mt_predicted_order.mt_sale")
)

# COMMAND ----------


from pathlib import Path
import pandas as pd
import pyspark.pandas as ps
file_name = str(Path("/dbfs"+remove_repeated_path).glob("*.xlsx").__next__())

df_header = pd.read_excel(file_name, header = 1)
usecols = []
for idx, col in enumerate(df_header.columns):
  if pd.notnull(col) and df_header.iloc[:, idx].nunique() > 1 and df_header.iloc[:, idx].isnull().sum() == 0:
    usecols.append([idx, col])
print("usecols: ", len(usecols))

df_sale = ps.read_excel(remove_repeated_path, 
                        header = 1, 
                        usecols= [col[1] for col in usecols][2:5], 
                        sheet_name = 0, 
                        dtype = "str",
                        engine='openpyxl',
                        nrows = 1000
                        )
# df_sale = df_sale.dropna(axis = "columns", how = "all")
# df_sale.columns = [clean_column_name(c) for c in df_sale.columns]
df_sale.head()

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/dao-minh.toan@unilever.com/ds_projects")

from mt_predicted_order.common.mlflow.mlflow_service import MlflowService


# COMMAND ----------


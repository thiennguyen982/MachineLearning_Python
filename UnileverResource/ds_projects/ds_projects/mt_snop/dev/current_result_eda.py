# Databricks notebook source
# MAGIC %pip install openpyxl ray

# COMMAND ----------

import pandas as pd
import numpy as np

import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

data_original = spark.createDataFrame(pd.read_excel('/dbfs/mnt/adls/MT_FC/DATA/FC Lag 4 - DP Cube.xlsx'))

# COMMAND ----------

data.display()

# COMMAND ----------

PRODUCT_HIERARCHY_PATH = '/mnt/adls/BHX_FC/FBFC/DATA/PRODUCT_MASTER.csv'
product_master_original = spark.read.format("csv").options(delimiter = ",", header = "True").load(PRODUCT_HIERARCHY_PATH)

# COMMAND ----------

product_master_data = (product_master_original.select('Material', 'CD DP Name')
                                              .withColumnRenamed('CD DP Name', 'DP_NAME')
                                              .dropDuplicates())

# COMMAND ----------

data = data_original.withColumnRenamed('Sku Code', 'skuid')
data = data.join(product_master_data, data.skuid == product_master_data.Material, 'left')

# COMMAND ----------

data = (data.select('DP_NAME', 'Category Name', 'Year Week', 'LagW4 Roll1 Total Header Forecast Quantity', 'Actual Delivered QuantityRoll1')
            .withColumnRenamed('Category Name', 'CATEGORY')
            .withColumnRenamed('Year Week', 'YEARWEEK')
            .withColumnRenamed('LagW4 Roll1 Total Header Forecast Quantity', 'PREDICTED_SALES')
            .withColumnRenamed('Actual Delivered QuantityRoll1', 'ACTUAL_SALES'))

# COMMAND ----------

def fa(y_true, y_pred):
  sum_err = sum([abs(pred - true) for true, pred in zip(y_true, y_pred)])
  sum_act = sum(y_true)

  return 1 - (sum_err / sum_act)

def fb(y_true, y_pred):
  sum_err = sum([pred - true for true, pred in zip(y_true, y_pred)])
  sum_act = sum(y_true)

  return sum_err / sum_act

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # FA/FB ALL DATA

# COMMAND ----------

df_predict = data.toPandas()

df_predict = df_predict[df_predict['ACTUAL_SALES'] != 'W']
df_predict[['PREDICTED_SALES', 'ACTUAL_SALES']] = df_predict[['PREDICTED_SALES', 'ACTUAL_SALES']].astype('int')

# COMMAND ----------

print(f"CURRENT SCORE:")
print(f"- FA: {fa(df_predict['ACTUAL_SALES'].values.tolist(), df_predict['PREDICTED_SALES'].values.tolist())}")
print(f"- FB: {fb(df_predict['ACTUAL_SALES'].values.tolist(), df_predict['PREDICTED_SALES'].values.tolist())}")

# COMMAND ----------

df_tmp = df_predict.copy()
df_tmp['FA_ERR'] = abs(df_tmp['PREDICTED_SALES'] - df_tmp['ACTUAL_SALES'])
df_tmp['FB_ERR'] = df_tmp['PREDICTED_SALES'] - df_tmp['ACTUAL_SALES']

df_fa = df_tmp.groupby('CATEGORY').agg({'FA_ERR': 'sum', 'ACTUAL_SALES': 'sum'})
df_fa['FA'] = 1 - (df_fa['FA_ERR'] / df_fa['ACTUAL_SALES'])
df_fa

# COMMAND ----------

df_fb = df_tmp.copy()
df_fb['NEGATIVE_BIAS'] = np.where(df_fb['FB_ERR'] < 0, df_fb['FB_ERR'], 0)
df_fb['POSITIVE_BIAS'] = np.where(df_fb['FB_ERR'] >= 0, df_fb['FB_ERR'], 0)

df_fb = df_fb.groupby('CATEGORY').agg({'FB_ERR': 'sum', 'ACTUAL_SALES': 'sum', 'NEGATIVE_BIAS': 'sum', 'POSITIVE_BIAS': 'sum'})
df_fb['FB'] = df_fb['FB_ERR'] / df_fb['ACTUAL_SALES']

df_fb

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # FA/FB First 8 Weeks of 2023

# COMMAND ----------

df_predict = data.toPandas()

df_predict = df_predict[(df_predict['ACTUAL_SALES'] != 'W') & (df_predict['YEARWEEK'].between(202301, 202308))]
df_predict[['PREDICTED_SALES', 'ACTUAL_SALES']] = df_predict[['PREDICTED_SALES', 'ACTUAL_SALES']].astype('int')

# COMMAND ----------

print(f"CURRENT SCORE:")
print(f"- FA: {fa(df_predict['ACTUAL_SALES'].values.tolist(), df_predict['PREDICTED_SALES'].values.tolist())}")
print(f"- FB: {fb(df_predict['ACTUAL_SALES'].values.tolist(), df_predict['PREDICTED_SALES'].values.tolist())}")

# COMMAND ----------

df_tmp = df_predict.copy()
df_tmp['FA_ERR'] = abs(df_tmp['PREDICTED_SALES'] - df_tmp['ACTUAL_SALES'])
df_tmp['FB_ERR'] = df_tmp['PREDICTED_SALES'] - df_tmp['ACTUAL_SALES']

df_fa = df_tmp.groupby('CATEGORY').agg({'FA_ERR': 'sum', 'ACTUAL_SALES': 'sum'})
df_fa['FA'] = 1 - (df_fa['FA_ERR'] / df_fa['ACTUAL_SALES'])
df_fa

# COMMAND ----------

df_fb = df_tmp.copy()
df_fb['NEGATIVE_BIAS'] = np.where(df_fb['FB_ERR'] < 0, df_fb['FB_ERR'], 0)
df_fb['POSITIVE_BIAS'] = np.where(df_fb['FB_ERR'] >= 0, df_fb['FB_ERR'], 0)

df_fb = df_fb.groupby('CATEGORY').agg({'FB_ERR': 'sum', 'ACTUAL_SALES': 'sum', 'NEGATIVE_BIAS': 'sum', 'POSITIVE_BIAS': 'sum'})
df_fb['FB'] = df_fb['FB_ERR'] / df_fb['ACTUAL_SALES']

df_fb

# COMMAND ----------


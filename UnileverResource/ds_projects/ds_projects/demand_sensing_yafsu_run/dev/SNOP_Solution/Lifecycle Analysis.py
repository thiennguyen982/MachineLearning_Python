# Databricks notebook source
import pandas as pd 
import numpy as np
from scipy.stats import linregress

# COMMAND ----------

DF = pd.read_parquet("/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/TEMP_DATA/BASELINE.parquet")

# COMMAND ----------

DF = DF[['KEY', 'YEARWEEK', 'ACTUALSALE', 'BASELINE', 'DTWORKINGDAY']]
DF['DAILY'] = DF['BASELINE'] / DF['DTWORKINGDAY']
DF['DAILY'] = np.where(DF['DTWORKINGDAY'] == 0, 0, DF['DAILY'])

# COMMAND ----------

DF

# COMMAND ----------

from tqdm import tqdm 

df_result = []
for key, df_group in tqdm(DF.groupby('KEY')):
  df_group = df_group.sort_values('YEARWEEK')
  df_group['YEARWEEK ORDER'] = df_group['YEARWEEK'].transform('rank').astype(int)
  
  if len(df_group) > 52:

    chunk_size = int(df_group.shape[0] / 26)
    df_group_split_arr = np.array_split(df_group, chunk_size)

    for idx, df_small_group in enumerate(df_group_split_arr):
      if len(df_small_group) > 13:
        slope, intercept, r_value, p_value, std_err = linregress(
          x=df_small_group['YEARWEEK ORDER'].values, 
          y=df_small_group['DAILY'].values)
        result_object = {
          "KEY": key,
          "GROUP": idx, 
          "SLOPE": slope, 
          "INTERCEPT": intercept
        }
        df_result.append(result_object)

df_result = pd.DataFrame(df_result)

# COMMAND ----------

df_result.to_csv("/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/LIFECYCLE_SLOPE.csv", index=False)
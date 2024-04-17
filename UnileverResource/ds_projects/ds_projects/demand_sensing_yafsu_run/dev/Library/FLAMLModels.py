# Databricks notebook source
# %run "../EnvironmentSetup"

# COMMAND ----------

# MAGIC %pip install -U scikit-learn "flaml[automl,ts_forecast]" pandas==1.5.3 

# COMMAND ----------

import logging

logging.getLogger("prophet").setLevel(logging.ERROR)
logging.getLogger("cmdstanpy").disabled = True
logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)
logging.getLogger("py4j.clientserver").setLevel(logging.ERROR)

import pandas as pd
import numpy as np
from scipy.stats import linregress
import warnings

warnings.filterwarnings("ignore")

from flaml import AutoML

import ray
runtime_env = {"pip": ["flaml[automl,ts_forecast]", "pandas==1.5.3", "scikit-learn==1.3.0"]}
ray.init(ignore_reinit_error=True, runtime_env=runtime_env)

# COMMAND ----------

# ray.shutdown()

# COMMAND ----------

DF = pd.read_parquet(
    "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/TEMP_DATA/BASELINE.parquet"
)
# Must comment in production code
# DF = DF.query("CATEGORY == 'FABSOL' ")

DF = DF[["KEY", "YEARWEEK", "ACTUALSALE", "BASELINE"]]
DF["FUTURE"] = "N"

DF_FUTURE = pd.DataFrame({"KEY": DF["KEY"].unique()})
DF_FUTURE["CHECK"] = 0
DF_YEARWEEK = pd.DataFrame({"YEARWEEK": range(DF["YEARWEEK"].max() + 1, 202353)})
DF_YEARWEEK["CHECK"] = 0
DF_FUTURE = DF_FUTURE.merge(DF_YEARWEEK, on="CHECK", how="outer")
DF_FUTURE["FUTURE"] = "Y"

DF = pd.concat([DF, DF_FUTURE])
DF = DF.fillna(0)

df_calendar = pd.read_excel(
    "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/LANDING-MASTER/Master ML Calendar.xlsx",
    sheet_name="BASEWEEK-CALENDAR",
)
df_calendar = df_calendar[["YEARWEEK", "DTWORKINGDAY"]]
DF = DF.merge(df_calendar, on="YEARWEEK")
DF["WORKINGDAY"] = DF["DTWORKINGDAY"]
DF["DAILY"] = np.where(DF["WORKINGDAY"] == 0, 0, DF['BASELINE'] / DF["WORKINGDAY"])
DF[['BANNER', 'CATEGORY', 'DPNAME']] = DF['KEY'].str.split('|', expand=True)

# COMMAND ----------

# MAGIC %md
# MAGIC # DEBUG AREA

# COMMAND ----------

DF_HCME_FABSOL = DF[((DF['BANNER'] == 'HO CHI MINH - EAST') & (DF['CATEGORY'] == 'FABSOL'))]
DF_HCME_FABSOL['DATE'] = pd.to_datetime(DF_HCME_FABSOL['YEARWEEK'].astype(str) + "-2", format="%G%V-%w")
# DF_HCME_FABSOL

# COMMAND ----------

df_calendar_custom = pd.read_excel("/dbfs/FileStore/tables/CUSTOM_CALENDAR_FEATURE.xlsx")
DF_HCME_FABSOL = DF_HCME_FABSOL.merge(df_calendar_custom, on='YEARWEEK')
CALENDAR_FEATURES = list(df_calendar_custom.columns)
CALENDAR_FEATURES.remove('YEARWEEK')

# COMMAND ----------

# "DATE" in DF_HCME_FABSOL.columns

# COMMAND ----------

def FLAML_forecast(key, df_group, target_var, exo_vars, categorical_vars):
  df_group = df_group.sort_values(by='YEARWEEK')
  df_group = df_group[['DATE', 'KEY', 'FUTURE', 'WORKINGDAY', target_var, *exo_vars, *categorical_vars]]
  df_group['DAILY'] = np.where(df_group['WORKINGDAY'] == 0, 0, df_group[target_var] / df_group['WORKINGDAY'])

  df_train = df_group.query("FUTURE == 'N' ")
  df_future = df_group.query("FUTURE == 'Y' ")
  df_train = df_train.drop(columns=[target_var, 'FUTURE'])

  automl = AutoML()
  time_horizon = 26

  # configure AutoML settings
  settings = {
      "time_budget": 60,  # total running time in seconds
      "metric": "mape",  # primary metric
      "task": "ts_forecast",  # task type
      "eval_method": "holdout",
      "label": "DAILY",
      "estimator_list": ['rf', 'sarimax'],
      "n_jobs": 1, 
      "period": time_horizon
  }

  # train the model
  automl.fit(dataframe=df_train, **settings)

  predict = automl.predict(df_future)
  df_future['FLAML_PREDICT_DAILY'] = predict
  return df_future

@ray.remote 
def REMOTE_FLAML_forecast(key, df_group, target_var, exo_vars, categorical_vars):
  try:
    result = FLAML_forecast(key, df_group, target_var, exo_vars, categorical_vars)
  except:
    result = pd.DataFrame()
  return result

# COMMAND ----------

tasks = [REMOTE_FLAML_forecast.remote(key, df_group, target_var='BASELINE', exo_vars=CALENDAR_FEATURES, categorical_vars=[]) for key, df_group in DF_HCME_FABSOL.groupby('KEY')]
tasks = ray.get(tasks)
df_result = pd.concat(tasks)

# COMMAND ----------

df_result.to_parquet("/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/TEMP_DATA/FABSOL_BASELINE_FC.parquet")

# COMMAND ----------

df_result.to_csv("/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/TEMP_DATA/FABSOL_BASELINE_FC.csv")

# COMMAND ----------

display(df_result)

# COMMAND ----------

df_result = df_result.append(DF_HCME_FABSOL.query("FUTURE == 'N'"))

# COMMAND ----------

display(df_result)

# COMMAND ----------

predict_result.round(2)
skip

# COMMAND ----------


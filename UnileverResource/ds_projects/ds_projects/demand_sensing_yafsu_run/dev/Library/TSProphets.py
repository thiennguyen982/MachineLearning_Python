# Databricks notebook source
# MAGIC %pip install prophet
# MAGIC %pip install ray

# COMMAND ----------

import pandas as pd
import numpy as np
from scipy.stats import linregress
import warnings

warnings.filterwarnings("ignore")

# COMMAND ----------

import ray

ray.init()

# COMMAND ----------

from prophet import Prophet

# COMMAND ----------

DF = pd.read_parquet(
    "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/TEMP_DATA/BASELINE.parquet"
)
# Must comment in production code
# DF = DF.query("CATEGORY == 'FABSOL' ")

DF = DF[["KEY", "YEARWEEK", "ACTUALSALE", "BASELINE"]]
DF["FUTURE"] = "N"
DF["DATE"] = DF["YEARWEEK"].astype(str)
DF["DATE"] = pd.to_datetime(DF["DATE"] + "-2", format="%G%V-%w")

df_calendar = pd.read_excel(
    "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/LANDING-MASTER/Master ML Calendar.xlsx",
    sheet_name="BASEWEEK-CALENDAR",
)
df_calendar = df_calendar[["YEARWEEK", "DTWORKINGDAY"]]
DF = DF.merge(df_calendar, on="YEARWEEK")
DF["WORKINGDAY"] = DF["DTWORKINGDAY"]
DF["DAILY"] = np.where(DF["WORKINGDAY"] == 0, 0, DF["BASELINE"] / DF["WORKINGDAY"])

# COMMAND ----------

import logging

logging.getLogger("prophet").setLevel(logging.ERROR)
logging.getLogger("cmdstanpy").disabled = True
logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)
logging.getLogger("py4j.clientserver").setLevel(logging.ERROR)

# COMMAND ----------

def fbprophet_linear_growth(key, df_group):
    import logging

    logging.getLogger("prophet").setLevel(logging.ERROR)
    logging.getLogger("cmdstanpy").disabled = True
    logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)
    logging.getLogger("py4j.clientserver").setLevel(logging.ERROR)
    import warnings

    warnings.filterwarnings("ignore")

    df_group = df_group.query("YEARWEEK > 202000")

    df_group = df_group.set_index("DATE")
    df_group = df_group.asfreq("W-TUE")
    df_group = df_group.sort_index()
    df_group["YEARWEEK"] = (
        df_group.index.isocalendar().year * 100 + df_group.index.isocalendar().week
    )
    df_group = df_group.fillna(0)
    df_group["DAILY"] = np.where(
        df_group["WORKINGDAY"] == 0, 0, df_group["BASELINE"] / df_group["WORKINGDAY"]
    )
    df_group = df_group.reset_index()

    # print(df_group.columns)

    df_prophet_train = df_group[["DATE", "DAILY"]]
    df_prophet_train.columns = ["ds", "y"]

    model_linear = Prophet(
        growth="linear", weekly_seasonality=False, yearly_seasonality=True
    )
    # model_linear.add_seasonality(name='monthly', period=30.5, fourier_order=4)
    model_linear.add_seasonality(name="quarterly", period=90, fourier_order=5)
    # model_linear.add_seasonality(name='midyearly', period=180, fourier_order=6)

    model_linear.fit(df_prophet_train, iter=1000)

    cap = df_prophet_train["y"].quantile(0.9) * 1.20
    floor = df_prophet_train["y"].quantile(0.1) * 1.10

    df_prophet_train["cap"], df_prophet_train["floor"] = cap, floor

    model_growth = Prophet(
        growth="logistic", weekly_seasonality=False, yearly_seasonality=True
    )
    # model_growth.add_seasonality(name='monthly', period=30.5, fourier_order=4)
    model_growth.add_seasonality(name="quarterly", period=90, fourier_order=5)
    # model_growth.add_seasonality(name='midyearly', period=180, fourier_order=6)
    model_growth.fit(df_prophet_train, iter=1000)

    df_prophet_future = model_linear.make_future_dataframe(periods=104, freq="w")

    df_predict_linear = model_linear.predict(df_prophet_future)
    df_predict_linear.rename(columns={"yhat": "yhat_linear"}, inplace=True)

    df_prophet_future["cap"], df_prophet_future["floor"] = cap, floor
    df_predict_growth = model_growth.predict(df_prophet_future)
    df_predict_growth.rename(columns={"yhat": "yhat_growth"}, inplace=True)

    df_predict = df_predict_linear[["ds", "yhat_linear"]].merge(
        df_predict_growth[["ds", "yhat_growth"]], how="inner", on="ds"
    )
    df_predict["YEARWEEK"] = (
        df_predict["ds"].dt.isocalendar().year * 100
        + df_predict["ds"].dt.isocalendar().week
    )
    df_predict["KEY"] = key

    df_predict = df_predict.merge(
        df_group[["YEARWEEK", "DAILY"]], on="YEARWEEK", how="left"
    )
    df_predict = df_predict.fillna(0)

    return df_predict


@ray.remote
def REMOTE_fbprophet_linear_growth(key, df_group):
    try:
        return fbprophet_linear_growth(key, df_group)
    except:
        return pd.DataFrame()

# COMMAND ----------

from tqdm import tqdm

tasks = [
    REMOTE_fbprophet_linear_growth.remote(key, df_group)
    for key, df_group in DF.groupby("KEY")
]
tasks = ray.get(tasks)
tasks = pd.concat(tasks)

# COMMAND ----------

display(tasks)

# COMMAND ----------

tasks.to_parquet(
    "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/OUTPUT_FORECAST/TSSTATS_METHOD/FBPROPHET_TRENDLINE.parquet"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # DEBUG AREA

# COMMAND ----------

key, df_group = data[150]
result = fbprophet_linear_growth(key, df_group)
print(key)
display(result)
display(df_group)

# COMMAND ----------



# COMMAND ----------


# Databricks notebook source
import logging

logging.getLogger("prophet").setLevel(logging.ERROR)
logging.getLogger("cmdstanpy").disabled = True
logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)
logging.getLogger("py4j.clientserver").setLevel(logging.ERROR)

# COMMAND ----------

import pandas as pd
import numpy as np
from scipy.stats import linregress
import warnings

warnings.filterwarnings("ignore")

# COMMAND ----------

import ray; ray.init(ignore_reinit_error=True)

# COMMAND ----------



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

# COMMAND ----------

# MAGIC %md
# MAGIC # DEBUG AREA
# MAGIC

# COMMAND ----------

DF_HISTORY = DF.query("FUTURE == 'N' ")

# COMMAND ----------

min_yearweek = 201918
DF_HISTORY['NPD'] = DF_HISTORY.groupby(['KEY'])['YEARWEEK'].transform('min') > min_yearweek
# DF_NPD = DF_HISTORY.query('NPD == True')
DF_NPD = DF_HISTORY

# COMMAND ----------

from sklearn.preprocessing import MinMaxScaler, StandardScaler

DF_NPD = DF_NPD.sort_values('YEARWEEK')
DF_NPD_ARR = []
for key, df_group in DF_NPD.groupby('KEY'):
  df_group['MINMAX_SCALER'] = MinMaxScaler().fit_transform(X = df_group['BASELINE'].values.reshape(-1, 1))
  df_group['STANDARD_SCALER'] = StandardScaler().fit_transform(X = df_group['BASELINE'].values.reshape(-1, 1))
  DF_NPD_ARR.append(df_group)
DF_NPD = pd.concat(DF_NPD_ARR)
  # break

# COMMAND ----------

DF_NPD = DF_NPD.sort_values('YEARWEEK')
DF_NPD['CUMSUM_BASELINE'] = DF_NPD.groupby(['KEY'])['BASELINE'].transform('cumsum')
DF_NPD['CUMSUM_MINMAX_SCALER'] = DF_NPD.groupby(['KEY'])['MINMAX_SCALER'].transform('cumsum')

# COMMAND ----------

DF_NPD.groupby('KEY')['MINMAX_SCALER'].sum().sort_values()

# COMMAND ----------

DF_NPD['YEARWEEK ORDER'] = DF_NPD.groupby(['KEY'])['YEARWEEK'].transform('rank')

# COMMAND ----------

DF_NPD['BASELINE_DIFF'] = DF_NPD.groupby(['KEY'])['BASELINE'].transform('diff')
DF_NPD['CUMSUM_BASELINE_DIFF'] =  DF_NPD.groupby(['KEY'])['BASELINE_DIFF'].transform('cumsum')

# COMMAND ----------

key = 'NATIONWIDE|HAIR|SUNSILK SHAMPOO BLACK SHINE 900G'
df_check = DF_NPD.query(f"KEY == '{key}'")
print (key)
# df_check['BASELINE'].hist()
df_check.plot(x='YEARWEEK ORDER', y='BASELINE')
df_check.plot(x='YEARWEEK ORDER', y='CUMSUM_BASELINE')
# df_check.plot(x='YEARWEEK ORDER', y='CUMSUM_BASELINE_DIFF')

from prophet import Prophet
df_check['ds'] = pd.to_datetime(df_check['YEARWEEK'].astype(str) + "-4", format="%G%V-%w")
df_check['y'] = df_check['CUMSUM_BASELINE']

model = Prophet(growth='linear', seasonality_mode='additive')
train = df_check[:-52]
test = df_check[-52:]

model.fit(train[['ds', 'y']])

test['FC_CUMSUM_BASELINE'] = list(model.predict(test[['ds']])['yhat'])
test['FC_CUMSUM_BASELINE_DIFF'] = test['FC_CUMSUM_BASELINE'].diff()
test.plot(x='YEARWEEK ORDER', y=['BASELINE', 'FC_CUMSUM_BASELINE_DIFF'])

train['FC_CUMSUM_BASELINE'] = list(model.predict(train[['ds']])['yhat'])
train['FC_CUMSUM_BASELINE_DIFF'] = train['FC_CUMSUM_BASELINE'].diff()
train.plot(x='YEARWEEK ORDER', y=['BASELINE', 'FC_CUMSUM_BASELINE_DIFF'])

# COMMAND ----------

key = 'NATIONWIDE|SAVOURY|KNORR BOUILLONS CHICKEN 330G'
df_check = DF_NPD.query(f"KEY == '{key}'")
print (key)
# df_check['BASELINE'].hist()
df_check.plot(x='YEARWEEK ORDER', y='BASELINE')
df_check.plot(x='YEARWEEK ORDER', y='CUMSUM_BASELINE')
# df_check.plot(x='YEARWEEK ORDER', y='CUMSUM_BASELINE_DIFF')

# COMMAND ----------

key = 'NATIONWIDE|SAVOURY|KNORR BOUILLONS CHICKEN 330G'
df_check = DF_NPD.query(f"KEY == '{key}'")

df_check.plot(x='YEARWEEK ORDER', y='BASELINE')
df_check.plot(x='YEARWEEK ORDER', y='CUMSUM_BASELINE')

# COMMAND ----------

key = 'NATIONWIDE|HAIR|SUNSILK SHAMPOO BLACK SHINE 900G'
df_check = DF_NPD.query(f"KEY == '{key}'")
print (key)
# df_check['BASELINE'].hist()
df_check.plot(x='YEARWEEK ORDER', y='BASELINE')
df_check.plot(x='YEARWEEK ORDER', y='CUMSUM_BASELINE')
# df_check.plot(x='YEARWEEK ORDER', y='CUMSUM_BASELINE_DIFF')

# COMMAND ----------

key = 'NATIONWIDE|FABSOL|SURF LIQUID FLORAL POUCH 3.5KG'
df_check = DF_NPD.query(f"KEY == '{key}'")
df_check['BASELINE'].hist()
df_check.plot(x='YEARWEEK ORDER', y='BASELINE')
df_check.plot(x='YEARWEEK ORDER', y='CUMSUM_BASELINE')

# COMMAND ----------



# COMMAND ----------

DF_NPD.query(f"KEY == '{key}'")['ACTUALSALE'].plot(x=)

# COMMAND ----------

ACTUALSALE

# COMMAND ----------

###############################################################

# COMMAND ----------

# MAGIC %md
# MAGIC # ALGO

# COMMAND ----------

def calculate_phasing_ratio(key, df_group):
  df_group = df_group[['YEARWEEK', 'FUTURE', 'DAILY']]
  df_group["DATE"] = pd.to_datetime(df_group["YEARWEEK"].astype(str) + "-4", format="%G%V-%w")
  df_group["YEAR"] = df_group["DATE"].dt.isocalendar().year
  df_group["MONTH"] = df_group["DATE"].dt.month
  df_group["WEEK"] = df_group["DATE"].dt.isocalendar().week 

  df_group["COUNT WEEKS"] = (
        df_group.groupby(["YEAR", "MONTH"])["WEEK"].transform("count").astype(int)
    )
  df_group["WEEK ORDER"] = (
        df_group.groupby(["YEAR", "MONTH"])["WEEK"].transform("rank").astype(int)
    )
  
  df_history = df_group.query("FUTURE == 'N' ")
  df_history["RATIO WEEK/MONTH"] = df_history["DAILY"] / df_history.groupby(
        ["YEAR", "MONTH"]
    )["DAILY"].transform(sum)
  
  df_ratio_generic = (
        df_history.groupby(["COUNT WEEKS", "WEEK ORDER"])["RATIO WEEK/MONTH"]
        .mean()
        .reset_index()
    )
  df_ratio_generic = df_ratio_generic.query(" `COUNT WEEKS` >= 4")
  df_ratio_generic = df_ratio_generic.rename(
        columns={"RATIO WEEK/MONTH": "RATIO_GENERIC"}
    )
  
  df_group = df_group.merge(df_ratio_generic, on=["COUNT WEEKS", "WEEK ORDER"])
  df_group['KEY'] = key
  df_group = df_group[['KEY', 'YEARWEEK', 'RATIO_GENERIC']]
  return df_group

# COMMAND ----------

from prophet import Prophet

def ts_stats_cumsum_forecast(key, df_group, target_var, exo_vars, categorical_vars):
  df_group = df_group.sort_values(by='YEARWEEK')
  df_group['CUMSUM_DAILY'] = df_group['DAILY'].transform('cumsum')
  df_group["ds"] = pd.to_datetime(df_group["YEARWEEK"].astype(str) + "-4", format="%G%V-%w")
  df_group['y'] = df_group['CUMSUM_DAILY']

  df_train = df_group.query("FUTURE == 'N' ")[['ds', 'y']]

  model = Prophet(growth='linear')
  model.fit(df_train, iter=1000)
  
  df_forecast = model.predict(df_group[['ds']])
  df_forecast = df_forecast[['ds', 'yhat']].sort_values('ds')
  df_forecast['YHAT_DAILY'] = df_forecast['yhat'].diff()
  df_forecast = df_forecast[['ds', 'YHAT_DAILY']]

  df_group["DATE"] = df_group["YEARWEEK"].astype(str)
  df_group["DATE"] = pd.to_datetime(df_group["DATE"] + "-4", format="%G%V-%w")
  df_group["YEAR"] = df_group["DATE"].dt.isocalendar().year
  df_group["MONTH"] = df_group["DATE"].dt.month
  df_group["WEEK"] = df_group["DATE"].dt.isocalendar().week 

  df_group = df_group.merge(df_forecast, on='ds', how='inner')
  df_ratio = calculate_phasing_ratio(key, df_group)
  df_group = df_group.merge(df_ratio, on=['KEY', 'YEARWEEK'], how='left')

  df_group['YHAT_DAILY_PHASING'] = df_group.groupby(['YEAR', 'MONTH'])['YHAT_DAILY'].transform('sum') * df_group['RATIO_GENERIC']
  df_group['YHAT_DAILY_PHASING'] = df_group['YHAT_DAILY_PHASING'].fillna(0)

  return df_group

@ray.remote
def REMOTE_ts_stats_cumsum_forecast(key, df_group, target_var, exo_vars, categorical_vars):
  try: 
    result = ts_stats_cumsum_forecast(key, df_group, target_var, exo_vars, categorical_vars)
    return result
  except:
    return pd.DataFrame()

# COMMAND ----------

# key = 'NATIONWIDE|HAIR|SUNSILK SHAMPOO BLACK SHINE 900G'
# df_group = DF[(DF['KEY'] == key)]
# ts_stats_cumsum_forecast(key, df_group, target_var='BASELINE', exo_vars=[], categorical_vars=[])

# COMMAND ----------



# COMMAND ----------

target_var = "BASELINE"
tasks = [REMOTE_ts_stats_cumsum_forecast.remote(key, df_group, target_var, exo_vars=[], categorical_vars=[]) for key, df_group in  DF.groupby(["KEY"])]
tasks = ray.get(tasks)
df_result = pd.concat(tasks)

# COMMAND ----------

df_result

# COMMAND ----------

df_result = df_result.dropna()
df_result[['BANNER', 'CATEGORY', 'DPNAME']] = df_result['KEY'].str.split('|', expand=True)
df_nationwide = df_result.query("BANNER == 'NATIONWIDE' ")

# COMMAND ----------

df_nationwide = df_nationwide.round(3)

# COMMAND ----------

display(df_nationwide)

# COMMAND ----------

display(df_nationwide)

# COMMAND ----------



# COMMAND ----------

def ts_stats_forecast(key, df_group, target_var, exo_vars, categorical_vars):

    df_group = df_group[["KEY", "YEARWEEK", "WORKINGDAY", "FUTURE", target_var]]
    df_group["DAILY"] = np.where(
        df_group["WORKINGDAY"] == 0, 0, df_group[target_var] / df_group["WORKINGDAY"]
    )
    # df_group = df_group.reset_index()
    df_group = df_group.sort_values("YEARWEEK")

    df_history = df_group.query("FUTURE == 'N' ")
    df_group = df_group.query(f"YEARWEEK > {df_history['YEARWEEK'].max() - 200}")

    df_group["DATE"] = df_group["YEARWEEK"].astype(str)
    df_group["DATE"] = pd.to_datetime(df_group["DATE"] + "-4", format="%G%V-%w")
    df_group["YEAR"] = df_group["DATE"].dt.isocalendar().year
    df_group["MONTH"] = df_group["DATE"].dt.month
    df_group["WEEK"] = df_group["DATE"].dt.isocalendar().week
    df_group["QUARTER"] = df_group["DATE"].dt.quarter
    df_group["QUARTER_GROUP"] = np.where(df_group["QUARTER"].isin([1, 2]), 1, 2)

    df_group["COUNT WEEKS"] = (
        df_group.groupby(["YEAR", "MONTH"])["WEEK"].transform("count").astype(int)
    )
    df_group["WEEK ORDER"] = (
        df_group.groupby(["YEAR", "MONTH"])["WEEK"].transform("rank").astype(int)
    )

    df_group["YEARWEEK ORDER"] = df_group["YEARWEEK"].transform("rank").astype("int")

    df_history = df_group.query("FUTURE == 'N' ")
    df_future = df_group.query("FUTURE == 'Y' ")

    df_history["RATIO WEEK/MONTH"] = df_history["DAILY"] / df_history.groupby(
        ["YEAR", "MONTH"]
    )["DAILY"].transform(sum)

    df_ratio_month = (
        df_history.groupby(["MONTH", "COUNT WEEKS", "WEEK ORDER"])["RATIO WEEK/MONTH"]
        .mean()
        .reset_index()
    )
    df_ratio_month = df_ratio_month.query(" `COUNT WEEKS` >= 4")
    df_ratio_month = df_ratio_month.rename(columns={"RATIO WEEK/MONTH": "RATIO_MONTH"})

    df_ratio_generic = (
        df_history.groupby(["COUNT WEEKS", "WEEK ORDER"])["RATIO WEEK/MONTH"]
        .mean()
        .reset_index()
    )
    df_ratio_generic = df_ratio_generic.query(" `COUNT WEEKS` >= 4")
    df_ratio_generic = df_ratio_generic.rename(
        columns={"RATIO WEEK/MONTH": "RATIO_GENERIC"}
    )

    ###############################################################################
    df_history = df_history.merge(df_ratio_generic, on=["COUNT WEEKS", "WEEK ORDER"])
    df_history = df_history.sort_values("YEARWEEK")

    df_group_arr = []
    for key, df_group in df_history.groupby(["YEAR", "QUARTER_GROUP"]):
        if len(df_group) >= 13:
            trend_model = linregress(
                x=df_group["YEARWEEK ORDER"].values, y=df_group["DAILY"].values
            )
            df_group["INTERCEPT"] = trend_model.intercept
            df_group["SLOPE"] = trend_model.slope
        df_group_arr.append(df_group)
    df_group_arr = pd.concat(df_group_arr)
    df_group_arr["INTERCEPT"] = (
        df_group_arr["INTERCEPT"].fillna(method="bfill").fillna(method="ffill")
    )
    df_group_arr["SLOPE"] = (
        df_group_arr["SLOPE"].fillna(method="bfill").fillna(method="ffill")
    )
    df_group_arr["TRENDLINE"] = (
        df_group_arr["INTERCEPT"]
        + df_group_arr["SLOPE"] * df_group_arr["YEARWEEK ORDER"]
    ) * df_group_arr["WORKINGDAY"]

    print(df_group_arr)

    df_group_arr[f"FC_TRENDLINE_{target_var}"] = (
        df_group_arr.groupby(["YEAR", "MONTH"])["TRENDLINE"].transform("sum")
        * df_group_arr["RATIO_GENERIC"]
    )

    df_history = df_group_arr

    # if (int(len(df_history) / 26) > 2):
    #   arr_length =  int(len(df_history) / 26)
    #   df_history_split_arr = np.array_split(df_history, arr_length)
    #   if len(df_history_split_arr[arr_length - 1]) < 13:
    #     df_history_split_arr[arr_length - 2] = df_history_split_arr[arr_length - 2].append(df_history_split_arr[arr_length - 1])
    #     df_history_split_arr = df_history_split_arr[:(arr_length-2)]

    #   df_history_result_arr = []
    #   for df_split in df_history_split_arr:
    #     trend_model = linregress(x=df_split['YEARWEEK ORDER'].values, y=df_split['DAILY'].values)
    #     df_split['TRENDLINE'] = (trend_model.intercept + trend_model.slope * df_split['YEARWEEK ORDER']) * df_split['WORKINGDAY']
    #     df_split[f'FC_TRENDLINE_{target_var}'] = df_split.groupby(['YEAR', 'MONTH'])['TRENDLINE'].transform('sum') * df_split['RATIO_GENERIC']
    #     df_history_result_arr.append(df_split)
    #   df_history = pd.concat(df_history_result_arr)
    #   # print (df_history_result_arr)
    # else:
    #   df_history['TRENDLINE'] = 0
    #   df_history[f'FC_TRENDLINE_{target_var}'] = 0
    #   ##### DEBUG ONLY #####
    #   # return df_history_result_arr
    ###############################################################################

    fulltime_slope, fulltime_intercept, r_value, p_value, std_err = linregress(
        x=df_history["YEARWEEK ORDER"].values, y=df_history["DAILY"].values
    )

    last_26weeks_slope, last_26weeks_intercept, r_value, p_value, std_err = linregress(
        x=df_history["YEARWEEK ORDER"][-26:].values, y=df_history["DAILY"][-26:].values
    )

    # print(fulltime_slope, fulltime_intercept, last_26weeks_slope, last_26weeks_intercept)

    weighted_arr = [1, 0.5]
    intercept_arr = [fulltime_intercept, last_26weeks_intercept]
    slope_arr = [fulltime_slope, last_26weeks_slope]

    forecast_intercept = sum(
        [intercept_arr[idx] * weight for idx, weight in enumerate(weighted_arr)]
    ) / sum(weighted_arr)
    forecast_slope = sum(
        [slope_arr[idx] * weight for idx, weight in enumerate(weighted_arr)]
    ) / sum(weighted_arr)

    # forecast_intercept = (1.5*fulltime_intercept + 0.5*last_26weeks_intercept)/2
    # forecast_slope  = (1.5*fulltime_slope + 0.5*last_26weeks_slope)/2

    df_future = df_future.merge(
        df_ratio_month, on=["MONTH", "COUNT WEEKS", "WEEK ORDER"], how="left"
    )
    df_future = df_future.merge(
        df_ratio_generic, on=["COUNT WEEKS", "WEEK ORDER"], how="left"
    )
    df_future["FINAL_RATIO"] = df_future["RATIO_MONTH"]
    df_future["FINAL_RATIO"] = df_future["RATIO_GENERIC"]
    # df_future['FINAL_RATIO'] = df_future['FINAL_RATIO'].fillna(df_future['RATIO_GENERIC'])
    # df_future['FINAL_RATIO'] = (df_future['FINAL_RATIO'] + df_future['RATIO_GENERIC'])/2

    df_future["TRENDLINE"] = (
        forecast_intercept + forecast_slope * df_future["YEARWEEK ORDER"]
    )
    df_future["FC_DAILY"] = (
        df_future.groupby(["YEAR", "MONTH"])["TRENDLINE"].transform("sum")
        * df_future["FINAL_RATIO"]
    )

    df_future[f"FC_TRENDLINE_{target_var}"] = (
        df_future["FC_DAILY"] * df_future["WORKINGDAY"]
    )
    df_future[f"TRENDLINE"] = df_future["TRENDLINE"] * df_future["WORKINGDAY"]

    # df_future['SLOPE_2_YEAR'] = fulltime_slope
    # df_future['INTERCEPT_2_YEAR'] = fulltime_intercept
    # df_future['SLOPE_26_WEEK'] = last_26weeks_slope
    # df_future['INTERCEPT_26_WEEK'] = last_26weeks_intercept
    # df_future['SLOPE_FUTURE'] =  forecast_slope
    # df_future['INTERCEPT_FUTURE'] = forecast_intercept

    debug_columns = "SLOPE_2_YEAR,INTERCEPT_2_YEAR,SLOPE_26_WEEK,INTERCEPT_26_WEEK,SLOPE_FUTURE,INTERCEPT_FUTURE".split(
        ","
    )

    # df_future = df_future[['KEY', 'YEARWEEK', f'FC_{target_var}', *debug_columns]]
    # df_future = df_future[['KEY', 'YEARWEEK', 'MONTH', 'COUNT WEEKS', 'WEEK ORDER', 'FINAL_RATIO', 'TRENDLINE', f'FC_{target_var}']]

    df_result = df_future.append(df_history)
    df_result = df_result[
        ["KEY", "YEARWEEK", "TRENDLINE", f"FC_TRENDLINE_{target_var}"]
    ]
    return df_result

# COMMAND ----------

key = "NATIONWIDE|HAIR|CLEAR SHAMPOO MEN COOL SPORT 900G"
df_group = DF.query(f"KEY == '{key}' ")
target_var = "BASELINE"

df_check = ts_stats_forecast(
    key, df_group, target_var, exo_vars=[], categorical_vars=[]
)
df_group = df_group.merge(df_check, on=["KEY", "YEARWEEK"], how="left").fillna(0)
display(df_group)

# COMMAND ----------

from tqdm import tqdm
import warnings

warnings.filterwarnings("ignore")
df_result = []
target_var = "BASELINE"
for key, df_group in tqdm(DF.groupby(["KEY"])):
    try:
        task = ts_stats_forecast(
            key, df_group, target_var, exo_vars=[], categorical_vars=[]
        )
        df_result.append(task)
    except:
        pass


df_result = pd.concat(df_result)
df_result = df_result.dropna()

DF_OUTPUT = DF.merge(df_result, on=["KEY", "YEARWEEK"], how="left")
DF_OUTPUT = DF_OUTPUT.fillna(0)
DF_OUTPUT[["BANNER", "CATEGORY", "DPNAME"]] = DF_OUTPUT["KEY"].str.split(
    "|", expand=True
)
DF_OUTPUT[f"FC_TRENDLINE_{target_var}"].loc[
    (DF_OUTPUT[f"FC_TRENDLINE_{target_var}"] < 0)
] = 0

# COMMAND ----------



# COMMAND ----------

DF_OUTPUT = DF_OUTPUT.drop(columns=["CHECK", "WORKINGDAY"])

# COMMAND ----------

# import shutil

# def write_excel_file(df, file_name, sheet_name, dbfs_directory):
#   df.to_excel(f"/tmp/{file_name}.xlsx", index=False, sheet_name=sheet_name)
#   shutil.copy(f"/tmp/{file_name}.xlsx", dbfs_directory + f"/{file_name}.xlsx")

# for key, df_group in DF_OUTPUT.groupby('CATEGORY'):
#   print (key)
#   write_excel_file(df_group, key, sheet_name='DATA', dbfs_directory='/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/OUTPUT_FORECAST/TSSTATS_METHOD/')

# COMMAND ----------

DF_OUTPUT.to_parquet(
    "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/OUTPUT_FORECAST/TRENDLINE_METHOD.parquet",
    index=False,
)
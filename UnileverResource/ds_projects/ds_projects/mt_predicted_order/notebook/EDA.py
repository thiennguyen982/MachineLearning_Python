# Databricks notebook source
# Import Neccessary Library
import os
# Setup environment

# os.environ["PYSPARK_PYTHON"] = "C:\\Users\\Dao-Minh.Toan\\AppData\\Local\\anaconda3\\python.exe"

import re
import sys

from datetime import date
from pathlib import Path
import numpy as np
import pandas as pd

# Spark Function
import pyspark.pandas as ps
from pyspark.sql.functions import expr
from pyspark.sql import SparkSession

# Visualization
import matplotlib.pyplot as plt

import seaborn as sns
import seaborn.objects as so
import plotly
import plotly.express as px
import plotly.graph_objects as go


from sklearn.pipeline import Pipeline 
from sklearn.compose import ColumnTransformer

# Preprocessing
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder

# Tree base model
from sklearn.ensemble import RandomForestRegressor
from lightgbm.sklearn import LGBMRegressor
from xgboost.sklearn import XGBRFRegressor
from catboost import CatBoostRegressor

# model selection
from sklearn.model_selection import train_test_split, TimeSeriesSplit, cross_val_predict, cross_validate

# statsmodels
# ACF and PACF
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf

from typing import Optional, Union, List


sys.path.insert(0, "/Workspace/Repos/dao-minh.toan@unilever.com/ds_projects/mt_demand_sensing/") # Add path to common package
from common.io_spark import read_excel_spark

# Init Spark Session
spark: SparkSession = (SparkSession.builder 
         .master("local")
         .appName("Forecasting Demand sensing") 
         .config("spark.driver.bindAddress", "0.0.0.0") 
         .config("spark.ui.port","4050") # Port of spark UI:
         .getOrCreate()
)

print(f"""
PYSPARK_PYTHON: {os.environ["PYSPARK_PYTHON"]}
JAVA_HOME: {os.environ["JAVA_HOME"]} 
Spark Context
spark_version: {spark.version}
""")


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
            df = pd.read_excel(file_path, header = header, sheet_name = sheet_name)
            if isinstance(df, dict):
                print(f"list of sheet_name: {df.keys()}")
                df = df.items()[0][1]
            dfs.append(df)
        data = pd.concat(dfs)
        data = data.apply(lambda x: x.astype(str))
        return data
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
    df_path = ps.DataFrame({"file_path": file_path})
    io_spark = IOSpark()

    def read_excel_spark_io(
        pdf, header: bool = True, sheet_name: str = None
    ):
        return io_spark.read_excel_batch(
            pdf["file_path"].tolist(), header=header, sheet_name=sheet_name
        )

    df_spark = df_path.pandas_on_spark.apply_batch(
        read_excel_spark_io, header=header, sheet_name=sheet_name
    )
    return df_spark

files_path = [
    str(file_path)
    for file_path in Path(remove_repeated_path).glob("*2023*SL_SAIGON COOP.xlsx")
]
print("files_path: ", files_path[:5])
df_sale: ps.DataFrame = read_excel_spark(files_path, header = 1, sheet_name = 0)

# COMMAND ----------

ps.read

# COMMAND ----------

df_sale = ps.read_excel("/mnt/adls/MT_POST_MODEL/REMOVE REPEATED/", header=1, sheet_name = 0, dtype = "str")


# COMMAND ----------

df_sale.head()


# COMMAND ----------

df_sale.shape

# COMMAND ----------

forecasing_level = [
    'ds',
    'dp_name',
    'region',
    'banner',
]

def print_columns(df: pd.DataFrame)-> None:
    for col in df.columns:
        print(f'"{col}",')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Collection

# COMMAND ----------

# Data PATH
# root_path = "./../data"
root_path = "/dbfs/mnt/adls"
promotion_lib_path = f"{root_path}/MT_POST_MODEL/PROMOTION_LIB/"
remove_repeated_path = f"{root_path}/MT_POST_MODEL/'REMOVE REPEATED'/"
master_path = f"{root_path}/MT_POST_MODEL/MASTER"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sub-Problem Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### Promotion Factor

# COMMAND ----------

# explore promotion lib
# Dimension: ds, banner, dp_name

combined_header = pd.read_excel(f"{promotion_lib_path}/2023 Promotion (for upload).xlsx", nrows=6, header = None)
df_promotion = pd.read_excel(f"{promotion_lib_path}/2023 Promotion (for upload).xlsx", header = None, skiprows = 6, nrows=10000)
df_promotion.columns = combined_header.iloc[1]
df_promotion = df_promotion.rename(columns = {"Banner": "banner", "DP Name": "dp_name"}) # without region
df_promotion.head()

# COMMAND ----------

for col in df_promotion.columns:
    print(col)

# COMMAND ----------

df_promotion['Order start date'] = pd.to_datetime(df_promotion['Order start date'])
df_promotion['Order end date'] = pd.to_datetime(df_promotion['Order end date'])
df_promotion['ds'] = df_promotion.apply(lambda row: pd.date_range(row['Order start date'], row['Order end date']), axis=1)

df_promotion_explode = df_promotion.explode("ds")
# df_promotion_explode
df_promotion_clean = df_promotion_explode.loc[:,[
    "ds",
    "banner",
    "dp_name",
    "ULV code",
    "Order start date",
    "Order end date",
    
    "Actual Mechanic",
    "On/Off post",
    "Promotion type",
    "Promotion grouping",
    "Gift type",
    "Gift value",
    "%TTS",
    "% BMI",
    "Basket promotion",
]]
df_promotion_clean

# COMMAND ----------

for col in df_promotion_clean.columns:
    print(col)

# COMMAND ----------

df_promotion_clean["On/Off post"].unique()

# COMMAND ----------

# finalize df promotion
features_promotion = [
    "ds",
    "banner",
    "dp_name",
    "ULV code",
    "Order start date",
    "Order end date",
    
    "Actual Mechanic",
    "On/Off post",
    "Promotion type",
    "Promotion grouping",
    "Gift type",
    "Gift value",
    "%TTS",
    "% BMI",
    "Basket promotion",
    
]
df_promotion_final = df_promotion_clean
df_promotion_final.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales Trend Analysis

# COMMAND ----------

files_path = [str(file_path) for file_path in Path(remove_repeated_path).glob("*2023*SL_SAIGON COOP.xlsx")]
# ps.Series(files_path)
print(files_path)

# COMMAND ----------

# explore sale data

# sale_paths = [
#   f"{remove_repeated_path}/2023.1_SL_SAIGON COOP.xlsx"
# ]
# df_sale = []
# for sale_path in sale_paths:
#     df = pd.read_excel(sale_path, header = 1)
#     df_sale.append(df)
# df_sale = pd.concat(df_sale)
files_path = [
    str(file_path)
    for file_path in Path(remove_repeated_path).glob("*2023*SL_SAIGON COOP.xlsx")
]
print("files_path: ", files_path[:5])
df_sale: ps.DataFrame = read_excel_spark(files_path, header = 1, sheet_name = 0)
df_sale = df_sale.to_pandas()

df_sale = df_sale.rename(columns = {"Billing Date": "ds", "Banner": "banner", "Region": "region", "DP Name": "dp_name"})
df_sale["ds"] = pd.to_datetime(df_sale["ds"])
df_sale.head(3)

# COMMAND ----------

df_sale_clean = df_sale.loc[:, [
    "ds",
    "banner",
    "region",
    "dp_name",
    "U Price (cs)",
    
    "Order (pcs)",
    "Order (CS)",
    "Key In Before(CS)",
    "Key In After(CS)",
    "Key In After(Ton-Plt)",
    "Key In After(Value)",
    "Loss at Keying (CS)",
    "Loss at Keying (Value)",
    "Delivery Plan (CS)",
    "Loss at Delivery Plan (CS)",
    "Billing (CS)",
    "Loss at Billing (CS)",
    
    
    "Est. Demand (cs)",
]]

# COMMAND ----------

df_sale_final = df_sale_clean
df_sale_final.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calendar Event and Seasonality

# COMMAND ----------

df_calendar = pd.read_excel(f"{master_path}/CALENDAR_MASTER.xlsx", engine="openpyxl")
df_calendar = df_calendar.rename(columns = {"BILLING DATE": "ds"})
df_calendar["ds"] = pd.to_datetime(df_calendar["ds"])
df_calendar.head()

# COMMAND ----------

print_columns(df_calendar)

# COMMAND ----------

features_calendar = [
    "ds",
    "YEAR",
"YEARWEEK",
"YEARDAY",
"HOLIDAY",
"WEIGHT_BEFORE",
"DAY_BEFORE_HOLIDAY_CODE",
"DAY_BEFORE_HOLIDAY",
"WEIGHT HOLIDAY",
"DAY_AFTER_HOLIDAY_CODE",
"ANNIVERSARYDAY",
"ANNIVERSARYDAY _CODE",
"BEFORE ANNIVERSARYDAY",
"AFTERANNIVERSARYDAY",
"MTWORKINGDAY",
"COVID_IMPACT",
"COVID_IMPACT_SOUTH",
"QUARTER",
"MONTH_TOTAL",
"DAY_TOTAL",
"WEEK_TOTAL",
"WEEKDAY_TEXT",
"WEEKDAY",
"WEEKOFMONTH",
]
df_calendar_final = df_calendar.loc[:, features_calendar]
df_calendar_final.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Price Sensitivity

# COMMAND ----------

df_price_change = pd.read_excel(f"{master_path}/PRICE_CHANGE_MASTER.xlsx", engine="openpyxl")
df_price_change = df_price_change.rename(columns = {"BILLING DATE": "ds", "BANNER": "banner", "DP NAME": "dp_name"})
df_price_change["ds"] = pd.to_datetime(df_price_change["ds"])
df_price_change.head()


# COMMAND ----------

df_price_change_final = df_price_change

df_price_change_final.head(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concatenate all of analysis

# COMMAND ----------

df_final = df_sale_final.merge(df_promotion_final, on = ['ds', 'banner', 'dp_name'], how = "left")
df_final = df_final.merge(df_price_change_final, on = ['ds', 'banner', 'dp_name'], how = "left")
df_final = df_final.merge(df_calendar_final, on = ['ds'])
label = "Est. Demand (cs)"
df_final.head(3)

# COMMAND ----------

# Convert dtypes
for col in df_final.columns:
    try:
        df_final[col] = pd.to_numeric(df_final[col], downcast="float")
    except:
        print(f"Error convert to numeric: {col}")
df_final["ds"] = pd.to_datetime(df_final["ds"])
df_final = df_final.sort_values(by = ["ds"])
print(f"""
dtype: {df_final.dtypes}
""")

# COMMAND ----------

# Correlation
plt.figure(figsize=(25, 10))
corr = df_final.select_dtypes(include='number').corr()
number_of_columns = 10
corr = corr.sort_values(by = label, ascending=False).iloc[:number_of_columns, :number_of_columns]

# corr.head()
mask = np.triu(np.ones_like(corr, dtype=bool))

ax = sns.heatmap(corr, mask=mask, xticklabels=corr.columns, yticklabels=corr.columns, annot=True, linewidths=.2, cmap='coolwarm', vmin=-1, vmax=1)


# COMMAND ----------

# ACF, PACF and stationarity
from statsmodels.tsa.stattools import adfuller


banner = "SAIGON COOP"
region = "V104"
dp_name = "Cornetto Royal Chocoluv 88G"

df_sample = df_final.query(f"(banner == '{banner}' & region == '{region}' & dp_name == '{dp_name}')")
df_sample = df_sample.sort_values(by = ["ds"])
df_sample["ds"] = pd.to_datetime(df_sample["ds"])
df_sample[label] = df_sample[label].astype(float)
fig, ax = plt.subplots(3, 1, figsize = (12, 10))
plot_acf(df_sample[label], ax = ax[0], lags = 100, title = f"Auto Correlation {label}")
plot_pacf(df_sample[label], ax = ax[1], lags = 100, title = f"Partial Auto Correlation {label}")
sns.lineplot(data = df_sample, x = "ds", y = label, ax = ax[2], sort=True)
plt.show()

# Perform Dickey-Fuller test
result = adfuller(df_sample[label])
# Output the results
print('ADF Statistic: %f' % result[0])
print('p-value: %f' % result[1])
print('Critical Values:')
for key, value in result[4].items():
    print('\t%s: %.3f' % (key, value))

# Interpretation
if result[1] > 0.05:
    print("Failed to reject the null hypothesis (H0), the data has a unit root and is non-stationary")
else:
    print("Reject the null hypothesis (H0), the data does not have a unit root and is stationary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Engineering

# COMMAND ----------

df_final

# COMMAND ----------

# Lag features
def create_lag_features(df: pd.DataFrame,
                        column_to_aggregate: str, 
                        datetime_name: str, 
                        group_keys: List[str], 
                        lags: List[int],
                        aggs: List[str] = ["sum"],
                        ):
    df = df.sort_values(by = group_keys + [datetime_name])
    df_lagged = df
    for lag in lags:
        new_col_name = f"{column_to_aggregate}_{lag}D_mean"
        df_lagged[new_col_name] = df.groupby(group_keys)
        df_lagged[new_col_name] = df.groupby(group_keys)[column_to_aggregate].transform(
                    lambda x: x.rolling(f'{lag}D', closed='left')).agg("mean")

        # df_agg = df.set_index(date_column).groupby(group_keys)[lagged_column_name].rolling(window = f"{day+1}D").apply(lambda x: np.mean(x[:-1]) if len(x) > 1 else np.nan).reset_index().rename(col)
        # df = df.merge(df_copy, how = "left", on = ["ds"] + group_keys)
    return df_lagged
df_final = create_lag_features(df_final, 
                        column_to_aggregate="Est. Demand (cs)",
                        datetime_name = "ds",
                        group_keys=["banner", "region", "dp_name"],
                        lags=[1, 7, 14]
                        )
df_final.head()

# COMMAND ----------

df_final["ds"].dtype

# COMMAND ----------



# COMMAND ----------

df_final.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Validation

# COMMAND ----------

# Train Test Validation
df_final = df_final.sort_values(by = ["ds"])
df_final = df_final.reset_index()

sample_size = df_final.shape[0]
test_size= 0.2
df_train, df_test = df_final.iloc[:int(sample_size*(1- test_size))], df_final.iloc[int(sample_size*(1-test_size)):]
print(f"""
train:  
  size: {df_train.shape} 
test:
  size: {df_test.shape}
""")


# COMMAND ----------


# df_train.to_csv("./fe.csv")
df_train.head(2)

# COMMAND ----------

for col in df_train.columns:
    print(f'"{col}",')

# COMMAND ----------

label = "Est. Demand (cs)"

numerical_columns = [
  "U Price (cs)",
  "Order (pcs)",
  "Order (CS)",
  # "Basket promotion",

  # Calendar
#   "YEAR",
# "YEARWEEK",
# "YEARDAY",

# "WEIGHT_BEFORE",
# "DAY_BEFORE_HOLIDAY_CODE",
# "DAY_BEFORE_HOLIDAY",
# "WEIGHT HOLIDAY",
# "DAY_AFTER_HOLIDAY_CODE",
# "ANNIVERSARYDAY",
# "ANNIVERSARYDAY _CODE",
# "BEFORE ANNIVERSARYDAY",
# "AFTERANNIVERSARYDAY",
# "MTWORKINGDAY",
# "COVID_IMPACT",
# "COVID_IMPACT_SOUTH",
# "QUARTER",
# "MONTH_TOTAL",
# "DAY_TOTAL",
# "WEEK_TOTAL",
# "WEEKDAY_TEXT",
# "WEEKDAY",
# "WEEKOFMONTH",

# Engineer estimate demand
"avg_Est. Demand (cs)_1_days_before",
"avg_Est. Demand (cs)_7_days_before",
"avg_Est. Demand (cs)_14_days_before",
]

categorical_columns = [
  "banner",
  "region",
  "PRICE CHANGE",

  # Calendar
  "HOLIDAY",
  
]
for col in numerical_columns:
    df_train[col] = pd.to_numeric(df_train[col])
    df_test[col] = pd.to_numeric(df_test[col])



# COMMAND ----------

# MAGIC %md
# MAGIC # Model Pipeline

# COMMAND ----------

# Model pipeline

numerical_preprocessing = Pipeline(steps = [
  ("imputer", SimpleImputer()),
])

categorical_preprocessing = Pipeline(steps = [
  ("imputer", SimpleImputer(strategy="most_frequent")),
  ("encoder", OneHotEncoder(handle_unknown = "ignore"))
])

combine_preprocessing = ColumnTransformer(transformers=[
        ("numerical", numerical_preprocessing, numerical_columns),
        ("categorial", categorical_preprocessing, categorical_columns)
    ],
                                          verbose_feature_names_out = False,
                                          n_jobs=-1
                                          )
pipe = Pipeline(steps = [
  ("combine", combine_preprocessing),
  ("estimator", RandomForestRegressor())
])
pipe

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fitting Model

# COMMAND ----------

cv_results = cross_validate(
  estimator=pipe,
  X = df_train.loc[:, numerical_columns + categorical_columns],
  y = df_train[label],
  scoring="neg_mean_absolute_percentage_error",
  cv = TimeSeriesSplit(n_splits=2),
  return_estimator=True,
  return_train_score=True
)

print(f"""
train_score: {cv_results["train_score"]}, avg_train_score: {np.mean(cv_results["train_score"])}
test_score: {cv_results["test_score"]}, avg_test_score: {np.mean(cv_results["test_score"])}

cv_results: {cv_results} 
""")

# COMMAND ----------

best_pipe = cv_results["estimator"][-1]
best_pipe

# COMMAND ----------

# Feature Importance

import seaborn.objects as so
ft = pd.DataFrame()
# best_pipe = cv_results["estimator"][np.argmax(cv_results["test_auc"])]
best_pipe = cv_results["estimator"][-1]
ft["features"] = best_pipe[-2].get_feature_names_out()
ft["feature_importances"] = best_pipe[-1].feature_importances_

# (so
#  .Plot(ft.sort_values(by = ["feature_importances"], ascending=False), y = "features", x = "feature_importances")
#  .add( so.Bars())
#  .layout(size=(8, 6))
# )
px.bar(
  data_frame= ft.sort_values(by = ["feature_importances"], ascending=True),
  y = "features",
  x = "feature_importances"
)

# COMMAND ----------

df_test.loc[:, numerical_columns + categorical_columns].head()

# COMMAND ----------

df_train["prediction"] = best_pipe.predict(df_train.loc[:, numerical_columns + categorical_columns])
df_test["prediction"] = best_pipe.predict(df_test.loc[:, numerical_columns + categorical_columns])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model/Error Analysis

# COMMAND ----------




banner = "SAIGON COOP"
region = "V104"
dp_name = "Cornetto Royal Chocoluv 88G"

df_train_viz = df_train.query(f"(banner == '{banner}' & region == '{region}' & dp_name == '{dp_name}')")
df_test_viz = df_test.query(f"(banner == '{banner}' & region == '{region}' & dp_name == '{dp_name}')")

fig = go.Figure([
    go.Scatter(name = "train y_true", x = df_train_viz["ds"], y = df_train_viz[label]),
    go.Scatter(name = "train y_pred", x = df_train_viz["ds"], y = df_train_viz["prediction"]),

    go.Scatter(name = "test y_true", x = df_test_viz["ds"], y = df_test_viz[label]),
    go.Scatter(name = "test y_pred", x = df_test_viz["ds"], y = df_test_viz["prediction"]),
])
fig.show()



# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# df_train["region"].unique()
df_train.groupby(["banner", "region", "dp_name"]).size().sort_values()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Explainability in shap value

# COMMAND ----------

# import shap

# # explainer = shap.Explainer(best_pipe)
# explainer = shap.KernelExplainer(best_pipe.predict, df_train.loc[:, numerical_columns+categorical_columns], link = "logit")

# shap_values = explainer.shap_values(df_test.loc[:, numerical_columns+categorical_columns], nsamples = 100)



# COMMAND ----------


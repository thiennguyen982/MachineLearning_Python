# Databricks notebook source
# MAGIC %md
# MAGIC # Prepare Data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Timeseries: ds
# MAGIC -- Customer: Banner, region, 
# MAGIC -- Product: ulv_code
# MAGIC
# MAGIC with date_ranges as (
# MAGIC select
# MAGIC -- primary key
# MAGIC trim(lower(banner)) as banner,
# MAGIC region,
# MAGIC trim(lower(dp_name)) as dp_name,
# MAGIC min(to_date(billing_date)) as min_date,
# MAGIC max(to_date(billing_date)) as max_date
# MAGIC
# MAGIC from mt_predicted_order.mt_sale
# MAGIC group by banner, region, dp_name
# MAGIC )
# MAGIC , dates_expanded AS (
# MAGIC SELECT
# MAGIC EXPLODE(sequence(to_date("2023-01-01"), to_date("2024-03-01"), INTERVAL 1 DAY)) AS ds,
# MAGIC banner,
# MAGIC region,
# MAGIC dp_name
# MAGIC
# MAGIC FROM date_ranges
# MAGIC )
# MAGIC , dates_final as (
# MAGIC select
# MAGIC ds,
# MAGIC banner,
# MAGIC region,
# MAGIC dp_name
# MAGIC from dates_expanded
# MAGIC )
# MAGIC
# MAGIC -- Calendar
# MAGIC , calendar as (
# MAGIC select 
# MAGIC to_date(date) as ds,
# MAGIC year,
# MAGIC quarter,
# MAGIC month,
# MAGIC dayofmonth,
# MAGIC dayofweek,
# MAGIC weekend,
# MAGIC dayofyear,
# MAGIC weekofyear,
# MAGIC
# MAGIC is_year_start,
# MAGIC is_year_end,
# MAGIC is_quarter_start,
# MAGIC is_quarter_end,
# MAGIC is_month_start,
# MAGIC is_month_end,
# MAGIC
# MAGIC is_leap_year,
# MAGIC vn_holiday,
# MAGIC us_holiday,
# MAGIC
# MAGIC lunar_date,
# MAGIC lunar_year,
# MAGIC lunar_month,
# MAGIC lunar_day,
# MAGIC season
# MAGIC
# MAGIC from mt_predicted_order.mt_calendar_master
# MAGIC )
# MAGIC
# MAGIC , calendar_final as (
# MAGIC select *
# MAGIC from calendar
# MAGIC )
# MAGIC , product_information as (
# MAGIC select 
# MAGIC cast(ulv_code as bigint) as ulv_code,
# MAGIC trim(lower(dp_name)) as dp_name,
# MAGIC class, 
# MAGIC category,
# MAGIC packsize,
# MAGIC packgroup,
# MAGIC brand_variant,
# MAGIC subbrand,
# MAGIC brand,
# MAGIC segment,
# MAGIC format,
# MAGIC production_segment
# MAGIC from mt_predicted_order.mt_product_master
# MAGIC )
# MAGIC
# MAGIC , price_change as (
# MAGIC select 
# MAGIC explode(sequence(
# MAGIC     case when to_date(start_date) <= to_date(end_date) then to_date(start_date) else null end,
# MAGIC     case when to_date(start_date) <= to_date(end_date) then to_date(end_date) else null end, 
# MAGIC     interval 1 day)) AS ds,
# MAGIC trim(lower(banner)) as banner,
# MAGIC trim(lower(dp_name)) as dp_name,
# MAGIC relaunch,
# MAGIC activity_type,
# MAGIC price_change
# MAGIC from mt_predicted_order.mt_price_change
# MAGIC )
# MAGIC
# MAGIC , price_change_final as (
# MAGIC select *
# MAGIC from price_change
# MAGIC )
# MAGIC
# MAGIC -- promotion
# MAGIC , promotion_explode as (
# MAGIC select
# MAGIC explode(sequence(
# MAGIC     case when to_date(order_start_date) <= to_date(order_end_date) then to_date(order_start_date) else null end,
# MAGIC     case when to_date(order_start_date) <= to_date(order_end_date) then to_date(order_end_date) else null end, 
# MAGIC     interval 1 day)) AS ds,
# MAGIC trim(lower(banner)) as banner,
# MAGIC trim(lower(dp_name)) as dp_name,
# MAGIC cast(ulv_code as bigint) as ulv_code,
# MAGIC
# MAGIC category,
# MAGIC post_name,
# MAGIC code_type,
# MAGIC on_off_post,
# MAGIC promotion_type,
# MAGIC promotion_grouping,
# MAGIC gift_type,
# MAGIC gift_value,
# MAGIC tts,
# MAGIC bmi,
# MAGIC actual_tts,
# MAGIC actual_bmi
# MAGIC
# MAGIC
# MAGIC from mt_predicted_order.mt_promotion
# MAGIC )
# MAGIC , promotion_final as (
# MAGIC select *
# MAGIC from promotion_explode
# MAGIC )
# MAGIC
# MAGIC -- sale
# MAGIC , sale as (
# MAGIC select 
# MAGIC -- primary key
# MAGIC to_date(billing_date) as ds,
# MAGIC lower(banner) as banner,
# MAGIC region,
# MAGIC trim(lower(dp_name)) as dp_name,
# MAGIC
# MAGIC cast(ulv_code as bigint) as ulv_code,
# MAGIC cast(est_demand_cs as bigint) as est_demand,
# MAGIC cast(order_cs as bigint) as order_cs,
# MAGIC cast(u_price_cs as bigint) as u_price_cs,
# MAGIC cast(key_in_after_value as bigint) as key_in_after_value
# MAGIC from mt_predicted_order.mt_sale
# MAGIC )
# MAGIC
# MAGIC , sale_final as (
# MAGIC select
# MAGIC -- primary key 
# MAGIC ds,
# MAGIC banner,
# MAGIC region,
# MAGIC dp_name,
# MAGIC ulv_code,
# MAGIC coalesce(est_demand, 0) as est_demand,
# MAGIC order_cs,
# MAGIC u_price_cs,
# MAGIC key_in_after_value
# MAGIC from sale
# MAGIC )
# MAGIC , final as (
# MAGIC select
# MAGIC -- primary key 
# MAGIC df.banner,
# MAGIC df.region,
# MAGIC df.dp_name,
# MAGIC concat_ws("_", year, month, dayofmonth) as ds_freq,
# MAGIC
# MAGIC first(df.ds) as ds,
# MAGIC -- sale
# MAGIC avg(u_price_cs) as u_price_cs,
# MAGIC sum(order_cs) as order_cs,
# MAGIC sum(key_in_after_value) as key_in_after_value,
# MAGIC sum(est_demand) as est_demand,
# MAGIC
# MAGIC -- product information
# MAGIC first(class) as class,
# MAGIC first(brand) as brand,
# MAGIC first(pi.packsize) as packsize,
# MAGIC first(pi.packgroup) as packgroup,
# MAGIC first(pi.brand_variant) as brand_variant,
# MAGIC first(pi.category) as category,
# MAGIC first(pi.format) as format,
# MAGIC first(pi.segment) as segment,
# MAGIC first(pi.subbrand) as subbrand,
# MAGIC first(pi.production_segment) as production_segment,
# MAGIC
# MAGIC -- price change
# MAGIC first(relaunch) as relaunch,
# MAGIC first(activity_type) as activity_type,
# MAGIC first(price_change) as price_change,
# MAGIC
# MAGIC -- promotion
# MAGIC concat_ws(", ", collect_list(post_name)) as post_name,
# MAGIC concat_ws(", ", collect_list(code_type)) as code_type,
# MAGIC concat_ws(", ", collect_list(on_off_post)) as on_off_post,
# MAGIC concat_ws(", ", collect_list(promotion_type)) as promotion_type,
# MAGIC concat_ws(", ", collect_list(promotion_grouping)) as promotion_grouping,
# MAGIC concat_ws(", ", collect_list(gift_type)) as gift_type,
# MAGIC avg(gift_value) as gift_value,
# MAGIC avg(tts) as tts,
# MAGIC avg(bmi) as bmi,
# MAGIC avg(actual_tts) as actual_tts,
# MAGIC avg(actual_bmi) as actual_bmi,
# MAGIC
# MAGIC -- calendar
# MAGIC avg(year) as year,
# MAGIC avg(quarter) as quarter,
# MAGIC avg(month) as month,
# MAGIC avg(dayofmonth) as dayofmonth,
# MAGIC avg(dayofweek) as dayofweek,
# MAGIC first(weekend) as weekend,
# MAGIC avg(dayofyear) as dayofyear,
# MAGIC avg(weekofyear) as weekofyear,
# MAGIC
# MAGIC first(case when is_year_start = true then true else null end) as is_year_start,
# MAGIC first(case when is_year_end = true then true else null end) as is_year_end,
# MAGIC first(case when is_quarter_start = true then true else null end) as is_quarter_start,
# MAGIC first(case when is_quarter_end = true then true else null end) as is_quarter_end,
# MAGIC first(case when is_month_start = true then true else null end) as is_month_start,
# MAGIC first(case when is_month_end = true then true else null end) as is_month_end,
# MAGIC first(case when is_leap_year = true then true else null end) as is_leap_year,
# MAGIC
# MAGIC coalesce(any_value(case when vn_holiday != 'Non-holiday' then vn_holiday end), 'Non-holiday') as vn_holiday,
# MAGIC coalesce(any_value(case when us_holiday != 'Non-holiday' then us_holiday end), 'Non-holiday') as us_holiday,
# MAGIC
# MAGIC avg(lunar_year) as lunar_year,
# MAGIC avg(lunar_month) as lunar_month,
# MAGIC avg(lunar_day) as lunar_day,
# MAGIC first(season) as season
# MAGIC
# MAGIC
# MAGIC from dates_final df
# MAGIC left join sale_final sf on df.ds = sf.ds and df.region = sf.region and df.dp_name = sf.dp_name and df.banner = sf.banner
# MAGIC left join product_information pi on sf.ulv_code = pi.ulv_code
# MAGIC left join price_change_final pcf on df.ds = pcf.ds and df.banner = pcf.banner
# MAGIC left join promotion_final pf on df.ds = pf.ds and df.banner = pf.banner and df.dp_name = pf.dp_name
# MAGIC left join calendar_final cf on df.ds = cf.ds
# MAGIC where 1=1
# MAGIC group by 1, 2, 3, 4
# MAGIC )
# MAGIC , filter_top_sku as (
# MAGIC select
# MAGIC dp_name,
# MAGIC sum(est_demand) as sum_est_demand
# MAGIC from final 
# MAGIC group by 1
# MAGIC order by 2 desc 
# MAGIC limit 100
# MAGIC )
# MAGIC
# MAGIC select 
# MAGIC f.*
# MAGIC from final f
# MAGIC inner join filter_top_sku fts  on f.dp_name = fts.dp_name
# MAGIC where 1=1
# MAGIC and ds >= '2023-06-01'
# MAGIC and (banner = "saigon coop")
# MAGIC

# COMMAND ----------

_sqldf.cache().createOrReplaceTempView("data_mpo_final")

# COMMAND ----------

# MAGIC %md
# MAGIC # Feature Engineering

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Feature engineering
# MAGIC select 
# MAGIC ds,
# MAGIC banner,
# MAGIC region,
# MAGIC dp_name,
# MAGIC
# MAGIC -- sale
# MAGIC u_price_cs,
# MAGIC order_cs,
# MAGIC key_in_after_value,
# MAGIC est_demand,
# MAGIC
# MAGIC -- product information
# MAGIC class,
# MAGIC brand,
# MAGIC packsize,
# MAGIC packgroup,
# MAGIC brand_variant,
# MAGIC category,
# MAGIC format,
# MAGIC segment,
# MAGIC subbrand,
# MAGIC production_segment,
# MAGIC
# MAGIC -- price change
# MAGIC relaunch,
# MAGIC activity_type,
# MAGIC price_change,
# MAGIC
# MAGIC -- promotion
# MAGIC post_name,
# MAGIC code_type,
# MAGIC on_off_post,
# MAGIC promotion_type,
# MAGIC promotion_grouping,
# MAGIC gift_type,
# MAGIC gift_value,
# MAGIC tts,
# MAGIC bmi,
# MAGIC actual_tts,
# MAGIC actual_bmi,
# MAGIC
# MAGIC -- calendar
# MAGIC year,
# MAGIC quarter,
# MAGIC month,
# MAGIC dayofmonth,
# MAGIC dayofweek,
# MAGIC weekend,
# MAGIC dayofyear,
# MAGIC weekofyear,
# MAGIC
# MAGIC is_year_start,
# MAGIC is_year_end,
# MAGIC is_quarter_start,
# MAGIC is_quarter_end,
# MAGIC is_month_start,
# MAGIC is_month_end,
# MAGIC
# MAGIC is_leap_year,
# MAGIC vn_holiday,
# MAGIC us_holiday,
# MAGIC
# MAGIC lunar_year,
# MAGIC lunar_month,
# MAGIC lunar_day,
# MAGIC season,
# MAGIC
# MAGIC
# MAGIC -- Feature Engineering
# MAGIC -- est demand
# MAGIC sum(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 1 preceding and 1 preceding) as sum_est_demand_d1,
# MAGIC
# MAGIC sum(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 7 preceding and 1 preceding) as sum_est_demand_d7,
# MAGIC avg(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 7 preceding and 1 preceding) as avg_est_demand_d7,
# MAGIC min(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 7 preceding and 1 preceding) as min_est_demand_d7,
# MAGIC max(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 7 preceding and 1 preceding) as max_est_demand_d7,
# MAGIC
# MAGIC sum(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 30 preceding and 1 preceding) as sum_est_demand_d30,
# MAGIC avg(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 30 preceding and 1 preceding) as avg_est_demand_d30,
# MAGIC min(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 30 preceding and 1 preceding) as min_est_demand_d30,
# MAGIC max(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 30 preceding and 1 preceding) as max_est_demand_d30,
# MAGIC
# MAGIC sum(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 180 preceding and 1 preceding) as sum_est_demand_d180,
# MAGIC avg(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 180 preceding and 1 preceding) as avg_est_demand_d180,
# MAGIC min(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 180 preceding and 1 preceding) as min_est_demand_d180,
# MAGIC max(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 180 preceding and 1 preceding) as max_est_demand_d180,
# MAGIC
# MAGIC sum(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 365 preceding and 1 preceding) as sum_est_demand_d365,
# MAGIC avg(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 365 preceding and 1 preceding) as avg_est_demand_d365,
# MAGIC min(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 365 preceding and 1 preceding) as min_est_demand_d365,
# MAGIC max(dmf.est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 365 preceding and 1 preceding) as max_est_demand_d365,
# MAGIC
# MAGIC ---- explanding est_demand
# MAGIC sum(est_demand) over (partition by banner, region, dp_name order by unix_date(ds) range between unbounded preceding and 1 preceding) as expanding_sum_est_demand,
# MAGIC avg(est_demand) over (partition by banner, region, dp_name order by unix_date(ds) range between unbounded preceding and 1 preceding) as expanding_avg_est_demand,
# MAGIC max(est_demand) over (partition by banner, region, dp_name order by unix_date(ds) range between unbounded preceding and 1 preceding) as expanding_max_est_demand,
# MAGIC count(case when est_demand > 0 then 1 end) over (partition by banner, region, dp_name order by unix_date(ds) range between unbounded preceding and 1 preceding) as expanding_countnonzero_est_demand,
# MAGIC
# MAGIC -- promotion
# MAGIC -- -- post_name
# MAGIC count(post_name) over (partition by banner, region, dp_name  order by unix_date(ds) range between 7 preceding and 1 preceding) as rolling_count_post_name_d7,
# MAGIC
# MAGIC count(post_name) over (partition by banner, region, dp_name  order by unix_date(ds) range between 30 preceding and 1 preceding) as rolling_count_post_name_d30,
# MAGIC
# MAGIC count(post_name) over (partition by banner, region, dp_name  order by unix_date(ds) range between 180 preceding and 1 preceding) as rolling_count_post_name_d180,
# MAGIC -- -- tts
# MAGIC sum(tts) over (partition by banner, region, dp_name  order by unix_date(ds) range between 7 preceding and 1 preceding) as rolling_sum_tts_d7,
# MAGIC avg(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 7 preceding and 1 preceding) as rolling_avg_tts_d7,
# MAGIC min(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 7 preceding and 1 preceding) as rolling_min_tts_d7,
# MAGIC max(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 7 preceding and 1 preceding) as rolling_max_tts_d7,
# MAGIC sum(case when tts > 0 then 1 end) over (partition by banner, region, dp_name  order by unix_date(ds) range between 7 preceding and 1 preceding) as rolling_count_tts_d7,
# MAGIC
# MAGIC sum(tts) over (partition by banner, region, dp_name  order by unix_date(ds) range between 30 preceding and 1 preceding) as rolling_sum_tts_d30,
# MAGIC avg(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 30 preceding and 1 preceding) as rolling_avg_tts_d30,
# MAGIC min(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 30 preceding and 1 preceding) as rolling_min_tts_d30,
# MAGIC max(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 30 preceding and 1 preceding) as rolling_max_tts_d30,
# MAGIC sum(case when tts > 0 then 1 end) over (partition by banner, region, dp_name  order by unix_date(ds) range between 30 preceding and 1 preceding) as rolling_count_tts_d30,
# MAGIC
# MAGIC sum(tts) over (partition by banner, region, dp_name  order by unix_date(ds) range between 180 preceding and 1 preceding) as rolling_sum_tts_d180,
# MAGIC avg(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 180 preceding and 1 preceding) as rolling_avg_tts_d180,
# MAGIC min(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 180 preceding and 1 preceding) as rolling_min_tts_d180,
# MAGIC max(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 180 preceding and 1 preceding) as rolling_max_tts_d180,
# MAGIC sum(case when tts > 0 then 1 end) over (partition by banner, region, dp_name  order by unix_date(ds) range between 180 preceding and 1 preceding) as rolling_count_tts_d180
# MAGIC from data_mpo_final dmf

# COMMAND ----------

_sqldf.write.mode("overwrite").saveAsTable("mt_feature_store.data_daily_fe")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Library

# COMMAND ----------

# Import Neccessary Library
## python native library
import os
import re
import sys
from datetime import date, datetime
import pendulum
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

# Sklearn
from sklearn.pipeline import Pipeline 
from sklearn.compose import ColumnTransformer

# Preprocessing
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, LabelEncoder, OrdinalEncoder

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

# mlflow
import mlflow
from mlflow import MlflowClient
from mlflow.models import infer_signature
## System metrics
os.environ["MLFLOW_ENABLE_SYSTEM_METRICS_LOGGING"] = "true"
mlflow.enable_system_metrics_logging()

from typing import Optional, Union, List


# COMMAND ----------

# mlflow.end_run()
# mlflow.start_run(run_name )
# mlflowClient: MlflowClient = MlflowClient()
# experiment_name = "/Users/dao-minh.dao@unilever.com/daily-mpi-forecasting"
# experiment_id = mlflowClient.create_experiment(experiment_name)
# # Start a new run in the experiment
# run = client.create_run(experiment_id)
# run_id = run.info.run_id

# COMMAND ----------

path_data = Path("/dbfs/mnt/adls/dev")
path_data.mkdir(parents = True, exist_ok = True)
df_final.to_parquet(path_data.joinpath("mt_predicted_order.parquet"), index = False)
if path_data.exists():
  df_final = pd.read_parquet(path_data.joinpath("mt_predicted_order.parquet"))
else:
  df_final.to_parquet(path_data.joinpath("mt_predicted_order.parquet"), index = False)
df_final.head()

# COMMAND ----------

df_final.shape

# COMMAND ----------

# Define variable
label = "est_demand"
mlflow.set_tag('mlflow.runName', f"MPO: {pendulum.now('Asia/Saigon').to_datetime_string()}")
mlflow.log_param("label", label)
print(f"""
Number of unique: {df_final[label].nunique()}   
""")
# df_final[label].fillna(0, inplace = True)
# sns.displot(data = df_final, x = label)


# COMMAND ----------

display(df_final)


# COMMAND ----------

# banner = "saigon coop"
# dp_name = "cf gold aroma 1-rinse 800ml"
# region = "V101"
# data = df_final.query(f"(banner == '{banner}' & region == '{region}' & dp_name == '{dp_name}')")
# sns.lineplot(data = data, x = "ds", y = "est_demand")

# COMMAND ----------

df_final = df_final.dropna(how = "all", subset=[label])
df_final = df_final.sort_values(by = ["ds"])
df_final = df_final.reset_index()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Test Validation

# COMMAND ----------

# Train Test Validation

sample_size = df_final.shape[0]
test_size= 0.2
df_train, df_test = df_final.iloc[:int(sample_size*(1- test_size))], df_final.iloc[int(sample_size*(1-test_size)):]
mlflow.log_param("train_size", df_train.shape[0])
mlflow.log_param("test_size", df_test.shape[0])

print(f"""
train:  
  size: {df_train.shape} 
test:
  size: {df_test.shape}
""")


# COMMAND ----------

numerical_features = []

categorical_features = []
for col in df_final.columns:
  try:
    pd.to_numeric(df_final[col])
    numerical_features.append(col)
  except:
    categorical_features.append(col)
    
print("categorical_features features")
for col in numerical_features:
  print(f'"{col}",')

print("categorical features")
for col in categorical_features:
  print(f'"{col}",')

# COMMAND ----------

numerical_columns = [

# Sale


# Promotion
"u_price_cs",
# "key_in_after_value",
"price_change",
"gift_value",
"tts",
"bmi",
"actual_tts",
"actual_bmi",


# Calendar
"year",
"quarter",
"month",
"dayofmonth",
"dayofweek",
"weekend",
"dayofyear",
"weekofyear",
"is_year_start",
"is_year_end",
"is_quarter_start",
"is_quarter_end",
"is_month_start",
"is_month_end",
"is_leap_year",
## Lunar date
"lunar_year",
"lunar_month",
"lunar_day",

# Feature Engineering
"sum_est_demand_d1",
"sum_est_demand_d7",
"avg_est_demand_d7",
"min_est_demand_d7",
"max_est_demand_d7",
"sum_est_demand_d30",
"avg_est_demand_d30",
"min_est_demand_d30",
"max_est_demand_d30",
"sum_est_demand_d180",
"avg_est_demand_d180",
"min_est_demand_d180",
"max_est_demand_d180",
"sum_est_demand_d365",
"avg_est_demand_d365",
"min_est_demand_d365",
"max_est_demand_d365",
"expanding_sum_est_demand",
"expanding_avg_est_demand",
"expanding_max_est_demand",
"expanding_countnonzero_est_demand",
"rolling_count_post_name_d7",
"rolling_count_post_name_d30",
"rolling_count_post_name_d180",
"rolling_sum_tts_d7",
"rolling_avg_tts_d7",
"rolling_min_tts_d7",
"rolling_max_tts_d7",
"rolling_count_tts_d7",
"rolling_sum_tts_d30",
"rolling_avg_tts_d30",
"rolling_min_tts_d30",
"rolling_max_tts_d30",
"rolling_count_tts_d30",
"rolling_sum_tts_d180",
"rolling_avg_tts_d180",
"rolling_min_tts_d180",
"rolling_max_tts_d180",
"rolling_count_tts_d180",
]

categorical_columns = [
"banner",
"region",
"dp_name",
"class",
"brand",
"packsize",
"packgroup",
"brand_variant",
"category",
"format",
"segment",
"subbrand",
"production_segment",
"on_off_post",
"promotion_type",
"promotion_grouping",
"gift_type",
"code_type",
"post_name",
"vn_holiday",
"us_holiday",
"season",
]

# assert(len(set(df_final.columns.tolist()).intersection(set(numerical_columns + categorical_columns))) == len(numerical_columns + categorical_columns))
if len(set(df_final.columns.tolist()).intersection(set(numerical_columns + categorical_columns))) !=len(numerical_columns + categorical_columns):
  for col in numerical_columns + categorical_columns:
    if col not in df_final.columns:
      print(col)

# COMMAND ----------

# Model pipeline

numerical_preprocessing = Pipeline(steps = [
  ("imputer", SimpleImputer()),
])

categorical_preprocessing = Pipeline(steps = [
  ("imputer", SimpleImputer(strategy="most_frequent")),
  ("encoder", OrdinalEncoder(handle_unknown = "use_encoded_value", unknown_value = -1)),
  # ("encoder", OneHotEncoder(handle_unknown = "ignore"))
])

combine_preprocessing = ColumnTransformer(transformers=[
        ("numerical", numerical_preprocessing, numerical_columns),
        ("categorial", categorical_preprocessing, categorical_columns)
    ],
                                          verbose_feature_names_out = False,
                                          n_jobs=-1
                                          )
rf = RandomForestRegressor(random_state = 2024, n_jobs = -1)
lgb = LGBMRegressor(
  categorical_features = [i for i in range(len(numerical_columns), len(numerical_columns) + len(categorical_columns))],
  n_jobs = -1
)
mlflow.log_param("model_name", "lgbm")
pipe = Pipeline(steps = [
  ("combine", combine_preprocessing),
  ("estimator", lgb),
])
pipe

# COMMAND ----------

cv_results = cross_validate(
  estimator=pipe,
  X = df_train.loc[:, numerical_columns + categorical_columns],
  y = df_train[label],
  scoring = {
    "rmse": "neg_root_mean_squared_error",
    "mae": "neg_mean_absolute_error",
    "mape": "neg_mean_absolute_percentage_error",
  },
  cv = TimeSeriesSplit(n_splits=2),
  return_estimator=True,
  return_train_score=True,
  error_score="raise",
  n_jobs = -1
)

# COMMAND ----------

df_cv = {
  "0_model": "CV_LGBM"
}

for key, value in cv_results.items():
  if re.search("(train|test)", key):
    cv_results[key] = np.mean(value)
    df_cv[key] = np.round(np.mean(value), 2)
    mlflow.log_metric(key, df_cv[key]) 

pd.DataFrame.from_records([df_cv]).sort_index(axis = 1, ascending = True).style.background_gradient(axis="rows")  

# COMMAND ----------

best_pipe = cv_results["estimator"][-1]

ft = pd.DataFrame()
# best_pipe = cv_results["estimator"][np.argmax(cv_results["test_auc"])]
best_pipe = cv_results["estimator"][-1]
# Log model
signature = infer_signature(df_test.loc[:, numerical_columns + categorical_columns], best_pipe.predict(df_test.loc[:, numerical_columns + categorical_columns]))
mlflow.sklearn.log_model(sk_model = best_pipe, artifact_path="model", signature = signature)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

ft["features"] = best_pipe[-2].get_feature_names_out()
ft["feature_importances"] = best_pipe[-1].feature_importances_

(
so
 .Plot(ft.sort_values(by = ["feature_importances"], ascending=False), y = "features", x = "feature_importances")
 .add( so.Bars())
 .layout(size=(8, 10))
)

# COMMAND ----------

df_train["prediction"] = best_pipe.predict(df_train.loc[:, numerical_columns + categorical_columns])
df_test["prediction"] = best_pipe.predict(df_test.loc[:, numerical_columns + categorical_columns])

# COMMAND ----------

# MAGIC %md
# MAGIC ## TS Prediction

# COMMAND ----------

def dropdown(fig, list_key, visibles, title = 'Tonne'):
    buttons=[]
    buttons.append(dict(method='update',
                            label="ALL",
                            args = [{'visible': [True for vs in visibles]},
                                    {'showlegend' : True} 
                                   ]
                           )
    )
    for key in list_key:
        buttons.append(dict(method='update',
                            label=key,
                            args = [{'visible': [key==vs for vs in visibles]},
                                    {'showlegend' : True} 
                                   ]
                           ))

    fig.update_layout( 
        updatemenus=[ 
            dict( 
            direction="down", 
            pad={"r": 10, "t": 10},
            showactive=True, 
            x=0.0, 
            xanchor="left",
            y=1.13, 
            yanchor="top",
            font = dict(color = 'Indigo',size = 14),
            buttons=buttons
            )])

    fig.update_layout(title = title,
                      title_x = 0.5,
                      title_font = dict(size = 20, color = 'MidnightBlue')
                     )
    
    return fig


dp_names = [
  'wall cup chocolate 55g',
 'wall cup chocolate 55g',
 'cornetto standard chocoberry 66g',
 'cornetto standard chocoberry 66g',
 'wall cup chocolate 55g',
 "wall asian taro 67g",
]
data_train = df_train
data_train["split"] = "train"
data_test = df_test
data_test["split"] = "test"

data = pd.concat([data_train, data_test], axis = 0)
data = data.loc[data["dp_name"].isin(dp_names)]
data["symbol"] = data['banner'] + '-' + data['region'] + '-' + data['dp_name']

visibles = []
fig = go.Figure()
for symbol in data.symbol.unique():
  symbol_data = data[data["symbol"] == symbol]
  for split in symbol_data["split"].unique():
    split_data = symbol_data[symbol_data["split"] == split]
        
    # Actual Values Trace
    fig.add_trace(go.Scatter(x=split_data["ds"], y=split_data[label],
                              mode='lines', name=f"{split} Actual",
                              line=dict(color="blue" if split == "train" else "blue"),
                              legendgroup=f"{symbol}-{split}", showlegend=True))
    
    # Prediction Trace (ensure your data contains a 'prediction' column)
    fig.add_trace(go.Scatter(x=split_data["ds"], y=split_data["prediction"],
                              mode='lines', name=f"{split} Predicted",
                              line=dict(color="goldenrod" if split == "train" else "red"),
                              legendgroup=f"{symbol}-{split}", showlegend=True))
    
    visibles.extend([symbol] * 2)
dropdown(fig, sorted(list(set(visibles))), visibles, title ="Demand sensing MT Predicted Order")
fig.show()

# COMMAND ----------

data["split"].unique()

# COMMAND ----------

data.head()

# COMMAND ----------


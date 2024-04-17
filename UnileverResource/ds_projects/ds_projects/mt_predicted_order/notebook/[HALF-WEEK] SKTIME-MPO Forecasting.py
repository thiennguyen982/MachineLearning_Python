# Databricks notebook source
# DBTITLE 1,Import neccessary Library
# Import Neccessary Library
## python native library
import os
import re
import sys
import time
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
import sklearn
import sklearn.pipeline
import sklearn.compose
from sklearn.base import BaseEstimator, TransformerMixin, RegressorMixin, ClassifierMixin
from sklearn.pipeline import Pipeline 
from sklearn.compose import ColumnTransformer, TransformedTargetRegressor
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, LabelEncoder, OrdinalEncoder, RobustScaler, FunctionTransformer
from sklearn.model_selection import train_test_split, TimeSeriesSplit, cross_val_predict, cross_validate, GridSearchCV, RandomizedSearchCV

# Tree base model
from sklearn.ensemble import RandomForestRegressor
from lightgbm.sklearn import LGBMRegressor
from xgboost.sklearn import XGBRFRegressor, XGBRegressor
from catboost import CatBoostRegressor

# statsmodels
# ACF and PACF
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf

# Sktime
import sktime
from sktime.transformations.series.boxcox import BoxCoxTransformer, LogTransformer
from sktime.transformations.series.cos import CosineTransformer
from sktime.transformations.series.exponent import ExponentTransformer, SqrtTransformer
from sktime.forecasting.compose import make_reduction
from sklearn.neighbors import KNeighborsRegressor
from sktime.utils.plotting import plot_series
from sktime.forecasting.arima import ARIMA
from sktime.forecasting.compose import TransformedTargetForecaster, ForecastingPipeline
from sktime.transformations.series.detrend import Deseasonalizer
from lightgbm.sklearn import LGBMRegressor
from sktime.transformations.series.date import DateTimeFeatures
from sktime.transformations.series.summarize import WindowSummarizer
from sktime.forecasting.model_evaluation import evaluate
from sktime.split import SlidingWindowSplitter, SingleWindowSplitter
from sktime.performance_metrics.forecasting import MeanAbsoluteError, make_forecasting_scorer, MeanAbsolutePercentageError


# mlflow
import mlflow
from mlflow import MlflowClient
from mlflow.models import infer_signature
## System metrics
os.environ["MLFLOW_ENABLE_SYSTEM_METRICS_LOGGING"] = "true"
mlflow.enable_system_metrics_logging()

from typing import Optional, Union, List, Dict, Any
from joblibspark import register_spark

register_spark()


# Utils function
MOUNT_DS_PROJECT = "/Workspace/Repos/dao-minh.toan@unilever.com/ds_projects/"
sys.path.append(MOUNT_DS_PROJECT)
from mt_predicted_order.common.sklearn import fa_avg_scorer, fa_sumup_scorer, fa_avg_score, fa_sumup_score, TimeSeriesFE
from mt_predicted_order.common.mlflow import MlflowService
from mt_predicted_order.common.logger import logger

# Setup SparkSession
spark: SparkSession = SparkSession.builder \
    .appName("HALF_WEEK_MPO_FORECASTING") \
    .getOrCreate()

# Setup MLflow Experiment
EXPERIMENT_NAME = "/Users/dao-minh.toan@unilever.com/HALF_WEEK_MPO_FORECASTING"
mlflowService: MlflowService = MlflowService(EXPERIMENT_NAME)


print(f"""
VERSION:    
    spark: {spark.version}
    sklearn: {sklearn.__version__}
    sktime: {sktime.__version__}
""")

# COMMAND ----------

# DBTITLE 1,Data Preparation
# MAGIC %sql
# MAGIC -- Timeseries: ds
# MAGIC -- Customer: Banner, region,
# MAGIC -- Product: ulv_code
# MAGIC -- sale
# MAGIC with core_skus as (
# MAGIC select 
# MAGIC distinct
# MAGIC trim(lower(banner)) as banner,
# MAGIC trim(lower(dp_name)) as dp_name
# MAGIC from mt_predicted_order.core_skus
# MAGIC )
# MAGIC , sale as (
# MAGIC select
# MAGIC -- primary key
# MAGIC to_date(billing_date) as billing_date,
# MAGIC trim(lower(sale.banner)) as banner,
# MAGIC trim(lower(region)) as region,
# MAGIC cast(ulv_code as bigint) as ulv_code,
# MAGIC cast(est_demand_cs as bigint) as est_demand,
# MAGIC cast(order_cs as bigint) as order_cs,
# MAGIC cast(u_price_cs as bigint) as u_price_cs,
# MAGIC -- at t: ready?
# MAGIC cast(key_in_after_value as bigint) as key_in_after_value,
# MAGIC cast(billing_cs as bigint) as billing_cs
# MAGIC from mt_predicted_order.mt_sale sale
# MAGIC where to_date(billing_date) >= to_date("2022-01-03", 'yyyy-MM-dd')
# MAGIC and trim(lower(sale.dp_name)) is not null
# MAGIC and trim(lower(sale.dp_name)) != ''
# MAGIC and trim(lower(sale.banner)) in ("saigon coop", "big_c")
# MAGIC
# MAGIC )
# MAGIC , sale_final as (
# MAGIC select
# MAGIC -- primary key
# MAGIC billing_date,
# MAGIC banner,
# MAGIC region,
# MAGIC ulv_code,
# MAGIC sum(est_demand) as est_demand,
# MAGIC sum(order_cs) as order_cs,
# MAGIC avg(u_price_cs) as u_price_cs,
# MAGIC sum(billing_cs) as actual_sale
# MAGIC from sale
# MAGIC group by 1, 2, 3, 4
# MAGIC
# MAGIC )
# MAGIC
# MAGIC -- Calendar
# MAGIC , calendar as (
# MAGIC select
# MAGIC distinct
# MAGIC to_date(date) as billing_date,
# MAGIC year,
# MAGIC quarter,
# MAGIC month,
# MAGIC dayofmonth,
# MAGIC dayofweek + 1 as dayofweek,
# MAGIC case when dayofweek + 1 in (1, 2, 3) then "first_half" else "last_half" end as half_week,
# MAGIC weekend,
# MAGIC dayofyear,
# MAGIC weekofyear,
# MAGIC yearweek,
# MAGIC is_year_start,
# MAGIC is_year_end,
# MAGIC is_quarter_start,
# MAGIC is_quarter_end,
# MAGIC is_month_start,
# MAGIC is_month_end,
# MAGIC is_leap_year,
# MAGIC vn_holiday,
# MAGIC us_holiday,
# MAGIC lunar_date,
# MAGIC lunar_year,
# MAGIC lunar_month,
# MAGIC lunar_day,
# MAGIC season
# MAGIC from mt_predicted_order.mt_calendar_master
# MAGIC where to_date(date) >= to_date("2022-01-03", 'yyyy-MM-dd')
# MAGIC ),
# MAGIC calendar_final as (
# MAGIC select
# MAGIC *
# MAGIC from calendar
# MAGIC ),
# MAGIC product_information as (
# MAGIC select
# MAGIC distinct
# MAGIC cast(ulv_code as bigint) as ulv_code,
# MAGIC trim(lower(dp_name)) as dp_name,
# MAGIC ph3_desc,
# MAGIC ph7_desc,
# MAGIC pack_size,
# MAGIC pcs_per_cs,
# MAGIC net_weight_kg_per_cs,
# MAGIC ph9,
# MAGIC code_type,
# MAGIC gross_weight_kg_per_cs,
# MAGIC ph8,
# MAGIC class,
# MAGIC trim(lower(category)) as category,
# MAGIC short_division,
# MAGIC segment,
# MAGIC brand
# MAGIC
# MAGIC from mt_predicted_order.mt_product
# MAGIC )
# MAGIC , price_change as (
# MAGIC select
# MAGIC distinct
# MAGIC to_date(billing_date) as billing_date,
# MAGIC trim(lower(banner)) as banner,
# MAGIC trim(lower(dp_name)) as dp_name,
# MAGIC relaunch,
# MAGIC activity_type,
# MAGIC price_change
# MAGIC from mt_predicted_order.mt_price_change
# MAGIC where to_date(billing_date) >= to_date("2022-01-03", 'yyyy-MM-dd')
# MAGIC )
# MAGIC , price_change_final as (
# MAGIC select
# MAGIC     *
# MAGIC from price_change
# MAGIC ) 
# MAGIC -- key event
# MAGIC , key_event_melt as (
# MAGIC select 
# MAGIC year,
# MAGIC trim(lower(banner)) as banner,
# MAGIC trim(lower(post_name)) as post_name,
# MAGIC trim(lower(key_event_name)) as key_event_name,
# MAGIC replace(trim(lower(kv_category)), "_", " ") as kv_category,
# MAGIC kv_value
# MAGIC from mt_predicted_order.mt_key_event
# MAGIC )
# MAGIC
# MAGIC -- promotion
# MAGIC , promotion as (
# MAGIC select
# MAGIC distinct
# MAGIC trim(lower(banner)) as banner,
# MAGIC trim(lower(dp_name)) as dp_name,
# MAGIC cast(ulv_code as bigint) as ulv_code,
# MAGIC trim(lower(category)) as category,
# MAGIC year,
# MAGIC trim(lower(post_name)) as post_name,
# MAGIC trim(lower(code_type)) as promotion_code_type,
# MAGIC trim(lower(on_off_post)) as on_off_post,
# MAGIC trim(lower(promotion_type)) as promotion_type,
# MAGIC trim(lower(promotion_grouping)) as promotion_grouping,
# MAGIC trim(lower(gift_type)) as gift_type,
# MAGIC cast(gift_value as float) as gift_value,
# MAGIC cast(tts as float) as tts,
# MAGIC cast(bmi as float) as bmi,
# MAGIC cast(actual_tts as float) as actual_tts,
# MAGIC cast(actual_bmi as float) as actual_bmi,
# MAGIC case
# MAGIC       when to_date(order_start_date) <= to_date(order_end_date) then to_date(order_start_date)
# MAGIC       else to_date(order_end_date) end as order_start_date,
# MAGIC  
# MAGIC case
# MAGIC       when to_date(order_start_date) <= to_date(order_end_date) then to_date(order_end_date)
# MAGIC       else to_date(order_start_date) end as order_end_date
# MAGIC from mt_predicted_order.mt_promotion
# MAGIC order by banner asc, order_start_date asc
# MAGIC  
# MAGIC )
# MAGIC , promotion_agg as (
# MAGIC select
# MAGIC banner,
# MAGIC ulv_code,
# MAGIC year,
# MAGIC post_name,
# MAGIC promotion_code_type,
# MAGIC first(category) category,
# MAGIC first(on_off_post, true) on_off_post,
# MAGIC first(promotion_type, true) promotion_type,
# MAGIC first(promotion_grouping, true) promotion_grouping,
# MAGIC first(gift_type, true) gift_type,
# MAGIC first(gift_value, true) gift_value,
# MAGIC first(tts, true) tts,
# MAGIC first(bmi, true) bmi,
# MAGIC first(actual_tts, true) actual_tts,
# MAGIC first(actual_bmi, true) actual_bmi,
# MAGIC min(order_start_date) order_start_date,
# MAGIC max(order_end_date) order_end_date
# MAGIC from promotion
# MAGIC group by 1,2,3,4,5
# MAGIC )
# MAGIC
# MAGIC , promotion_explode as (
# MAGIC select
# MAGIC explode(sequence(order_start_date, order_end_date, interval 1 day)) as billing_date,
# MAGIC banner,
# MAGIC ulv_code,
# MAGIC category,
# MAGIC post_name,
# MAGIC promotion_code_type,
# MAGIC on_off_post,
# MAGIC promotion_type,
# MAGIC promotion_grouping,
# MAGIC gift_type,
# MAGIC gift_value,
# MAGIC tts,
# MAGIC bmi,
# MAGIC actual_tts,
# MAGIC actual_bmi,
# MAGIC order_end_date,
# MAGIC order_start_date
# MAGIC from promotion_agg
# MAGIC where order_start_date >= to_date("2022-01-03", 'yyyy-MM-dd') 
# MAGIC and promotion_grouping is not null
# MAGIC )
# MAGIC
# MAGIC , promotion_transform as (
# MAGIC select
# MAGIC billing_date,
# MAGIC pe.banner,
# MAGIC ulv_code,
# MAGIC pe.post_name,
# MAGIC promotion_code_type,
# MAGIC coalesce(on_off_post, "off") on_off_post,
# MAGIC promotion_type,
# MAGIC promotion_grouping,
# MAGIC gift_type,
# MAGIC gift_value,
# MAGIC tts,
# MAGIC bmi,
# MAGIC actual_tts,
# MAGIC actual_bmi,
# MAGIC trim(lower(ke.kv_value)) as  key_event,
# MAGIC -- transform promotion
# MAGIC case
# MAGIC   when promotion_grouping rlike '.*hot price.*' then 'hot price'
# MAGIC   when promotion_grouping rlike '.*mnudl.*' then 'mnudl'
# MAGIC   when promotion_grouping rlike '.*pwp.*' then 'pwp'
# MAGIC   else 'unknown'
# MAGIC end as special_event,
# MAGIC -- check keyevent sheet from ML factor
# MAGIC coalesce(case when promotion_grouping RLIKE '.*(?<!u-)gift.*' then gift_type else 'no-gift' end, 'no-gift') as gift,
# MAGIC coalesce(case when promotion_grouping RLIKE 'u-gift' then gift_type else 'no-gift' end, 'no-gift') as ugift,
# MAGIC coalesce(case when promotion_grouping RLIKE 'discount - (normal|mnudl|pwd|hot price)' then promotion_grouping else 'no-discount' end, 'no-discount') as discount
# MAGIC  
# MAGIC from promotion_explode pe
# MAGIC left join key_event_melt ke on year(billing_date) = ke.year and pe.banner = ke.banner and pe.post_name = ke.post_name and pe.category = ke.kv_category
# MAGIC )
# MAGIC , promotion_final as (
# MAGIC select
# MAGIC billing_date,
# MAGIC banner,
# MAGIC ulv_code,
# MAGIC first(post_name) post_name,
# MAGIC first(promotion_code_type) promotion_code_type,
# MAGIC first(on_off_post) on_off_post,
# MAGIC first(promotion_type) promotion_type,
# MAGIC first(promotion_grouping) promotion_grouping,
# MAGIC first(gift_type) gift_type,
# MAGIC first(gift_value) gift_value,
# MAGIC first(tts) tts,
# MAGIC first(bmi) bmi,
# MAGIC first(actual_tts) actual_tts,
# MAGIC first(actual_bmi) actual_bmi,
# MAGIC first(special_event) special_event,
# MAGIC first(key_event) key_event,
# MAGIC first(gift) gift,
# MAGIC first(ugift) ugift,
# MAGIC first(discount) discount
# MAGIC
# MAGIC from promotion_transform
# MAGIC group by 1,2,3
# MAGIC ) 
# MAGIC
# MAGIC , date_ranges as (
# MAGIC select
# MAGIC -- primary key
# MAGIC sale_final.banner,
# MAGIC sale_final.region,
# MAGIC sale_final.ulv_code,
# MAGIC min(billing_date) as min_date,
# MAGIC max(billing_date) as max_date
# MAGIC from sale_final
# MAGIC group by 1, 2, 3
# MAGIC )
# MAGIC , dates_expanded AS (
# MAGIC SELECT
# MAGIC explode(sequence(to_date("2022-01-03", 'yyyy-MM-dd'), to_date("2024-03-17", 'yyyy-MM-dd'), INTERVAL 1 day)) AS billing_date,
# MAGIC banner,
# MAGIC region,
# MAGIC ulv_code,
# MAGIC min_date,
# MAGIC max_date
# MAGIC
# MAGIC FROM date_ranges
# MAGIC )
# MAGIC , dates_final as (
# MAGIC select
# MAGIC billing_date,
# MAGIC date_sub(billing_date, IF(dayofweek(billing_date)=1, 6, dayofweek(billing_date)-2)) AS weekly,
# MAGIC banner,
# MAGIC region,
# MAGIC ulv_code
# MAGIC from dates_expanded
# MAGIC where 1 = 1
# MAGIC ) 
# MAGIC , final as (
# MAGIC select
# MAGIC -- primary key
# MAGIC df.banner,
# MAGIC df.region,
# MAGIC pi.dp_name,
# MAGIC yearweek,
# MAGIC weekly as ds,
# MAGIC half_week,
# MAGIC concat_ws("|", df.banner, df.region, pi.dp_name, half_week) as unique_id,
# MAGIC
# MAGIC -- sale
# MAGIC coalesce(avg(u_price_cs), 0) as u_price_cs,
# MAGIC coalesce(sum(order_cs), 0) as order_cs,
# MAGIC coalesce(sum(est_demand), 0) as est_demand,
# MAGIC coalesce(sum(actual_sale), 0) as actual_sale,
# MAGIC
# MAGIC -- product information
# MAGIC -- [null, A, B, C] -> return A
# MAGIC first(pi.pack_size, true) as pack_size,
# MAGIC first(pi.ph3_desc, true) as ph3_desc,
# MAGIC first(pi.ph7_desc, true) as ph7_desc,
# MAGIC first(pi.pcs_per_cs, true) as pcs_per_cs,
# MAGIC first(pi.net_weight_kg_per_cs, true) as net_weight_kg_per_cs,
# MAGIC first(pi.ph9, true) as ph9,
# MAGIC first(pi.code_type, true) as code_type,
# MAGIC first(pi.gross_weight_kg_per_cs, true) as gross_weight_kg_per_cs,
# MAGIC first(pi.ph8, true) as ph8,
# MAGIC first(pi.class, true) as class,
# MAGIC first(pi.category, true) as category,
# MAGIC first(pi.short_division, true) as short_division,
# MAGIC first(pi.segment, true) as segment,
# MAGIC first(pi.brand, true) as brand,
# MAGIC
# MAGIC
# MAGIC -- PRICE CHANGE
# MAGIC coalesce(avg(relaunch), 0) as relaunch,
# MAGIC -- get max
# MAGIC coalesce(first(activity_type, true), "unknown") as activity_type,
# MAGIC --
# MAGIC coalesce(first(price_change, true), "unknown") as price_change,
# MAGIC -- get mean
# MAGIC -- promotion
# MAGIC coalesce(element_at(array_distinct(collect_list(post_name)), -1), "unknown")  as post_name,
# MAGIC -- get freq
# MAGIC coalesce(element_at(array_distinct(collect_list(promotion_code_type)), -1), "unknown") as promotion_code_type,
# MAGIC coalesce(element_at(array_distinct(collect_list(on_off_post)), -1), "unknown") as on_off_post,
# MAGIC coalesce(element_at(array_distinct(collect_list(promotion_type)), -1), "unknown") as promotion_type,
# MAGIC coalesce(element_at(array_distinct(collect_list(promotion_grouping)), -1), "unknown") as promotion_grouping,
# MAGIC coalesce(element_at(array_distinct(collect_list(gift_type)), -1), "unknown") as gift_type,
# MAGIC coalesce(avg(gift_value), 0) as gift_value,
# MAGIC coalesce(avg(tts), 0) as tts,
# MAGIC coalesce(avg(bmi), 0) as bmi,
# MAGIC coalesce(avg(actual_tts), 0) as actual_tts,
# MAGIC coalesce(avg(actual_bmi), 0) as actual_bmi,
# MAGIC -- transform promotion
# MAGIC coalesce(first(special_event, true), 'unknown') as special_event,
# MAGIC coalesce(first(key_event, true), 'unknown') as key_event,
# MAGIC -- get freq
# MAGIC coalesce(first(gift, true), "unknown") as gift,
# MAGIC coalesce(first(ugift, true), "unknown") as ugift,
# MAGIC coalesce(first(discount, true), "unknown") as discount,
# MAGIC
# MAGIC -- CALENDAR
# MAGIC avg(year) as year,
# MAGIC avg(quarter) as quarter,
# MAGIC avg(month) as month,
# MAGIC avg(dayofmonth) as dayofmonth,
# MAGIC avg(dayofweek) as dayofweek,
# MAGIC first(weekend, true) as weekend,
# MAGIC avg(dayofyear) as dayofyear,
# MAGIC avg(weekofyear) as weekofyear,
# MAGIC coalesce(first(case when is_year_start = true then true end, true), false) as is_year_start,
# MAGIC coalesce(first(case when is_year_end = true then true end, true), false) as is_year_end,
# MAGIC coalesce(first(case when is_quarter_start = true then true end, true), false) as is_quarter_start,
# MAGIC coalesce(first(case when is_quarter_end = true then true end, true), false) as is_quarter_end,
# MAGIC coalesce(first(case when is_month_start = true then true end, true), false) as is_month_start,
# MAGIC coalesce(first(case when is_month_end = true then true end, true), false) as is_month_end,
# MAGIC coalesce(first(case when is_leap_year = true then true end, true), false) as is_leap_year,
# MAGIC coalesce(first(case when vn_holiday != 'non-holiday' then vn_holiday end, true), 'non-holiday') as vn_holiday,
# MAGIC coalesce(first(case when us_holiday != 'non-holiday' then us_holiday end, true), 'non-holiday') as us_holiday,
# MAGIC avg(lunar_year) as lunar_year,
# MAGIC avg(lunar_month) as lunar_month,
# MAGIC avg(lunar_day) as lunar_day,
# MAGIC first(season, true) as season
# MAGIC
# MAGIC from dates_final df
# MAGIC left join sale_final sf on df.billing_date = sf.billing_date and df.banner = sf.banner and df.region = sf.region and df.ulv_code  = sf.ulv_code 
# MAGIC left join calendar_final as cal on df.billing_date = cal.billing_date
# MAGIC left join promotion_final pf on df.billing_date = pf.billing_date and df.banner = pf.banner and df.ulv_code = pf.ulv_code
# MAGIC left join product_information pi on df.ulv_code = pi.ulv_code
# MAGIC left join price_change_final pcf on df.billing_date = pcf.billing_date and df.banner = pcf.banner and pi.dp_name = pcf.dp_name
# MAGIC inner join core_skus cs on pi.dp_name = cs.dp_name and df.banner = cs.banner
# MAGIC where 1=1
# MAGIC group by 1, 2, 3, 4, 5, 6, 7
# MAGIC
# MAGIC )
# MAGIC , filter_zero_demand as (
# MAGIC select 
# MAGIC unique_id
# MAGIC from final 
# MAGIC where 1=1
# MAGIC -- and ds >= to_date("2023-01-02", 'yyyy-MM-dd')
# MAGIC group by 1
# MAGIC having sum(est_demand) > 0
# MAGIC )
# MAGIC , sale_halfweek as (
# MAGIC   select banner, region, dp_name, yearweek, half_week,  sum(est_demand) est_demand_test
# MAGIC   from sale_final sf
# MAGIC   left join product_information pi on sf.ulv_code = pi.ulv_code
# MAGIC   left join calendar_final cf on sf.billing_date = cf.billing_date
# MAGIC   group by 1,2,3,4,5
# MAGIC )
# MAGIC , final_check as (
# MAGIC select banner, region, dp_name, yearweek, half_week,  
# MAGIC sum(est_demand) est_demand,
# MAGIC min(ds) as ds
# MAGIC from final 
# MAGIC group by 1, 2, 3, 4, 5
# MAGIC )
# MAGIC
# MAGIC select 
# MAGIC *
# MAGIC from final df

# COMMAND ----------

# _sqldf.cache().createOrReplaceTempView("data_mpo_final")
_sqldf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("mt_feature_store.data_halfweek")

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC distinct
# MAGIC yearweek
# MAGIC from mt_predicted_order.mt_calendar_master
# MAGIC order by 1

# COMMAND ----------

# %sql
# -- Feature engineering
# with final as (
#   select
#     ds,
#     ds_freq,
#     concat_ws("|", banner, region, dp_name) as unique_id,
#     banner,
#     region,
#     dp_name,
#     -- sale
#     u_price_cs,
#     order_cs,
#     key_in_after_value,
#     coalesce(est_demand, 0) as est_demand,
#     actual_sale,
#     -- product information
#     class,
#     brand,
#     packsize,
#     packgroup,
#     brand_variant,
#     category,
#     format,
#     segment,
#     subbrand,
#     production_segment,
#     -- price change
#     relaunch,
#     activity_type,
#     price_change,
#     -- promotion
#     post_name,
#     code_type,
#     on_off_post,
#     promotion_type,
#     promotion_grouping,
#     gift_type,
#     gift_value,
#     tts,
#     bmi,
#     actual_tts,
#     actual_bmi,
#     special_event,
#     key_event,
#     gift,
#     ugift,
#     discount,
#     -- calendar
#     year,
#     quarter,
#     month,
#     dayofmonth,
#     dayofweek,
#     weekend,
#     dayofyear,
#     weekofyear,
#     is_year_start,
#     is_year_end,
#     is_quarter_start,
#     is_quarter_end,
#     is_month_start,
#     is_month_end,
#     is_leap_year,
#     vn_holiday,
#     us_holiday,
#     lunar_year,
#     lunar_month,
#     lunar_day,
#     season
    
#   from
#     data_mpo_final dmf
#     where ds >= to_date('2023-01-02', 'yyyy-MM-dd')
# )
# , filter_small_length as (
#   select 
#   banner,
#   region,
#   dp_name
#   from final
#   group by 1, 2, 3
#   having count(distinct ds) > 100
# )
# select
#   f.*
# from final f
# inner join filter_small_length fsl on f.banner = fsl.banner and f.region = fsl.region and f.dp_name = fsl.dp_name

# COMMAND ----------

# _sqldf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("mt_feature_store.data_halfweek_fe")

# COMMAND ----------

# Read dataset
df_spark: ps.DataFrame = ps.read_delta("/mnt/adls/mt_feature_store/data_halfweek")
df = df_spark.to_pandas()

# Reformat to TS
df["ds"] = pd.to_datetime(df["ds"])
df = df.sort_values(by = ["ds"])
df = df.reset_index()

mlflowService.log_param("num_row", df.shape[0])
mlflowService.log_param("num_column", df.shape[1])
display(df)

# COMMAND ----------

for col in df.columns:
    try:
        df[col] = pd.to_numeric(df[col])
        # print(f'"{col}",')
    except:
        a = 1
        print(f'"{col}",')

# df.loc[df["unique_id"] == "big_c|v101|cf conc. white pouch 3.2l|first_half"]

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

from sktime.forecasting.model_selection import temporal_train_test_split

lag_columns = [
"order_cs",
"est_demand",
"actual_sale"

]

numerical_columns = [

# Sale
"u_price_cs",

# Product Information
"pcs_per_cs",
"net_weight_kg_per_cs",
"gross_weight_kg_per_cs",


# Price change
"relaunch",

# Promotion 
"gift_value",
"tts",
"bmi",
"actual_tts",
"actual_bmi",

 # Calendar
"year",
"weekofyear",
"quarter",
"month",
"dayofmonth",
"weekend",
"dayofyear",
"is_year_start",
"is_year_end",
"is_quarter_start",
"is_quarter_end",
"is_month_start",
"is_month_end",
"is_leap_year",
"lunar_year",
"lunar_month",
"lunar_day",

]

categorical_columns = [
"banner",
"region",
"dp_name",

# Production Information
"pack_size",
"ph3_desc",
"ph7_desc",
"ph9",
"code_type",
"ph8",
"class",
"category",
"short_division",
"segment",
"brand",

# Price change
"activity_type",
"price_change",

# promotion
"post_name",
"promotion_code_type",
"on_off_post",
"promotion_type",
"promotion_grouping",
"gift_type",
"special_event",
"gift",
"ugift",
"discount",
# key event
"key_event",

# Calendar
"yearweek",
"half_week",
"vn_holiday",
"us_holiday",
"season",
]

data = df.copy()
data["ds"] = pd.to_datetime(data["ds"])

def transform(df):
    data = df.isna().sum()/len(df)
    data = data.sort_values(ascending = False)
    print(data.loc[data == 1])
    return df


# data.groupby(["unique_id"]).apply(lambda x: transform(x))

data = data.sort_values(by = ["unique_id", "ds"])

# data.groupby(["unique_id"]).apply(lambda x: x.isna().sum()/len(x))

data = data.set_index(["unique_id", "ds"])
data.index = data.index.set_levels(pd.to_datetime(data.index.levels[1]), level=1)
data.index = data.index.set_levels(data.index.levels[1].to_period('W'), level=1)
for col in categorical_columns:
    data[col] = data[col].astype("category")

data.loc[:, numerical_columns + categorical_columns + lag_columns].isna().sum()/len(data)


# COMMAND ----------

len(set(data.columns)), len(data.columns)
# data.loc[:, numerical_columns + categorical_columns + lag_columns].head()
# (data.groupby(level = 0)["index"].count()==116)
data.loc[data.index.get_level_values(0) == "big_c|v101|cf conc. white pouch 3.2l|first_half"]

# COMMAND ----------

category_transform = sklearn.pipeline.Pipeline(
    steps=[
        ("imputer", SimpleImputer(strategy="most_frequent", keep_empty_features = True, missing_values = pd.NA)),
        ("encoder", OrdinalEncoder(handle_unknown = "use_encoded_value", unknown_value = -1, dtype = np.int32)),
        # ("encoder", OneHotEncoder(handle_unknown = "ignore", sparse_output = False)),
        # ("encoder", LabelEncoder()),
        # ("cat", CategoricalTransformer(cat_columns=categorical_columns)),
    ]
)
# category_transform = sklearn.compose.ColumnTransformer(transformers = [
#     ("categorial", categorical_preprocessing, categorical_columns)
#     ],
#                                                        verbose_feature_names_out=False
#                                                        )

data[category_transform.get_feature_names_out()] = category_transform.fit_transform(data.loc[:, categorical_columns])

categorical_columns = list(category_transform.get_feature_names_out())
print("cat_columns: ", categorical_columns)
data.head()


# COMMAND ----------

train, test = list(SlidingWindowSplitter(fh=[1, 2], window_length = 96, step_length=1).split(data.loc[:, numerical_columns + lag_columns + categorical_columns]))[0]
# # train, test = list(SingleWindowSplitter(fh = [1, 2], window_length0).split(data))[0]
# data.iloc[test]
# data.index.get_level_values(1).unique()
data.iloc[test]

# COMMAND ----------

from sktime.transformations.panel.compose import ColumnTransformer
from sktime.transformations.compose import ColumnEnsembleTransformer , TransformerPipeline
from sktime.pipeline import Pipeline, make_pipeline
# from sklearn.pipeline import Pipeline
from sktime.transformations.series.impute import Imputer
from sktime.transformations.series.adapt import TabularToSeriesAdaptor
from sklearn.preprocessing import LabelEncoder
from sktime.transformations.compose import Id
from sktime.performance_metrics.forecasting import MeanAbsolutePercentageError
from sktime.transformations.series.detrend import Deseasonalizer, Detrender
from sktime.regression.base import BaseRegressor
from sktime.forecasting.trend import PolynomialTrendForecaster
from sktime.transformations.series.detrend import Deseasonalizer
from sktime.transformations.series.difference import Differencer
from sktime.transformations.hierarchical.aggregate import Aggregator
from sktime.transformations.compose import TransformerPipeline

# Regression
# from sktime.regression.distance_based import KNeighborsTimeSeriesRegressor
from sktime.regression.deep_learning.lstmfcn import LSTMFCNRegressor
from sktime.regression.deep_learning import TapNetRegressor
# from sktime.regression.deep_learning.mlp import MLPRegressor

class GBTRegressor(BaseEstimator, RegressorMixin, BaseRegressor):
  def __init__(self, regressor, categorical_feature: List[Union[int, str]]):
    self.regressor = regressor
    self.categorical_feature = categorical_feature
  def fit(self, X, y = None):
    X = self.transform_categorical(X)
    self.regressor.fit(X, y)
    return self
  def predict(self, X):
    X = self.transform_categorical(X)
    return self.regressor.predict(X)
  
  def transform_categorical(self, X):
    is_list_of_ints = lambda obj: isinstance(obj, list) and all(isinstance(elem, int) for elem in obj)
    is_list_of_strs = lambda obj: isinstance(obj, list) and all(isinstance(elem, str) for elem in obj)
    if is_list_of_strs(self.categorical_feature):
      for col in self.categorical_feature:
        X[col] = X[col].astype("category")
    elif is_list_of_ints(self.categorical_feature):
      for idx in self.categorical_feature:
        X.iloc[:, idx] = X.iloc[:, idx].astype("category")
    else:
      raise Exception("Sorry, categorical_feature isn't compatible type. It should be List[int] or List[str]")
    return X
    


# Numerical Processing
numerical_preprocessing = TransformerPipeline(
    steps=[
        ("imputer", Imputer(method = "backfill")),
    ]
)

# Categorical Processing
categorical_preprocessing = TransformerPipeline(steps = [
    ("imputer", Imputer(method = "backfill")),
    # ("encoder", TabularToSeriesAdaptor(OrdinalEncoder(handle_unknown = "use_encoded_value", unknown_value = -1))),
    # ("oh_encoder", TabularToSeriesAdaptor(OneHotEncoder(drop='first', sparse_output=False, handle_unknown='ignore'))),
]
)

window_kwargs = {
  "lag_feature": {
    "lag": [1, 4, 8, 16, 20, 24, 28, 32, 36, 40, 44, 48],
    "sum": [[0, 7, 13, 19, 23, 29, 48], [7, 13, 19, 21], [3, 7, 9, 13, 19, 27], [2, 4, 8, 12, 16, 20, 24, 30]],
    "mean": [[0, 7, 13, 19, 23, 29, 48], [7, 13, 19, 21], [3, 7, 9, 13, 19, 27], [2, 4, 8, 12, 16, 20, 24, 30]],
    "std": [[0, 7, 13, 19, 23, 29, 48], [7, 13, 19, 21], [3, 7, 9, 13, 19, 27], [2, 4, 8, 12, 16, 20, 24, 30]],
    "max": [[0, 7, 13, 19, 23, 29, 48], [7, 13, 19, 21], [3, 7, 9, 13, 19, 27], [2, 4, 8, 12, 16, 20, 24, 30]],
    "skew": [[0, 7, 13, 19, 23, 29, 48], [7, 13, 19, 21], [3, 7, 9, 13, 19, 27], [2, 4, 8, 12, 16, 20, 24, 30]],
    "kurt": [[0, 7, 13, 19, 23, 29, 48], [7, 13, 19, 21], [3, 7, 9, 13, 19, 27], [2, 4, 8, 12, 16, 20, 24, 30]],
    # "corr": [[0, 7, 13, 19, 23, 29], [7, 13, 19, 21], [3, 7, 9, 13, 19, 27], [2, 4, 8, 12, 16, 20, 24, 30]],
    }
}
# window_kwargs = {
#   "lag_feature": {
#     "lag": [1, 4, 8, 16],
#     "sum": [[0, 7, 13], [7, 19], [3, 7, 9], [2, 4]],
#     "mean": [[0, 7, 13], [7, 19], [3, 7, 9], [2, 4]],
#     "std": [[0, 7, 13], [7, 19], [3, 7, 9], [2, 4]],
#     "max": [[0, 7, 13], [7, 19], [3, 7, 9], [2, 4]],
#     "skew": [[0, 7, 13], [7, 19], [3, 7, 9], [2, 4]],
#     }
# }

combine_preprocessing = ColumnEnsembleTransformer(
    transformers=[
      ("categorial", categorical_preprocessing, categorical_columns),
      ("numerical", numerical_preprocessing, numerical_columns),
      ("promotion_window", WindowSummarizer(**window_kwargs, target_cols=["gift_value", "tts", "bmi"], n_jobs = -1, truncate = "bfill"), ["gift_value", "tts", "bmi"]),
      ("sale_window", WindowSummarizer(**window_kwargs, target_cols=["est_demand", "order_cs", "actual_sale"], n_jobs = -1, truncate = "bfill"), ["est_demand", "order_cs", "actual_sale"]),
    ],
)

lgbm_regressor = LGBMRegressor(
  n_jobs = -1, 
  # categorical_features = [idx for idx, col in enumerate(categorical_columns)],
  # categorical_features = [f"name:{col}" for idx, col in enumerate(categorical_columns)],
  verbose = -1,
  # objective = "poisson"
  )
catboost_regressor = CatBoostRegressor(
  cat_features= categorical_columns,
  verbose = False
)
xgb_regressor = XGBRegressor(
  enable_categorical = True,
  n_jobs = -1
  )
regressor = GBTRegressor(regressor = catboost_regressor, categorical_feature= categorical_columns)
# regressor = LSTMFCNRegressor()
# regressor = MLPRegressor()
# regressor = KNeighborsTimeSeriesRegressor(n_neighbors=1)

class DummyModel(BaseRegressor):
    def fit(self, X, y = None):
        print(f"shape: {X.shape}")
        # print("X: {X}")
        return self 
    def predict(self, X):
        return [1]*len(X)
# regressor = DummyModel()
# regressor = TapNetRegressor()
target = TransformedTargetForecaster(steps=[
  ("imputer", Imputer(method = "backfill")),
  ("log1p", LogTransformer(offset = 1)),
  # ("detrender", Detrender(forecaster=PolynomialTrendForecaster(degree=1))),
  ("deseasonal", Deseasonalizer(sp = 1)),
  # ("differencer", Differencer()),
  # ("aggregator", Aggregator()),
  ("imputer", Imputer(method = "backfill")),
  ("forecaster", make_reduction(regressor,
                                window_length = 1,
                                # transformers = [Imputer(method = "backfill")], 
                                strategy= "recursive",
                                scitype = "tabular-regressor",
                                windows_identical = True,
                                # pooling = "global",
                                ))
  ])

sktime_transformer = TransformerPipeline(steps = [
  ("combine_preprocessing", combine_preprocessing),
  # ("imputer", Imputer(method = "backfill")),
])
sktime_pipe = ForecastingPipeline(steps = [
  ("combine_preprocessing", combine_preprocessing),
  ("target", target),
])

target_transform = LogTransformer(offset=1)
transformed_target = sklearn.compose.TransformedTargetRegressor(regressor,
                                                      transformer=sklearn.pipeline.Pipeline(steps = [
                                                        # ("deseasonal", Deseasonalizer(sp = 1)),
                                                        ("log", LogTransformer(offset=1)),
                                                        # ("difference", Differencer()),
                                                        # ("boxcox", BoxCoxTransformer()),
                                                        ]
                                                                                            )
                                                      )
sklearn_pipe = sklearn.pipeline.Pipeline(steps = [
  ("sktime_transfomer", combine_preprocessing),
  ("lgbm", transformed_target),
  # ("lgbm", regressor)
])      

df_results= []
# for train, test in SingleWindowSplitter(fh = [1, 2], window_length=60).split(data[["est_demand"]]):
for idx, (train, test) in enumerate(SlidingWindowSplitter(fh=[1, 2], window_length = 96, step_length=1).split(data[["est_demand"]])):
  print(f"Start index: {idx}")
  X_train = data.iloc[train].loc[:, numerical_columns + categorical_columns + lag_columns]
  X_test = data.iloc[test].loc[:, numerical_columns + categorical_columns + lag_columns]
  y_train = data.iloc[train][["est_demand"]]
  y_test = data.iloc[test][["est_demand"]]
  
  # print("y_test: ", y_test.loc[y_test.index.get_level_values(0) == "big_c|v101|cf conc. white pouch 3.2l|first_half"])
  sklearn_pipe.fit(X_train, y_train)
  y_pred = sklearn_pipe.predict(X_test)
  fa_avg = fa_avg_score(y_test, y_pred)
  
  print(f"FA_avg: {fa_avg}")
  y_test["est_demand_pred"] = y_pred
  y_test["horizon"] = y_test.groupby(level = "unique_id").cumcount() + 1
  print(f"y_test: \n{y_test}")
  
  df_results.append(y_test)
  break

sktime_pipe

# COMMAND ----------

def get_feature_importances(pipe):
  model = pipe[-1]
  if isinstance(model, TransformedTargetRegressor):
    model = model.regressor_
  if isinstance(model, GBTRegressor):
    model = model.regressor
  
  if isinstance(model, CatBoostRegressor):
    return model.feature_importances_
  elif isinstance(model, LGBMRegressor):
    return model.feature_importances_
  else:
    raise Exception("Model is outside of CatBoostRegressor, LGBMRegressor")
def get_feature_names(pipe):
  model = pipe[-1]
  if isinstance(model, TransformedTargetRegressor):
    model = model.regressor_
  if isinstance(model, GBTRegressor):
    model = model.regressor
  if isinstance(model, CatBoostRegressor):
    return model.feature_names_
  elif isinstance(model, LGBMRegressor):
    return model.feature_name_
  else:
    raise Exception("Model is outside of CatBoostRegressor, LGBMRegressor")
  
# best_pipe = cv_results["estimator"][np.argmax(cv_results["test_fa_avg"])]
best_pipe = sklearn_pipe
ft_df = pd.DataFrame()

ft_df["features"] = get_feature_names(best_pipe)
ft_df["feature_importances"] = get_feature_importances(best_pipe)
ft_df["feature_importances_percentage"] = ft_df["feature_importances"]*100.0 / ft_df["feature_importances"].sum()
display(ft_df.sort_values(by = ["feature_importances"], ascending = False).head(50))

# COMMAND ----------

 # cv_results = evaluate(
#   forecaster = sktime_pipe,
#   # cv = SlidingWindowSplitter(fh=[1, 2], window_length = 60, step_length=2), # Current window_length = 44
#   cv = SingleWindowSplitter(fh = [1, 2], window_length=60),
#   strategy = "refit",
#   y = data.loc[:, ["est_demand"]],
#   X = data.loc[:, numerical_columns + categorical_columns + lag_columns],
#   scoring = [make_forecasting_scorer(fa_avg_score, name = "fa_avg", greater_is_better = True)],
#   error_score='raise',
#   return_data = True,
#   backend = None,
#   # backend = "joblib",
#   # backend_params= {
#   #   "backend": "spark",
#   #   "n_jobs": 1
#   # }
# )

# cv_results

# COMMAND ----------

# def transform_cutoff(df, df_ts):
#     y_test = df.y_test.iloc[0]
#     y_pred = df.y_pred.iloc[0].rename(columns = {"est_demand": "est_demand_pred"})
#     y_pred["est_demand_pred"] = y_pred["est_demand_pred"]
#     # y_pred["est_demand_pred"] = y_pred["est_demand_pred"].where(y_pred['est_demand_pred'] > 0, 0)

#     data = y_test.join(y_pred)
#     data = data.join(df_ts.loc[:, ["banner", "region", "dp_name", "category", "half_week", "weekofyear", "year"]])
#     data["horizon"] = data.groupby(level = "unique_id").cumcount() + 1
    
#     # print(data)
#     return data.reset_index()
# results = cv_results.groupby(["cutoff"]).apply(lambda x: transform_cutoff(x, data)).reset_index()
# display(results)

# COMMAND ----------

data_inverse = data.copy()
data_inverse[categorical_columns] = category_transform[-1].inverse_transform(data_inverse.loc[:, categorical_columns])
df_result = pd.concat(df_results).join(data_inverse.loc[:, ["banner", "region", "dp_name", "category", "half_week", "yearweek"]])
df_result = df_result.reset_index()
display(df_result)

# COMMAND ----------

# df_result[df_result["horizon"]>=3]

# COMMAND ----------

label = "est_demand"
prediction = "est_demand_pred"
fa_avg = fa_avg_score(df_result[label], df_result[prediction])
display(fa_avg)

# breaking down horizon
fa_avg_horizon = df_result.groupby(['horizon']).apply(
    lambda x: fa_avg_score(x[label], x[prediction])
).reset_index(name='fa_avg')
display(fa_avg_horizon)

# breaking down banner
fa_avg_banner = df_result.groupby(['banner']).apply(
    lambda x: fa_avg_score(x[label], x[prediction])
).reset_index(name='fa_avg')
display(fa_avg_banner)

# breaking down category
fa_avg_category = df_result.groupby(['category']).apply(
    lambda x: fa_avg_score(x[label], x[prediction])
).reset_index(name='fa_avg')
display(fa_avg_category)

# breaking down region
fa_avg_region = df_result.groupby(['region']).apply(
    lambda x: fa_avg_score(x[label], x[prediction])
).reset_index(name='fa_avg')
display(fa_avg_region)

# COMMAND ----------

df_result["ds"] = df_result["ds"].astype(str)
spark.createDataFrame(df_result).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("mt_feature_store.forecaster_v0_0_2")

display(spark.createDataFrame(df_result))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC min(est_demand_pred)
# MAGIC from mt_feature_store.forecaster_v0_0_1

# COMMAND ----------



# COMMAND ----------

results = evaluate(
    forecaster=pipe,
    X = X,
    y = y,
    cv = cv,
    scoring=[MeanAbsoluteError()]
)
results

# COMMAND ----------

from sktime.datatypes import get_examples
data = get_examples(mtype="pd-multiindex")[0]
# data.index.get_level_values(level = -1)
X = data.drop(columns = ["var_0"])
y = data[["var_0"]]
# y_train, y_test, X_train, X_test = temporal_train_test_split(y, X, test_size = 0.2)


# COMMAND ----------

from sktime.data

# COMMAND ----------

import pyspark.pandas as ps
spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")
path = "/mnt/adls/SGC_BIGC_DS/YAFSU/2024-03-26/BIG_C_FIRST_HALF-202349/OUTPUT/RESULT_nixtla_mlforecast_model_FC_EST_DEMAND.pkl.parquet"
path_lib = "/mnt/adls/SGC_BIGC_DS/YAFSU/2024-03-26/BIG_C_FIRST_HALF-202349/OUTPUT/RESULT_nixtla_mlforecast_model_FC_EST_DEMAND.pkl.parquet"
df = ps.read_parquet(path_lib)
df

# COMMAND ----------

import pandas as pd
path_lib = "/mnt/adls/SGC_BIGC_DS/YAFSU/2024-03-26/BIG_C_FIRST_HALF-202349/OUTPUT/RESULT_nixtla_mlforecast_model_FC_EST_DEMAND.pkl.parquet"
# path_lib = "/mnt/adls/SGC_BIGC_DS/YAFSU/2024-03-26/BIG_C_FIRST_HALF-202350/OUTPUT/RESULT_nixtla_mlforecast_model_FC_EST_DEMAND.pkl.parquet"
pd.read_parquet(f"/dbfs{path_lib}").dtypes

# COMMAND ----------

import pandas as pd
df = pd.read_excel("/dbfs/mnt/adls/MT_POST_MODEL/MASTER/Machine learning Factors.xlsx", 
                   sheet_name = "Key Event",
                   header = 1,
                    )
import re


def clean_column_name(column_name):
    # Replace any sequence of characters that is not alphanumeric or underscore with an underscore
    cleaned_name = re.sub(r'[^a-zA-Z0-9_]', '_', column_name)
    cleaned_name = re.sub(r'_{2,}', '_', cleaned_name)
    # Optionally, you might want to remove leading or trailing underscores
    cleaned_name = re.sub(r'^_+|_+$', '', cleaned_name)
    # Convert to lowercase
    cleaned_name = cleaned_name.lower()
    return cleaned_name

df.columns = [clean_column_name(col) for col in df.columns]
df_melted = pd.melt(df, id_vars = ["year", "banner", "post_name", "key_event_name"],
                    value_vars = ["pc_up", "deo", "oral", "skin", "skin_cleansing", "hair", "hhc", "fab_sen", "fab_sol", "tea", "culinary", "ice_cream"],
                    var_name = "kv_category", 
                    value_name = "kv_value"
                    )
df_melted

spark.createDataFrame(df_melted).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("mt_predicted_order.mt_key_event")

# COMMAND ----------

import pandas as pd
from sktime.transformations.hierarchical.aggregate import Aggregator

# Create a MultiIndex to represent the hierarchical structure of product and store
index = pd.MultiIndex.from_tuples([
    ('Product A', 'Store 1'),
    ('Product A', 'Store 2'),
    ('Product B', 'Store 1'),
    ('Product B', 'Store 2'),
], names=['Product', 'Store'])

# Create a DataFrame with sample sales data
data = {
    'Day 1': [100, 150, 200, 250],
    'Day 2': [110, 160, 210, 260],
    'Day 3': [120, 170, 220, 270],
    'Day 4': [130, 180, 230, 280],
    'Day 5': [140, 190, 240, 290]
}

sales_data = pd.DataFrame(data, index=index)

# Define the index structure for aggregation
# Here, we're aggregating at the 'Product' level
index_structure = [['Product']]

# Instantiate and fit the Aggregator
aggregator = Aggregator(index_structure=index_structure)
aggregated_data = aggregator.fit_transform(sales_data)

aggregated_data


# COMMAND ----------


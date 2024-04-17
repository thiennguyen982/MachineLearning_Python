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
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import Pipeline 
from sklearn.compose import ColumnTransformer, TransformedTargetRegressor
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, LabelEncoder, OrdinalEncoder, RobustScaler, FunctionTransformer
from sklearn.model_selection import train_test_split, TimeSeriesSplit, cross_val_predict, cross_validate, GridSearchCV, RandomizedSearchCV

# Tree base model
from sklearn.ensemble import RandomForestRegressor
from lightgbm.sklearn import LGBMRegressor
from xgboost.sklearn import XGBRFRegressor
from catboost import CatBoostRegressor

# statsmodels
# ACF and PACF
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf

# Sktime
from sktime.transformations.series.boxcox import BoxCoxTransformer, LogTransformer
from sktime.transformations.series.cos import CosineTransformer
from sktime.transformations.series.exponent import ExponentTransformer, SqrtTransformer


# mlflow
import mlflow
from mlflow import MlflowClient
from mlflow.models import infer_signature
## System metrics
os.environ["MLFLOW_ENABLE_SYSTEM_METRICS_LOGGING"] = "true"
mlflow.enable_system_metrics_logging()

from typing import Optional, Union, List, Dict, Any



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
""")

# COMMAND ----------

# DBTITLE 1,Data Preparation
# MAGIC %sql
# MAGIC -- Timeseries: ds
# MAGIC -- Customer: Banner, region,
# MAGIC -- Product: ulv_code
# MAGIC with date_ranges as (
# MAGIC   select
# MAGIC     -- primary key
# MAGIC     trim(lower(banner)) as banner,
# MAGIC     trim(lower(region)) as region,
# MAGIC     trim(lower(dp_name)) as dp_name,
# MAGIC     min(to_date(billing_date)) as min_date,
# MAGIC     max(to_date(billing_date)) as max_date
# MAGIC   from
# MAGIC     mt_predicted_order.mt_sale
# MAGIC   group by
# MAGIC     banner,
# MAGIC     region,
# MAGIC     dp_name
# MAGIC ),
# MAGIC dates_expanded AS (
# MAGIC   SELECT
# MAGIC     EXPLODE(
# MAGIC       sequence(
# MAGIC         to_date("2022-01-01", 'yyyy-MM-dd'),
# MAGIC         to_date("2024-03-01", 'yyyy-MM-dd'),
# MAGIC         INTERVAL 1 DAY
# MAGIC       )
# MAGIC     ) AS ds,
# MAGIC     banner,
# MAGIC     region,
# MAGIC     dp_name,
# MAGIC     min_date,
# MAGIC     max_date
# MAGIC   FROM
# MAGIC     date_ranges
# MAGIC ),
# MAGIC dates_final as (
# MAGIC   select
# MAGIC     ds,
# MAGIC     banner,
# MAGIC     region,
# MAGIC     dp_name
# MAGIC   from
# MAGIC     dates_expanded
# MAGIC   where
# MAGIC     1 = 1
# MAGIC     and ds between min_date
# MAGIC     and max_date
# MAGIC ) -- Calendar
# MAGIC ,
# MAGIC calendar as (
# MAGIC   select
# MAGIC     to_date(date) as ds,
# MAGIC     year,
# MAGIC     quarter,
# MAGIC     month,
# MAGIC     dayofmonth,
# MAGIC     dayofweek + 1 as dayofweek,
# MAGIC     case
# MAGIC       when dayofweek + 1 in (1, 2, 3) then "first_half"
# MAGIC       else "last_half"
# MAGIC     end as half_week,
# MAGIC     weekend,
# MAGIC     dayofyear,
# MAGIC     weekofyear,
# MAGIC     is_year_start,
# MAGIC     is_year_end,
# MAGIC     is_quarter_start,
# MAGIC     is_quarter_end,
# MAGIC     is_month_start,
# MAGIC     is_month_end,
# MAGIC     is_leap_year,
# MAGIC     vn_holiday,
# MAGIC     us_holiday,
# MAGIC     lunar_date,
# MAGIC     lunar_year,
# MAGIC     lunar_month,
# MAGIC     lunar_day,
# MAGIC     season
# MAGIC   from
# MAGIC     mt_predicted_order.mt_calendar_master
# MAGIC ),
# MAGIC calendar_final as (
# MAGIC   select
# MAGIC     *
# MAGIC   from
# MAGIC     calendar
# MAGIC ),
# MAGIC product_information as (
# MAGIC   select
# MAGIC     cast(ulv_code as bigint) as ulv_code,
# MAGIC     trim(lower(dp_name)) as dp_name,
# MAGIC     class,
# MAGIC     category,
# MAGIC     packsize,
# MAGIC     packgroup,
# MAGIC     brand_variant,
# MAGIC     subbrand,
# MAGIC     brand,
# MAGIC     segment,
# MAGIC     format,
# MAGIC     production_segment
# MAGIC   from
# MAGIC     mt_predicted_order.mt_product_master
# MAGIC ),
# MAGIC price_change as (
# MAGIC   select
# MAGIC     explode(
# MAGIC       sequence(
# MAGIC         case
# MAGIC           when to_date(start_date, 'yyyy-MM-dd') < to_date(end_date, 'yyyy-MM-dd') then to_date(start_date, 'yyyy-MM-dd')
# MAGIC           else to_date(end_date, 'yyyy-MM-dd')
# MAGIC         end,
# MAGIC         case
# MAGIC           when to_date(start_date, 'yyyy-MM-dd') < to_date(end_date, 'yyyy-MM-dd') then to_date(end_date, 'yyyy-MM-dd')
# MAGIC           else to_date(start_date, 'yyyy-MM-dd')
# MAGIC         end,
# MAGIC         interval 1 day
# MAGIC       )
# MAGIC     ) AS ds,
# MAGIC     --duplicate
# MAGIC     trim(lower(banner)) as banner,
# MAGIC     trim(lower(dp_name)) as dp_name,
# MAGIC     relaunch,
# MAGIC     activity_type,
# MAGIC     price_change
# MAGIC   from
# MAGIC     mt_predicted_order.mt_price_change
# MAGIC ),
# MAGIC price_change_final as (
# MAGIC   select
# MAGIC     *
# MAGIC   from
# MAGIC     price_change
# MAGIC ) -- promotion
# MAGIC ,
# MAGIC promotion_explode as (
# MAGIC   select
# MAGIC     explode(
# MAGIC       sequence(
# MAGIC         case
# MAGIC           when to_date(order_start_date) <= to_date(order_end_date) then to_date(order_start_date)
# MAGIC           else null
# MAGIC         end,
# MAGIC         case
# MAGIC           when to_date(order_start_date) <= to_date(order_end_date) then to_date(order_end_date)
# MAGIC           else null
# MAGIC         end,
# MAGIC         interval 1 day
# MAGIC       )
# MAGIC     ) AS ds,
# MAGIC     trim(lower(banner)) as banner,
# MAGIC     trim(lower(dp_name)) as dp_name,
# MAGIC     cast(ulv_code as bigint) as ulv_code,
# MAGIC     category,
# MAGIC     trim(lower(post_name)) as post_name,
# MAGIC     trim(lower(code_type)) as code_type,
# MAGIC     trim(lower(on_off_post)) as on_off_post,
# MAGIC     trim(lower(promotion_type)) as promotion_type,
# MAGIC     trim(lower(promotion_grouping)) as promotion_grouping,
# MAGIC     trim(lower(gift_type)) as gift_type,
# MAGIC     cast(gift_value as float) as gift_value,
# MAGIC     cast(tts as float) as tts,
# MAGIC     cast(bmi as float) as bmi,
# MAGIC     cast(actual_tts as float) as actual_tts,
# MAGIC     cast(actual_bmi as float) as actual_bmi
# MAGIC   from
# MAGIC     mt_predicted_order.mt_promotion
# MAGIC ),
# MAGIC promotion_final as (
# MAGIC   select
# MAGIC     *,
# MAGIC     -- transform promotion
# MAGIC     case
# MAGIC       when promotion_grouping rlike '.*hot price.*' then 'hot price'
# MAGIC       when promotion_grouping rlike '.*mnudl.*' then 'mnudl'
# MAGIC       when promotion_grouping rlike '.*pwp.*' then 'pwp'
# MAGIC       else 'unknown'
# MAGIC     end as special_event,
# MAGIC     case
# MAGIC       when regexp_replace(promotion_grouping, ' ', '') rlike '.*keyevent.*' then true
# MAGIC       else false
# MAGIC     end as key_event,
# MAGIC     -- check keyevent sheet from ML factor
# MAGIC     coalesce(
# MAGIC       case
# MAGIC         when promotion_grouping RLIKE '.*(?<!u-)gift.*' then gift_type
# MAGIC         else 'no-gift'
# MAGIC       end,
# MAGIC       'no-gift'
# MAGIC     ) as gift,
# MAGIC     coalesce(
# MAGIC       case
# MAGIC         when promotion_grouping RLIKE 'u-gift' then gift_type
# MAGIC         else 'no-gift'
# MAGIC       end,
# MAGIC       'no-gift'
# MAGIC     ) as ugift,
# MAGIC     coalesce(
# MAGIC       case
# MAGIC         when promotion_grouping RLIKE 'discount - (normal|mnudl|pwd|hot price)' then promotion_grouping
# MAGIC         else 'no-discount'
# MAGIC       end,
# MAGIC       'no-discount'
# MAGIC     ) as discount
# MAGIC   from
# MAGIC     promotion_explode
# MAGIC ) -- sale
# MAGIC ,
# MAGIC sale as (
# MAGIC   select
# MAGIC     -- primary key
# MAGIC     to_date(billing_date) as ds,
# MAGIC     trim(lower(banner)) as banner,
# MAGIC     trim(lower(region)) as region,
# MAGIC     trim(lower(dp_name)) as dp_name,
# MAGIC     cast(ulv_code as bigint) as ulv_code,
# MAGIC     cast(est_demand_cs as bigint) as est_demand,
# MAGIC     cast(order_cs as bigint) as order_cs,
# MAGIC     cast(u_price_cs as bigint) as u_price_cs,
# MAGIC     -- at t: ready?
# MAGIC     cast(key_in_after_value as bigint) as key_in_after_value,
# MAGIC     cast(billing_cs as bigint) as billing_cs
# MAGIC   from
# MAGIC     mt_predicted_order.mt_sale
# MAGIC ),
# MAGIC sale_final as (
# MAGIC   select
# MAGIC     -- primary key
# MAGIC     ds,
# MAGIC     banner,
# MAGIC     region,
# MAGIC     dp_name,
# MAGIC     ulv_code,
# MAGIC     coalesce(est_demand, 0) as est_demand,
# MAGIC     order_cs,
# MAGIC     u_price_cs,
# MAGIC     key_in_after_value,
# MAGIC     billing_cs as actual_sale
# MAGIC   from
# MAGIC     sale
# MAGIC ),
# MAGIC final as (
# MAGIC   select
# MAGIC     -- primary key
# MAGIC     df.banner,
# MAGIC     df.region,
# MAGIC     df.dp_name,
# MAGIC     concat_ws("_", year, weekofyear, half_week) as ds_freq,
# MAGIC     min(df.ds) as ds,
# MAGIC     -- sale
# MAGIC     avg(u_price_cs) as u_price_cs,
# MAGIC     sum(order_cs) as order_cs,
# MAGIC     sum(key_in_after_value) as key_in_after_value,
# MAGIC     sum(est_demand) as est_demand,
# MAGIC     sum(actual_sale) as actual_sale,
# MAGIC     -- product information
# MAGIC     first(class, true) as class,
# MAGIC     -- [null, A, B, C] -> return A
# MAGIC     first(brand, true) as brand,
# MAGIC     first(pi.packsize, true) as packsize,
# MAGIC     first(pi.packgroup, true) as packgroup,
# MAGIC     first(pi.brand_variant, true) as brand_variant,
# MAGIC     first(pi.category, true) as category,
# MAGIC     first(pi.format, true) as format,
# MAGIC     first(pi.segment, true) as segment,
# MAGIC     first(pi.subbrand, true) as subbrand,
# MAGIC     first(pi.production_segment, true) as production_segment,
# MAGIC     -- price change
# MAGIC     first(relaunch, true) as relaunch,
# MAGIC     -- get max
# MAGIC     first(activity_type, true) as activity_type,
# MAGIC     --
# MAGIC     first(price_change, true) as price_change,
# MAGIC     -- get mean
# MAGIC     -- promotion
# MAGIC     element_at(array_distinct(collect_list(post_name)), -1) as post_name,
# MAGIC     -- get freq
# MAGIC     element_at(array_distinct(collect_list(code_type)), -1) as code_type,
# MAGIC     element_at(array_distinct(collect_list(on_off_post)), -1) as on_off_post,
# MAGIC     element_at(array_distinct(collect_list(promotion_type)), -1) as promotion_type,
# MAGIC     element_at(
# MAGIC       array_distinct(collect_list(promotion_grouping)),
# MAGIC       -1
# MAGIC     ) as promotion_grouping,
# MAGIC     element_at(array_distinct(collect_list(gift_type)), -1) as gift_type,
# MAGIC     avg(gift_value) as gift_value,
# MAGIC     avg(tts) as tts,
# MAGIC     avg(bmi) as bmi,
# MAGIC     avg(actual_tts) as actual_tts,
# MAGIC     avg(actual_bmi) as actual_bmi,
# MAGIC     -- transform promotion
# MAGIC     coalesce(first(special_event, true), 'unknown') as special_event,
# MAGIC     coalesce(first(key_event, true), false) as key_event,
# MAGIC     -- get freq
# MAGIC     first(gift, true) as gift,
# MAGIC     first(ugift, true) as ugift,
# MAGIC     first(discount, true) as discount,
# MAGIC     -- calendar
# MAGIC     avg(year) as year,
# MAGIC     avg(quarter) as quarter,
# MAGIC     avg(month) as month,
# MAGIC     avg(dayofmonth) as dayofmonth,
# MAGIC     avg(dayofweek) as dayofweek,
# MAGIC     first(half_week, true) as half_week,
# MAGIC     first(weekend, true) as weekend,
# MAGIC     avg(dayofyear) as dayofyear,
# MAGIC     avg(weekofyear) as weekofyear,
# MAGIC     coalesce(
# MAGIC       first(
# MAGIC         case
# MAGIC           when is_year_start = true then true
# MAGIC         end,
# MAGIC         true
# MAGIC       ),
# MAGIC       false
# MAGIC     ) as is_year_start,
# MAGIC     coalesce(
# MAGIC       first(
# MAGIC         case
# MAGIC           when is_year_end = true then true
# MAGIC         end,
# MAGIC         true
# MAGIC       ),
# MAGIC       false
# MAGIC     ) as is_year_end,
# MAGIC     coalesce(
# MAGIC       first(
# MAGIC         case
# MAGIC           when is_quarter_start = true then true
# MAGIC         end,
# MAGIC         true
# MAGIC       ),
# MAGIC       false
# MAGIC     ) as is_quarter_start,
# MAGIC     coalesce(
# MAGIC       first(
# MAGIC         case
# MAGIC           when is_quarter_end = true then true
# MAGIC         end,
# MAGIC         true
# MAGIC       ),
# MAGIC       false
# MAGIC     ) as is_quarter_end,
# MAGIC     coalesce(
# MAGIC       first(
# MAGIC         case
# MAGIC           when is_month_start = true then true
# MAGIC         end,
# MAGIC         true
# MAGIC       ),
# MAGIC       false
# MAGIC     ) as is_month_start,
# MAGIC     coalesce(
# MAGIC       first(
# MAGIC         case
# MAGIC           when is_month_end = true then true
# MAGIC         end,
# MAGIC         true
# MAGIC       ),
# MAGIC       false
# MAGIC     ) as is_month_end,
# MAGIC     coalesce(
# MAGIC       first(
# MAGIC         case
# MAGIC           when is_leap_year = true then true
# MAGIC         end,
# MAGIC         true
# MAGIC       ),
# MAGIC       false
# MAGIC     ) as is_leap_year,
# MAGIC     coalesce(
# MAGIC       first(
# MAGIC         case
# MAGIC           when vn_holiday != 'non-holiday' then vn_holiday
# MAGIC         end,
# MAGIC         true
# MAGIC       ),
# MAGIC       'non-holiday'
# MAGIC     ) as vn_holiday,
# MAGIC     coalesce(
# MAGIC       first(
# MAGIC         case
# MAGIC           when us_holiday != 'non-holiday' then us_holiday
# MAGIC         end,
# MAGIC         true
# MAGIC       ),
# MAGIC       'non-holiday'
# MAGIC     ) as us_holiday,
# MAGIC     avg(lunar_year) as lunar_year,
# MAGIC     avg(lunar_month) as lunar_month,
# MAGIC     avg(lunar_day) as lunar_day,
# MAGIC     first(season, true) as season
# MAGIC   from
# MAGIC     dates_final df
# MAGIC     left join sale_final sf on df.ds = sf.ds
# MAGIC     and df.region = sf.region
# MAGIC     and df.dp_name = sf.dp_name
# MAGIC     and df.banner = sf.banner
# MAGIC     left join product_information pi on df.dp_name = pi.dp_name -- sf.ulv_code = pi.ulv_code
# MAGIC     left join price_change_final pcf on df.ds = pcf.ds
# MAGIC     and df.banner = pcf.banner
# MAGIC     and df.dp_name = pcf.dp_name
# MAGIC     left join promotion_final pf on df.ds = pf.ds
# MAGIC     and df.banner = pf.banner
# MAGIC     and df.dp_name = pf.dp_name
# MAGIC     left join calendar_final cf on df.ds = cf.ds
# MAGIC   where
# MAGIC     1 = 1
# MAGIC   group by
# MAGIC     1,
# MAGIC     2,
# MAGIC     3,
# MAGIC     4
# MAGIC ),
# MAGIC filter_top_sku as (
# MAGIC   select
# MAGIC     dp_name,
# MAGIC     sum(est_demand) as sum_est_demand
# MAGIC   from
# MAGIC     final
# MAGIC   group by
# MAGIC     1
# MAGIC   order by
# MAGIC     2 desc
# MAGIC   limit
# MAGIC     100
# MAGIC )
# MAGIC select
# MAGIC   f.*
# MAGIC from
# MAGIC   final f
# MAGIC   inner join filter_top_sku fts on f.dp_name = fts.dp_name
# MAGIC where
# MAGIC   1 = 1
# MAGIC   and ds >= to_date("2022-01-01", 'yyyy-MM-dd')
# MAGIC   and (banner in ("saigon coop", "big_c")) -- select
# MAGIC   -- -- ds_freq,
# MAGIC   -- -- size(array_distinct(collect_list(ds))) as cnt_ds,
# MAGIC   -- -- min(ds) as ds,
# MAGIC   -- -- array_distinct(collect_list(ds)) as list_ds
# MAGIC   -- banner,
# MAGIC   -- region,
# MAGIC   -- dp_name,
# MAGIC   -- ds,
# MAGIC   -- ds_freq
# MAGIC   -- from final
# MAGIC   -- where 1=1
# MAGIC   -- and ds >= to_date("2024-01-01", 'yyyy-MM-dd')
# MAGIC   -- and (banner in  ("saigon coop", "big_c"))
# MAGIC   -- and weekofyear = 8
# MAGIC   -- and year = 2024
# MAGIC   -- and banner = "saigon coop"
# MAGIC   -- and dp_name = "omo gold 3900 gr"
# MAGIC   -- -- group by 1
# MAGIC   -- -- order by 2 desc, 1 desc
# MAGIC   -- order by ds
# MAGIC   -- select
# MAGIC   -- banner,
# MAGIC   -- region,
# MAGIC   -- dp_name,
# MAGIC   -- df.ds,
# MAGIC   -- cf.dayofweek as dayofweek,
# MAGIC   -- cf.year,
# MAGIC   -- cf.weekofyear,
# MAGIC   -- cf.half_week
# MAGIC   -- -- cf.* except (ds)
# MAGIC   -- from dates_final df
# MAGIC   -- left join calendar_final cf on df.ds = cf.ds
# MAGIC   -- where 1=1
# MAGIC   -- and banner = "saigon coop"
# MAGIC   -- and dp_name = "omo gold 3900 gr"

# COMMAND ----------

_sqldf.cache().createOrReplaceTempView("data_mpo_final")
# _sqldf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("mt_feature_store.data_halfweek")

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   1
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Feature engineering
# MAGIC with final as (
# MAGIC   select
# MAGIC     ds,
# MAGIC     ds_freq,
# MAGIC     concat_ws("|", banner, region, dp_name) as unique_id,
# MAGIC     banner,
# MAGIC     region,
# MAGIC     dp_name,
# MAGIC     -- sale
# MAGIC     u_price_cs,
# MAGIC     order_cs,
# MAGIC     key_in_after_value,
# MAGIC     coalesce(est_demand, 0) as est_demand,
# MAGIC     actual_sale,
# MAGIC     -- product information
# MAGIC     class,
# MAGIC     brand,
# MAGIC     packsize,
# MAGIC     packgroup,
# MAGIC     brand_variant,
# MAGIC     category,
# MAGIC     format,
# MAGIC     segment,
# MAGIC     subbrand,
# MAGIC     production_segment,
# MAGIC     -- price change
# MAGIC     relaunch,
# MAGIC     activity_type,
# MAGIC     price_change,
# MAGIC     -- promotion
# MAGIC     post_name,
# MAGIC     code_type,
# MAGIC     on_off_post,
# MAGIC     promotion_type,
# MAGIC     promotion_grouping,
# MAGIC     gift_type,
# MAGIC     gift_value,
# MAGIC     tts,
# MAGIC     bmi,
# MAGIC     actual_tts,
# MAGIC     actual_bmi,
# MAGIC     special_event,
# MAGIC     key_event,
# MAGIC     gift,
# MAGIC     ugift,
# MAGIC     discount,
# MAGIC     -- calendar
# MAGIC     year,
# MAGIC     quarter,
# MAGIC     month,
# MAGIC     dayofmonth,
# MAGIC     dayofweek,
# MAGIC     weekend,
# MAGIC     dayofyear,
# MAGIC     weekofyear,
# MAGIC     is_year_start,
# MAGIC     is_year_end,
# MAGIC     is_quarter_start,
# MAGIC     is_quarter_end,
# MAGIC     is_month_start,
# MAGIC     is_month_end,
# MAGIC     is_leap_year,
# MAGIC     vn_holiday,
# MAGIC     us_holiday,
# MAGIC     lunar_year,
# MAGIC     lunar_month,
# MAGIC     lunar_day,
# MAGIC     season -- Feature Engineering
# MAGIC     -- -- promotion
# MAGIC     -- -- -- post_name
# MAGIC     -- size(array_distinct(collect_list(post_name) over (partition by banner, region, dp_name  order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding))) as rolling_count_post_name_d14,
# MAGIC     -- size(array_distinct(collect_list(post_name) over (partition by banner, region, dp_name  order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding))) as rolling_count_post_name_d28,
# MAGIC     -- size(array_distinct(collect_list(post_name) over (partition by banner, region, dp_name  order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding))) as rolling_count_post_name_d90,
# MAGIC     -- size(array_distinct(collect_list(post_name) over (partition by banner, region, dp_name  order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding))) as rolling_count_post_name_d180,
# MAGIC     -- size(array_distinct(collect_list(post_name) over (partition by banner, region, dp_name  order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding))) as rolling_count_post_name_d365,
# MAGIC     -- -- Generate FE
# MAGIC     -- sum(est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_sum_est_demand_d14,
# MAGIC     -- avg(est_demand) over (partition by banner, region, dp_name order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_avg_est_demand_d14,
# MAGIC     -- min(est_demand) over (partition by banner, region, dp_name order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_min_est_demand_d14,
# MAGIC     -- max(est_demand) over (partition by banner, region, dp_name order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_max_est_demand_d14,
# MAGIC     -- std(est_demand) over (partition by banner, region, dp_name order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_std_est_demand_d14,
# MAGIC     -- sum(case when est_demand > 0 then 1 end) over (partition by banner, region, dp_name order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_count_est_demand_d14,
# MAGIC     -- sum(est_demand) over (partition by banner, region, dp_name, half_week  order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_sum_half_week_est_demand_d14,
# MAGIC     -- avg(est_demand) over (partition by banner, region, dp_name, half_week order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_avg_half_week_est_demand_d14,
# MAGIC     -- min(est_demand) over (partition by banner, region, dp_name, half_week order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_min_half_week_est_demand_d14,
# MAGIC     -- max(est_demand) over (partition by banner, region, dp_name, half_week order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_max_half_week_est_demand_d14,
# MAGIC     -- std(est_demand) over (partition by banner, region, dp_name, half_week order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_std_half_week_est_demand_d14,
# MAGIC     -- sum(case when est_demand > 0 then 1 end) over (partition by banner, region, dp_name, half_week order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_count_half_week_est_demand_d14,
# MAGIC     -- sum(est_demand) over (partition by banner, region, category  order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_sum_category_est_demand_d14,
# MAGIC     -- avg(est_demand) over (partition by banner, region, category order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_avg_category_est_demand_d14,
# MAGIC     -- min(est_demand) over (partition by banner, region, category order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_min_category_est_demand_d14,
# MAGIC     -- max(est_demand) over (partition by banner, region, category order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_max_category_est_demand_d14,
# MAGIC     -- std(est_demand) over (partition by banner, region, category order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_std_category_est_demand_d14,
# MAGIC     -- sum(case when est_demand > 0 then 1 end) over (partition by banner, region, category order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_count_category_est_demand_d14,
# MAGIC     -- sum(actual_sale) over (partition by banner, region, dp_name  order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_sum_actual_sale_d14,
# MAGIC     -- avg(actual_sale) over (partition by banner, region, dp_name order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_avg_actual_sale_d14,
# MAGIC     -- min(actual_sale) over (partition by banner, region, dp_name order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_min_actual_sale_d14,
# MAGIC     -- max(actual_sale) over (partition by banner, region, dp_name order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_max_actual_sale_d14,
# MAGIC     -- std(actual_sale) over (partition by banner, region, dp_name order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_std_actual_sale_d14,
# MAGIC     -- sum(case when actual_sale > 0 then 1 end) over (partition by banner, region, dp_name order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_count_actual_sale_d14,
# MAGIC     -- sum(tts) over (partition by banner, region, dp_name  order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_sum_tts_d14,
# MAGIC     -- avg(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_avg_tts_d14,
# MAGIC     -- min(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_min_tts_d14,
# MAGIC     -- max(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_max_tts_d14,
# MAGIC     -- std(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_std_tts_d14,
# MAGIC     -- sum(case when tts > 0 then 1 end) over (partition by banner, region, dp_name order by unix_date(ds) range between 14 preceding and ${LAG}*3 preceding) as rolling_count_tts_d14,
# MAGIC     -- sum(est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_sum_est_demand_d28,
# MAGIC     -- avg(est_demand) over (partition by banner, region, dp_name order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_avg_est_demand_d28,
# MAGIC     -- min(est_demand) over (partition by banner, region, dp_name order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_min_est_demand_d28,
# MAGIC     -- max(est_demand) over (partition by banner, region, dp_name order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_max_est_demand_d28,
# MAGIC     -- std(est_demand) over (partition by banner, region, dp_name order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_std_est_demand_d28,
# MAGIC     -- sum(case when est_demand > 0 then 1 end) over (partition by banner, region, dp_name order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_count_est_demand_d28,
# MAGIC     -- sum(est_demand) over (partition by banner, region, dp_name, half_week  order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_sum_half_week_est_demand_d28,
# MAGIC     -- avg(est_demand) over (partition by banner, region, dp_name, half_week order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_avg_half_week_est_demand_d28,
# MAGIC     -- min(est_demand) over (partition by banner, region, dp_name, half_week order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_min_half_week_est_demand_d28,
# MAGIC     -- max(est_demand) over (partition by banner, region, dp_name, half_week order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_max_half_week_est_demand_d28,
# MAGIC     -- std(est_demand) over (partition by banner, region, dp_name, half_week order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_std_half_week_est_demand_d28,
# MAGIC     -- sum(case when est_demand > 0 then 1 end) over (partition by banner, region, dp_name, half_week order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_count_half_week_est_demand_d28,
# MAGIC     -- sum(est_demand) over (partition by banner, region, category  order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_sum_category_est_demand_d28,
# MAGIC     -- avg(est_demand) over (partition by banner, region, category order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_avg_category_est_demand_d28,
# MAGIC     -- min(est_demand) over (partition by banner, region, category order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_min_category_est_demand_d28,
# MAGIC     -- max(est_demand) over (partition by banner, region, category order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_max_category_est_demand_d28,
# MAGIC     -- std(est_demand) over (partition by banner, region, category order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_std_category_est_demand_d28,
# MAGIC     -- sum(case when est_demand > 0 then 1 end) over (partition by banner, region, category order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_count_category_est_demand_d28,
# MAGIC     -- sum(actual_sale) over (partition by banner, region, dp_name  order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_sum_actual_sale_d28,
# MAGIC     -- avg(actual_sale) over (partition by banner, region, dp_name order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_avg_actual_sale_d28,
# MAGIC     -- min(actual_sale) over (partition by banner, region, dp_name order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_min_actual_sale_d28,
# MAGIC     -- max(actual_sale) over (partition by banner, region, dp_name order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_max_actual_sale_d28,
# MAGIC     -- std(actual_sale) over (partition by banner, region, dp_name order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_std_actual_sale_d28,
# MAGIC     -- sum(case when actual_sale > 0 then 1 end) over (partition by banner, region, dp_name order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_count_actual_sale_d28,
# MAGIC     -- sum(tts) over (partition by banner, region, dp_name  order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_sum_tts_d28,
# MAGIC     -- avg(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_avg_tts_d28,
# MAGIC     -- min(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_min_tts_d28,
# MAGIC     -- max(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_max_tts_d28,
# MAGIC     -- std(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_std_tts_d28,
# MAGIC     -- sum(case when tts > 0 then 1 end) over (partition by banner, region, dp_name order by unix_date(ds) range between 28 preceding and ${LAG}*3 preceding) as rolling_count_tts_d28,
# MAGIC     -- sum(est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_sum_est_demand_d90,
# MAGIC     -- avg(est_demand) over (partition by banner, region, dp_name order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_avg_est_demand_d90,
# MAGIC     -- min(est_demand) over (partition by banner, region, dp_name order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_min_est_demand_d90,
# MAGIC     -- max(est_demand) over (partition by banner, region, dp_name order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_max_est_demand_d90,
# MAGIC     -- std(est_demand) over (partition by banner, region, dp_name order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_std_est_demand_d90,
# MAGIC     -- sum(case when est_demand > 0 then 1 end) over (partition by banner, region, dp_name order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_count_est_demand_d90,
# MAGIC     -- sum(est_demand) over (partition by banner, region, dp_name, half_week  order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_sum_half_week_est_demand_d90,
# MAGIC     -- avg(est_demand) over (partition by banner, region, dp_name, half_week order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_avg_half_week_est_demand_d90,
# MAGIC     -- min(est_demand) over (partition by banner, region, dp_name, half_week order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_min_half_week_est_demand_d90,
# MAGIC     -- max(est_demand) over (partition by banner, region, dp_name, half_week order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_max_half_week_est_demand_d90,
# MAGIC     -- std(est_demand) over (partition by banner, region, dp_name, half_week order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_std_half_week_est_demand_d90,
# MAGIC     -- sum(case when est_demand > 0 then 1 end) over (partition by banner, region, dp_name, half_week order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_count_half_week_est_demand_d90,
# MAGIC     -- sum(est_demand) over (partition by banner, region, category  order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_sum_category_est_demand_d90,
# MAGIC     -- avg(est_demand) over (partition by banner, region, category order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_avg_category_est_demand_d90,
# MAGIC     -- min(est_demand) over (partition by banner, region, category order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_min_category_est_demand_d90,
# MAGIC     -- max(est_demand) over (partition by banner, region, category order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_max_category_est_demand_d90,
# MAGIC     -- std(est_demand) over (partition by banner, region, category order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_std_category_est_demand_d90,
# MAGIC     -- sum(case when est_demand > 0 then 1 end) over (partition by banner, region, category order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_count_category_est_demand_d90,
# MAGIC     -- sum(actual_sale) over (partition by banner, region, dp_name  order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_sum_actual_sale_d90,
# MAGIC     -- avg(actual_sale) over (partition by banner, region, dp_name order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_avg_actual_sale_d90,
# MAGIC     -- min(actual_sale) over (partition by banner, region, dp_name order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_min_actual_sale_d90,
# MAGIC     -- max(actual_sale) over (partition by banner, region, dp_name order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_max_actual_sale_d90,
# MAGIC     -- std(actual_sale) over (partition by banner, region, dp_name order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_std_actual_sale_d90,
# MAGIC     -- sum(case when actual_sale > 0 then 1 end) over (partition by banner, region, dp_name order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_count_actual_sale_d90,
# MAGIC     -- sum(tts) over (partition by banner, region, dp_name  order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_sum_tts_d90,
# MAGIC     -- avg(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_avg_tts_d90,
# MAGIC     -- min(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_min_tts_d90,
# MAGIC     -- max(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_max_tts_d90,
# MAGIC     -- std(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_std_tts_d90,
# MAGIC     -- sum(case when tts > 0 then 1 end) over (partition by banner, region, dp_name order by unix_date(ds) range between 90 preceding and ${LAG}*3 preceding) as rolling_count_tts_d90,
# MAGIC     -- sum(est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_sum_est_demand_d180,
# MAGIC     -- avg(est_demand) over (partition by banner, region, dp_name order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_avg_est_demand_d180,
# MAGIC     -- min(est_demand) over (partition by banner, region, dp_name order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_min_est_demand_d180,
# MAGIC     -- max(est_demand) over (partition by banner, region, dp_name order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_max_est_demand_d180,
# MAGIC     -- std(est_demand) over (partition by banner, region, dp_name order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_std_est_demand_d180,
# MAGIC     -- sum(case when est_demand > 0 then 1 end) over (partition by banner, region, dp_name order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_count_est_demand_d180,
# MAGIC     -- sum(est_demand) over (partition by banner, region, dp_name, half_week  order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_sum_half_week_est_demand_d180,
# MAGIC     -- avg(est_demand) over (partition by banner, region, dp_name, half_week order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_avg_half_week_est_demand_d180,
# MAGIC     -- min(est_demand) over (partition by banner, region, dp_name, half_week order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_min_half_week_est_demand_d180,
# MAGIC     -- max(est_demand) over (partition by banner, region, dp_name, half_week order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_max_half_week_est_demand_d180,
# MAGIC     -- std(est_demand) over (partition by banner, region, dp_name, half_week order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_std_half_week_est_demand_d180,
# MAGIC     -- sum(case when est_demand > 0 then 1 end) over (partition by banner, region, dp_name, half_week order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_count_half_week_est_demand_d180,
# MAGIC     -- sum(est_demand) over (partition by banner, region, category  order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_sum_category_est_demand_d180,
# MAGIC     -- avg(est_demand) over (partition by banner, region, category order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_avg_category_est_demand_d180,
# MAGIC     -- min(est_demand) over (partition by banner, region, category order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_min_category_est_demand_d180,
# MAGIC     -- max(est_demand) over (partition by banner, region, category order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_max_category_est_demand_d180,
# MAGIC     -- std(est_demand) over (partition by banner, region, category order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_std_category_est_demand_d180,
# MAGIC     -- sum(case when est_demand > 0 then 1 end) over (partition by banner, region, category order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_count_category_est_demand_d180,
# MAGIC     -- sum(actual_sale) over (partition by banner, region, dp_name  order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_sum_actual_sale_d180,
# MAGIC     -- avg(actual_sale) over (partition by banner, region, dp_name order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_avg_actual_sale_d180,
# MAGIC     -- min(actual_sale) over (partition by banner, region, dp_name order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_min_actual_sale_d180,
# MAGIC     -- max(actual_sale) over (partition by banner, region, dp_name order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_max_actual_sale_d180,
# MAGIC     -- std(actual_sale) over (partition by banner, region, dp_name order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_std_actual_sale_d180,
# MAGIC     -- sum(case when actual_sale > 0 then 1 end) over (partition by banner, region, dp_name order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_count_actual_sale_d180,
# MAGIC     -- sum(tts) over (partition by banner, region, dp_name  order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_sum_tts_d180,
# MAGIC     -- avg(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_avg_tts_d180,
# MAGIC     -- min(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_min_tts_d180,
# MAGIC     -- max(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_max_tts_d180,
# MAGIC     -- std(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_std_tts_d180,
# MAGIC     -- sum(case when tts > 0 then 1 end) over (partition by banner, region, dp_name order by unix_date(ds) range between 180 preceding and ${LAG}*3 preceding) as rolling_count_tts_d180,
# MAGIC     -- sum(est_demand) over (partition by banner, region, dp_name  order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_sum_est_demand_d365,
# MAGIC     -- avg(est_demand) over (partition by banner, region, dp_name order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_avg_est_demand_d365,
# MAGIC     -- min(est_demand) over (partition by banner, region, dp_name order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_min_est_demand_d365,
# MAGIC     -- max(est_demand) over (partition by banner, region, dp_name order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_max_est_demand_d365,
# MAGIC     -- std(est_demand) over (partition by banner, region, dp_name order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_std_est_demand_d365,
# MAGIC     -- sum(case when est_demand > 0 then 1 end) over (partition by banner, region, dp_name order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_count_est_demand_d365,
# MAGIC     -- sum(est_demand) over (partition by banner, region, dp_name, half_week  order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_sum_half_week_est_demand_d365,
# MAGIC     -- avg(est_demand) over (partition by banner, region, dp_name, half_week order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_avg_half_week_est_demand_d365,
# MAGIC     -- min(est_demand) over (partition by banner, region, dp_name, half_week order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_min_half_week_est_demand_d365,
# MAGIC     -- max(est_demand) over (partition by banner, region, dp_name, half_week order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_max_half_week_est_demand_d365,
# MAGIC     -- std(est_demand) over (partition by banner, region, dp_name, half_week order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_std_half_week_est_demand_d365,
# MAGIC     -- sum(case when est_demand > 0 then 1 end) over (partition by banner, region, dp_name, half_week order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_count_half_week_est_demand_d365,
# MAGIC     -- sum(est_demand) over (partition by banner, region, category  order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_sum_category_est_demand_d365,
# MAGIC     -- avg(est_demand) over (partition by banner, region, category order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_avg_category_est_demand_d365,
# MAGIC     -- min(est_demand) over (partition by banner, region, category order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_min_category_est_demand_d365,
# MAGIC     -- max(est_demand) over (partition by banner, region, category order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_max_category_est_demand_d365,
# MAGIC     -- std(est_demand) over (partition by banner, region, category order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_std_category_est_demand_d365,
# MAGIC     -- sum(case when est_demand > 0 then 1 end) over (partition by banner, region, category order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_count_category_est_demand_d365,
# MAGIC     -- sum(actual_sale) over (partition by banner, region, dp_name  order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_sum_actual_sale_d365,
# MAGIC     -- avg(actual_sale) over (partition by banner, region, dp_name order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_avg_actual_sale_d365,
# MAGIC     -- min(actual_sale) over (partition by banner, region, dp_name order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_min_actual_sale_d365,
# MAGIC     -- max(actual_sale) over (partition by banner, region, dp_name order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_max_actual_sale_d365,
# MAGIC     -- std(actual_sale) over (partition by banner, region, dp_name order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_std_actual_sale_d365,
# MAGIC     -- sum(case when actual_sale > 0 then 1 end) over (partition by banner, region, dp_name order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_count_actual_sale_d365,
# MAGIC     -- sum(tts) over (partition by banner, region, dp_name  order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_sum_tts_d365,
# MAGIC     -- avg(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_avg_tts_d365,
# MAGIC     -- min(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_min_tts_d365,
# MAGIC     -- max(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_max_tts_d365,
# MAGIC     -- std(tts) over (partition by banner, region, dp_name order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_std_tts_d365,
# MAGIC     -- sum(case when tts > 0 then 1 end) over (partition by banner, region, dp_name order by unix_date(ds) range between 365 preceding and ${LAG}*3 preceding) as rolling_count_tts_d365
# MAGIC   from
# MAGIC     data_mpo_final dmf
# MAGIC )
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   final
# MAGIC where
# MAGIC   1 = 1
# MAGIC   and ds >= to_date('2023-01-01', 'yyyy-MM-dd')

# COMMAND ----------

_sqldf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("mt_feature_store.data_halfweek_fe")

# COMMAND ----------

# Read dataset
df_spark: ps.DataFrame = ps.read_delta("/mnt/adls/mt_feature_store/data_halfweek_fe")
df = df_spark.to_pandas()

# Reformat to TS
df["ds"] = pd.to_datetime(df["ds"])
df = df.sort_values(by = ["ds"])
df = df.reset_index()

mlflowService.log_param("num_row", df.shape[0])
mlflowService.log_param("num_column", df.shape[1])
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# DBTITLE 1,Train Test Split
test_time = pd.DataFrame()
test_time["ds"] = df.query("ds >= '2023-11-01'").ds.sort_values().unique()
test_time
data_splits = []
for idx in range(0, len(test_time)-4):
    data = test_time.iloc[idx:idx+4]
    if len(data) < 4:
        continue
    data["horizon"] = [i for i in range(1, 5)]
    train = df.loc[df['ds']<data.iloc[0, 0]]
    train["horizon"] = 0
    test = df.merge(data, on = ["ds"], how = "inner", suffixes=("", ""))
    data_splits.append((train, test))

df_train, df_test = data_splits[0]
mlflowService.log_param("train_size", df_train.shape[0])
mlflowService.log_param("test_size", df_test.shape[0])


print(f"""
train:  
  size: {df_train.shape} 
test:
  size: {df_test.shape}
""")

# COMMAND ----------

# numerical_features = []

# categorical_features = []
# for col in df.columns:
#   try:
#     pd.to_numeric(df[col])
#     numerical_features.append(col)
#   except:
#     categorical_features.append(col)
    
# print("numerical features")
# for col in numerical_features:
#   print(f'"{col}",')

# print("categorical features")
# for col in categorical_features:
#   print(f'"{col}",')

# COMMAND ----------

ts_columns = [
  "unique_id",
  "ds"
]
# Feature selection
numerical_columns = [

# Sale


# Promotion
"u_price_cs",
# "key_in_after_value",
"gift_value",
"tts",
"bmi",
"actual_tts",
"actual_bmi",
"key_event",


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

# Promotion
# "price_change",
"on_off_post",
# "promotion_type", -- double check it in the future
"promotion_grouping",
"gift_type",
"special_event",
"gift",
"ugift",
"discount",
"code_type",
"post_name",

# Calendar
"vn_holiday",
"us_holiday",
"season",
]

label = "est_demand"
mlflowService.log_param("numerical_columns", numerical_columns)
mlflowService.log_param("categorical_columns", categorical_columns)
mlflowService.log_param("label", label)
# assert(len(set(df_final.columns.tolist()).intersection(set(numerical_columns + categorical_columns))) == len(numerical_columns + categorical_columns))
if len(set(df.columns.tolist()).intersection(set(numerical_columns + categorical_columns))) !=len(numerical_columns + categorical_columns):
  for col in numerical_columns + categorical_columns:
    if col not in df.columns:
      print(col)


# COMMAND ----------

class CategoricalTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, columns):
        self.columns = columns

    def fit(self, X, y=None):
        data = pd.DataFrame(X, columns = self.columns)
        
        _, self.categorical_features = self.auto_cast_type(data)
        return self

    def transform(self, X):
        data = pd.DataFrame(X, columns = self.columns)
        data, _ = self.auto_cast_type(data, self.categorical_features)
        return data

    @staticmethod
    def auto_cast_type(df: pd.DataFrame, categorical_features = None) -> pd.DataFrame:
        cat_features = []
        if categorical_features is not None:
            for col in df.columns:
                if col in categorical_features:
                    df[col] = df[col].astype("category")
                else:
                    df[col] = pd.to_numeric(df[col])
            cat_features = categorical_features
        else:
            for col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col])
                except Exception as e:
                    df[col] = df[col].astype("category")
                    cat_features.append(col)
        return df, cat_features

class Rolling:
    def __init__(self, window: int):
        self.window = window


class RollingMean(Rolling):
    def get_name(self):
        return f"rolling_{self.window}_mean"
    def __call__(self, series):
        return series.rolling(window = self.window, min_periods = 1).mean()

class RollingSum(Rolling):
    def get_name(self):
        return f"rolling_{self.window}_sum"
    def __call__(self, series):
        return series.rolling(window = self.window, min_periods = 1).sum()
class RollingMax(Rolling):
    def get_name(self):
        return f"rolling_{self.window}_max"
    def __call__(self, series):
        return series.rolling(window = self.window, min_periods = 1).max()

class TimeSeriesFE(BaseEstimator, TransformerMixin):
    def __init__(
        self,
        model: Pipeline,
        label: str,
        lags: List[int] = [1, 4],
        lag_transforms: Optional[Dict[int, Any]] = None,
        fe_column: Optional[bool] = False,
        horizon: int = 10,
    ):
        self.model = model
        self.label = label
        self.horizon = horizon
        self.lags = lags
        self.lag_transforms = lag_transforms
        self.fe_column = fe_column
        self.fit_ts: Optional[pd.DataFrame] = (None,)

    def fit(self, X, y=None):
        X_transform, self.fe_columns = self.window_transform(X)
        self.fit_ts = X
        self.fit_ts["future"] = False
        self.feature_names_out = X_transform.columns
        X_transform, self.cat_features = self.auto_cast_type(X_transform)
        logger.info(f"cat_features: {self.cat_features}")
        X_transform = X_transform.drop(
            columns=["unique_id", "ds", self.label]
        )
        if self.fe_column == False:
            self.fe_columns = X_transform.columns.tolist()

        self.model.fit(X_transform, y)
        return self

    def transform(self, X):
        data = X.copy()

        if self.fit_ts is not None:
            future_df = data.merge(
                self.fit_ts, on=["unique_id", "ds"], how="outer", suffixes=("_y", ""), validate = "one_to_one"
            )
            future_df = future_df.drop(columns=[col for col in future_df.columns if re.match(r'.*_y$', col)])
            future_df = future_df.loc[future_df["future"] != False]
            if len(future_df) > 0:
                future_df["future"] = True
                data = pd.concat([self.fit_ts, future_df])
                data["ds"] = pd.to_datetime(data["ds"])
                data = data.sort_values(by=["ds"])
                # Create horizon column
                data = (
                    data.groupby("unique_id")
                    .apply(self.transform_future_horizon)
                    .reset_index(drop=True)
                )
                data = data.sort_values(by=["ds"])

                for step in range(1, int(data.horizon.max()) + 1):
                    data, _ = self.window_transform(data)
                    horizon_predict, _ = self.auto_cast_type(
                        data.loc[data["horizon"] == step, :].drop(
                            columns=["future", "horizon", "unique_id", "ds", self.label]
                        ),
                        self.cat_features
                    )
                    data.loc[data["horizon"] == step, self.label] = self.model.predict(
                        horizon_predict
                    )
                data = data.drop(columns=["horizon", "future"])
            else:
                logger.info("Go to because future_df = 0")

        else:
            logger.info("Go to without fit_ts")

        data, _ = self.window_transform(data)
        X = X.merge(data, on=["unique_id", "ds"], how="inner", suffixes=("_y", ""), validate = "one_to_one")
        X = X.drop(columns=[col for col in X.columns if re.match(r'.*_y$', col)])
        if self.fe_column == True:
            return X.loc[:, self.fe_columns]
        else:
            return X

    def get_feature_names_out(self, input_features: Optional[List[str]] = None):
        return self.fe_columns

    def window_transform(self, X):
        data = X.copy()
        data = data.sort_values(by=["ds"]).reset_index()
        fe_columns = []
        for group in ["unique_id"]:
            for lag in self.lags:
                column_name = f"{self.label}_shift_{lag}"
                data = data.join(data.groupby(group)[self.label].transform(lambda x: x.shift(lag)).rename(column_name), lsuffix='_x')
                data = data.drop(columns=[col for col in data.columns if re.match(r'.*_x$', col)])
                fe_columns.append(column_name)
                if lag in self.lag_transforms:
                    transform_details = self.lag_transforms[lag]
                    for transform in transform_details:
                        column_name = f"{self.label}_lag_{lag}_{transform.get_name()}"
                        data = data.join(data.groupby(group)[self.label].transform(lambda x: transform(x.shift(lag))).rename(column_name), lsuffix = "_x")
                        data = data.drop(columns=[col for col in data.columns if re.match(r'.*_x$', col)])
                        fe_columns.append(column_name)

        X = X.merge(
            data[[group, "ds"] + fe_columns],
            on=[group, "ds"],
            how="inner",
            suffixes=("_y", ""),
            validate = "one_to_one"
        )
        X = X.drop(columns=[col for col in X.columns if re.match(r'.*_y$', col)])
        return X, fe_columns

    @staticmethod
    def transform_future_horizon(group) -> pd.DataFrame:
        group["horizon"] = group["future"].cumsum()
        return group

    @staticmethod
    def auto_cast_type(df: pd.DataFrame, categorical_features = None) -> pd.DataFrame:
        cat_features = []
        if categorical_features is not None:
            for col in df.columns:
                if col in categorical_features:

                    df[col] = df[col].astype("category")
                else:
                    df[col] = pd.to_numeric(df[col])
            cat_features = categorical_features
        else:
            for col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col])
                except Exception as e:
                    df[col] = df[col].astype("category")
                    cat_features.append(col)
        return df, cat_features
 
lgb = LGBMRegressor()

fe_ts = TimeSeriesFE(model = LGBMRegressor(), 
               label = "est_demand", 
               lags = [1, 4, 8, 16], 
               lag_transforms = {
                   1: [RollingMean(window = 4), RollingMean(window = 8), RollingMean(window = 16)],
                   4: [RollingMean(window = 4), RollingMean(window = 8), RollingMean(window = 16)],
               },
               fe_column = True)

# X_train = df_train.loc[:, ["unique_id", "ds", "est_demand"] + numerical_columns + categorical_columns]
# y_train = df_train["est_demand"]

# X_test = df_test.loc[:, ["unique_id", "ds", "est_demand"] + numerical_columns + categorical_columns]
# y_test = df_test["est_demand"]

# fe_ts.fit(X_train, y_train)
# fe_ts.transform(X_test)
# fe_ts.get_feature_names_out()

# COMMAND ----------

# DBTITLE 1,Target Transform
class DifferenceTransformer(BaseEstimator, TransformerMixin):
    def fit(self, X, y=None):
        return self

    def transform(self, X):
        return np.diff(X, axis=0, prepend=X[0].reshape(1, -1))

    def inverse_transform(self, X_diff):
        return np.cumsum(X_diff, axis=0)

def difference(x):
    return np.diff(x, prepend=0)

def inverse_difference(x_diff):
    return np.cumsum(x_diff)

difference_transformer = FunctionTransformer(func=difference, inverse_func=inverse_difference)

logarit_transformer = FunctionTransformer(func=np.log1p, inverse_func=np.expm1)

target_transform = Pipeline(steps = [
    ("logarit_transform", logarit_transformer),
    ("difference", difference_transformer),
])

data = df.groupby("ds")["est_demand"].sum().reset_index().copy()
target_transform.fit(data["est_demand"])
data["target_transform"] = target_transform.transform(data["est_demand"])
data["inverse_target_transform"] = target_transform.inverse_transform(data["target_transform"])
display(data)

# COMMAND ----------

X_train = df_train.loc[
    :, ts_columns + numerical_columns + categorical_columns + ["est_demand"]
]
X_test = df_test.loc[
        :, ts_columns + numerical_columns + categorical_columns + ["est_demand"]
]
y_train = df_train[["est_demand"]]

# Model pipeline
numerical_preprocessing = Pipeline(
    steps=[
        ("imputer", SimpleImputer(strategy = "mean", keep_empty_features = True, missing_values = pd.NA)),
        ("scaler", RobustScaler()),
    ]
)

categorical_preprocessing = Pipeline(
    steps=[
        ("imputer", SimpleImputer(strategy="most_frequent", keep_empty_features = True, missing_values = pd.NA)),
        # ("encoder", OrdinalEncoder(handle_unknown = "use_encoded_value", unknown_value = -1)),
        # ("encoder", OneHotEncoder(handle_unknown = "ignore")),
        # ("cat", CategoricalTransformer(cat_columns=categorical_columns)),
    ]
)

lags = [1, 4, 8, 16, 32]
lag_transforms = {}
for lag in lags:
    lag_transforms[lag] = [
        RollingMean(window=4),
        RollingMean(window=8),
        RollingMean(window=16),
        RollingMean(window=32),
        RollingSum(window=4),
        RollingSum(window=8),
        RollingSum(window=16),
        RollingSum(window=32),
        RollingMax(window=4),
        RollingMax(window=8),
        RollingMax(window=16),
        RollingMax(window=32),
    ]

demand_fe = TimeSeriesFE(
    model=LGBMRegressor(n_jobs=-1),
    label="est_demand",
    lags=lags,
    lag_transforms=lag_transforms,
    fe_column=True,
)

tts_fe = TimeSeriesFE(
    model=LGBMRegressor(n_jobs=-1),
    label="tts",
    lags=lags,
    lag_transforms=lag_transforms,
    fe_column=True,
)

bmi_fe = TimeSeriesFE(
    model=LGBMRegressor(n_jobs=-1),
    label="bmi",
    lags=lags,
    lag_transforms=lag_transforms,
    fe_column=True,
)

combine_preprocessing = ColumnTransformer(
    transformers=[
        ("numerical", numerical_preprocessing, ["u_price_cs"]),
        ("categorial", categorical_preprocessing, categorical_columns),
        (
            "demand_fe",
            demand_fe,
            ["unique_id", "ds", "est_demand"] + numerical_columns + categorical_columns,
        ),
        # (
        #     "tts_fe",
        #     tts_fe,
        #     ["unique_id", "ds", "est_demand"] + numerical_columns + categorical_columns,
        # ),
        # (
        #     "bmi_fe",
        #     bmi_fe,
        #     ["unique_id", "ds", "est_demand"] + numerical_columns + categorical_columns,
        # ),
    ],
    verbose_feature_names_out=False,
    n_jobs=-1,
    verbose = True
)
combine_preprocessing.fit(X_train.iloc[:10], y_train.iloc[:10])
combine_columns = combine_preprocessing.get_feature_names_out()
len_column = combine_preprocessing.transform(X_train.iloc[:10]).shape[1]

# Target Transform
target_transform = Pipeline(steps = [
    ("logarit_transform", logarit_transformer),
    # ("difference", difference_transformer),
])


rf = RandomForestRegressor(random_state=2024, n_jobs=-1)
lgb = LGBMRegressor(
    categorical_features=[
        i
        for i in range(
            len(numerical_columns), len(numerical_columns) + len(categorical_columns)
        )
    ],
    n_jobs=-1,
    objective="poisson",
)

lgb = LGBMRegressor(categorical_features="auto", n_jobs=-1, objective="poisson")

# xgb = XGBRFRegressor()
# catboost = CatBoostRegressor(
#   cat_features = categorical_columns
#   #cat_features = [i for i in range(len(numerical_columns), len(numerical_columns) + len(categorical_columns))],

# )


lgb = LGBMRegressor(categorical_features="auto", n_jobs=-1, objective="poisson")
catboost = CatBoostRegressor(cat_features=categorical_columns)
parameters = {
    "estimator__regressor": [
        LGBMRegressor(categorical_features="auto", n_jobs=-1, objective="poisson"),
        XGBRFRegressor(enable_categorical=True),
        CatBoostRegressor(cat_features=categorical_columns),
    ],
    # "estimator__regressor__categorical_features": ["auto"]
}
pipe = Pipeline(
    steps=[
        ("combine", combine_preprocessing),
        (
            "cat",
            CategoricalTransformer(columns=combine_columns),
        ),
        (
            "estimator",
            TransformedTargetRegressor(lgb, transformer=target_transform),
        ),
        # ("estimator", LGBMRegressor(categorical_features = "auto",n_jobs = -1, objective = "poisson")),
        # ("estimator", LGBMRegressor(n_jobs = -1, objective = "poisson")),
    ],
    verbose = True
)

mlflow.log_param("model_name", "lgbm")
grid_pipe = GridSearchCV(
    estimator=pipe,
    param_grid=parameters,
    scoring={
        "fa_avg": fa_avg_scorer,
        "fa_sumup": fa_sumup_scorer,
        "rmse": "neg_root_mean_squared_error",
        "mae": "neg_mean_absolute_error",
        "mape": "neg_mean_absolute_percentage_error",
    },
    refit=False,
    cv=TimeSeriesSplit(n_splits=2),
    return_train_score=True,
    error_score="raise",
    n_jobs=-1,
)
# grid_pipe.fit(
#   X = df_train.loc[:, numerical_columns + categorical_columns],
#   y = df_train[label]
# )
# pd.DataFrame(grid_pipe.cv_results_)
print("Start fitting ...")
pipe.fit(X_train, y_train)

print(("Start predicting ..."))
# X_test_transform = pipe[:-2].transform(X_test)
X_test["prediction"] = pipe.predict(X_test)

fa_avg = fa_avg_score(X_test["est_demand"], X_test["prediction"])
print(f"FA_avg: {fa_avg}")
pipe

# COMMAND ----------

display(X_test.loc[:, ["ds", "est_demand", "prediction"]])

# COMMAND ----------

# cv_results = cross_validate(
#   estimator=pipe,
#   X = df_train.loc[:, ts_columns + numerical_columns + categorical_columns + ["est_demand"]],
#   y = df_train[label],
#   scoring = {
#     "fa_avg": fa_avg_scorer,
#     "fa_sumup": fa_sumup_scorer,
#     "rmse": "neg_root_mean_squared_error",
#     "mae": "neg_mean_absolute_error",
#     "mape": "neg_mean_absolute_percentage_error",
#   },
#   cv = TimeSeriesSplit(n_splits=2),
#   return_estimator=True,
#   return_train_score=True,
#   error_score="raise",
#   n_jobs = -1
# )

# COMMAND ----------

# df_cv = {
#   "0_model": "CV_LGBM"
# }
# for key, value in cv_results.items():
#   if re.search("(train|test)", key):
#     cv_results[key] = np.mean(value)
#     df_cv[key] = np.round(np.mean(value), 2)
#     mlflow.log_metric(key, df_cv[key]) 

# display(pd.DataFrame.from_records([df_cv]).sort_index(axis = 1, ascending = True))

# COMMAND ----------

def get_feature_importances(pipe):
  model = pipe[-1]
  if isinstance(model, TransformedTargetRegressor):
    model = model.regressor_
  return model.feature_importances_

# best_pipe = cv_results["estimator"][np.argmax(cv_results["test_fa_avg"])]
best_pipe = pipe
ft_df = pd.DataFrame()

ft_df["features"] = best_pipe[-3].get_feature_names_out()
ft_df["feature_importances"] = get_feature_importances(best_pipe)
ft_df["feature_importances_percentage"] = ft_df["feature_importances"]*100.0 / ft_df["feature_importances"].sum()
display(ft_df.sort_values(by = ["feature_importances"], ascending = False).head(50))


# COMMAND ----------

df_evaluation = []
for idx, (train, test) in enumerate(data_splits):
    pipe.fit(train.loc[:, ts_columns + numerical_columns + categorical_columns + ["est_demand"]], train["est_demand"])
    if idx == 0:
        train["prediction"] = pipe.predict(train.loc[:, ts_columns + numerical_columns + categorical_columns + ["est_demand"]])
        df_evaluation.append(train[["ds", "unique_id", "banner", "region", "dp_name", "est_demand", "prediction", "horizon"]])
    test["prediction"] = pipe.predict(test.loc[:, ts_columns + numerical_columns + categorical_columns + ["est_demand"]])
    df_evaluation.append(test[["ds", "unique_id", "banner", "region", "dp_name", "est_demand", "prediction", "horizon"]])
df_evaluation = pd.concat(df_evaluation)

# COMMAND ----------

prediction = "prediction"
df_test = df_evaluation.query("horizon>0")
fa_avg = fa_avg_score(df_test['est_demand'], df_test[prediction])
fa_avg_df = pd.DataFrame({"FA_avg": [fa_avg]}, index = ["all"])

fa_avg_horizon = df_test.groupby(['horizon']).apply(
    lambda x: fa_avg_score(x['est_demand'], x[prediction])
).reset_index(name='fa_avg')


fa_avg_banner = df_test.groupby(['banner']).apply(
    lambda x: fa_avg_score(x['est_demand'], x[prediction])
).reset_index(name='fa_avg')

fa_avg_banner_region = df_test.groupby(['banner', 'region']).apply(
    lambda x: fa_avg_score(x['est_demand'], x[prediction])
).reset_index(name='fa_avg')


fa_avg_banner_region_dp_name = df_test.groupby(['banner', 'region', 'dp_name']).apply(
    lambda x: fa_avg_score(x['est_demand'], x[prediction])
).reset_index(name='fa_avg')

display(fa_avg_df)
display(fa_avg_horizon)
display(fa_avg_banner)
display(fa_avg_banner_region)
display(fa_avg_banner_region_dp_name)

# COMMAND ----------

cols = ["ds", "ds_freq", "banner", "region", "dp_name", "est_demand", "prediction"]
display(df_evaluation.query("horizon in [0, 1]"))
# display(pd.concat([df_train.loc[:, cols], df_test.loc[:, cols]]))

# COMMAND ----------

df_evaluation.query("horizon in [0, 1]").ds.max()

# COMMAND ----------

# data_train = df_train.copy()
# data_train["stage"] = "train"
# data_test = df_test.copy()
# data_test["stage"] = "test"

# data = pd.concat([data_train, data_test])
# data["absolute_error"] = np.abs(data["est_demand"], data["prediction"])
# display(data)

# COMMAND ----------

shap_columns = list(best_pipe[-3].get_feature_names_out()) + ["expected_value"]
shap_value_df_train = df_train["prediction"] = best_pipe.predict(df_train.loc[:, numerical_columns + categorical_columns], pred_contrib = True)
shap_value_df_train = pd.DataFrame(shap_value_df_train, columns = shap_columns)
shap_value_df_train[["ds", "banner", "region", "dp_name"]] = df_train[["ds", "banner", "region", "dp_name"]]

shap_value_df_test = df_test["prediction"] = best_pipe.predict(df_test.loc[:, numerical_columns + categorical_columns], pred_contrib = True)
shap_value_df_test = pd.DataFrame(shap_value_df_test, columns = shap_columns)
shap_value_df_test[["ds", "banner", "region", "dp_name"]] = df_test[["ds", "banner", "region", "dp_name"]]
shap_value_df = pd.concat([shap_value_df_train, shap_value_df_test])

# COMMAND ----------

data = shap_value_df.groupby(["banner"]).mean()
data_t = data.transpose().reset_index()
display(data_t.sort_values(by = ["saigon coop"], ascending = False))

# COMMAND ----------

import shap
instance_index = 0  # For example, the first instance

# Extract SHAP values for the chosen instance (excluding the last value which is the expected value)
shap_values_instance = shap_values_matrix[instance_index][:-1]

# Extract the expected value (base value) for the chosen instance
expected_value = shap_values_matrix[instance_index][-1]

# Get feature names
feature_names = X.columns.tolist()

# Visualization with waterfall plot
shap.waterfall_plot(expected_value, shap_values_instance, feature_names)

# COMMAND ----------

shap.plots.waterfall(data)

# COMMAND ----------


# shap_values_instance = shap_value_df.loc[0, best_pipe[-2].get_feature_names_out()].values
# base_value = shap_value_df.iloc[0, -1]
# feature_values = shap_value_df.iloc[0, :-1].values
# feature_names = best_pipe[-2].get_feature_names_out()
# explanation = shap.Explanation(
#     values=shap_values_instance.reshape(1, -1),  # SHAP values for the instance, reshaped as a 2D array
#     base_values=np.array([expected_value]),  # Base value, wrapped in an array
#     data=feature_values.reshape(1, -1),  # Actual feature values for the instance, reshaped as a 2D array
#     feature_names=feature_names  # Feature names
# )

# # Use the waterfall plot with the manually created Explanation object
# shap.plots.waterfall(explanation[0])

# COMMAND ----------

shap_values_instance

# COMMAND ----------

explanation.data.shape

# COMMAND ----------

len(feature_names)

# COMMAND ----------

from pathlib import Path
import functools
import pyspark.pandas as ps
from mt_predicted_order.de.etl.utils import clean_column_name
from pyspark.sql.types import StructType
from pyspark.sql.functions import to_date, col
from typing import List


# Set Spark configuration to dynamically overwrite partitions
spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

# Define paths and exclusion criteria
base_path = Path("/dbfs/mnt/adls/MT_POST_MODEL/REMOVE REPEATED/")
chunk_size = 100
def read_excel_chunks(file_paths: List[str], dataAddress: str, chunk_size: int = 100):
  df = spark.createDataFrame([], StructType([]))
  for i in range(0, len(file_paths), chunk_size):
    print(f"Process chunk from {i} -> {i+chunk_size}")
    try:
      df_spark = (spark.read.format("excel").option("dataAddress", "'Raw data'!A2")
                                            .option("header", "true")
                                            .option("maxRowsInMemory", 10000)
                                            .option("maxByteArraySize", 2147483647)
                                            .option("tempFileThreshold", 10000000)
                                            .load(file_paths[i:i+chunk_size])
                                          )
      df = df.unionByName(df_spark, allowMissingColumns=True)
    except Exception as e:
      print(f"Error reading chunk: {i}->{i+chunk_size}, error: {e}")
      for idx in range(i, i+chunk_size):
        file_path = file_paths[idx]
        try:
          df_spark = (spark.read.format("excel").option("dataAddress", dataAddress)
                                                .option("header", "true")
                                                .option("maxRowsInMemory", 10000)
                                                .option("maxByteArraySize", 2147483647)
                                                .option("tempFileThreshold", 10000000)
                                                .load(file_path)
                                                )

          df = df.unionByName(df_spark, allowMissingColumns=True)
        except Exception as e:
          print(f"Error reading file: {file_path} and skip it")
  return df

file_paths = []
for file_path in base_path.glob("*.xlsx"):
  file_path = str(file_path).replace('/dbfs/', '/')
  file_paths.append(file_path)

df_sale = read_excel_chunks(file_paths=file_paths[:2], dataAddress="'Raw data'!A2", chunk_size=200)


for column in df_sale.columns:
  df_sale = df_sale.withColumnRenamed(column, clean_column_name(column))
df_sale = df_sale.pandas_api()
df_sale["billing_date"] = ps.to_datetime(df_sale["billing_date"])
df_sale = df_sale.to_spark()
# df_sale = df_sale.withColumn("billing_date", to_date(col("billing_date"), "d/M/yy"))
df_mt_sale = spark.read.format("delta").load("/mnt/adls/mt_predicted_order/mt_sale")
df_filtered = df_sale.join(df_mt_sale, 
                           on=['banner', 'billing_date', 'region', 'ulv_code'],
                           how='left_anti')

(
    df_filtered
    .write
    .partitionBy("billing_date")
    .format("delta")
    .mode("append")
    .saveAsTable("mt_predicted_order.mt_sale")

)
# display(df_sale)

# COMMAND ----------

df_filtered.count()

# COMMAND ----------

# df_sale = df_sale.withColumn("billing_date", to_date(col("billing_date")))

# df_sale.columns = [clean_column_name(col) for col in df_sale.columns]

display(df_sale)

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   distinct billing_date
# MAGIC from
# MAGIC   mt_predicted_order.mt_sale

# COMMAND ----------


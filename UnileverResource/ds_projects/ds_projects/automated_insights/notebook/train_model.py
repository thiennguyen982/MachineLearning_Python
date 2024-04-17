# Databricks notebook source
import os
import sys
sys.path.append(os.path.abspath('/Workspace/Repos/'))

from ds_vn.ds_core import data_processing_utils
import pandas as pd
import numpy as np
import pickle
import lightgbm as lgbm
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from loguru import logger
import xgboost as xgb
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql import DataFrame as PySparkDataFrame
from sklearn.multioutput import MultiOutputClassifier
from sklearn.model_selection import train_test_split

# COMMAND ----------

# MAGIC %md
# MAGIC # Load Master DF

# COMMAND ----------

def load_daily_sales(from_date: datetime, to_date:datetime):
  """
  Load daily sales within a period of time
  """
  logger.info("Load daily sales from {0} to {1}".format(from_date.strftime("%Y%m%d"), to_date.strftime("%Y%m%d")))
  daily_sales = spark.read.table("dailysales")
  daily_sales_selected_columns = [
    "transactional_outlet_code", "transactional_site_code", "transactional_distributor_code",
    "product_code", "orig_invoice_date", "net_invoice_val", "invoice_number", 
    "invoicetype", "gross_sales_val"
  ]
  daily_sales = daily_sales.select(*daily_sales_selected_columns)
  daily_sales = daily_sales.filter(F.col("invoicetype") != "ZPR")
  daily_sales = daily_sales.filter((F.col("invoice_date") >= F.lit(from_date)) & (F.col("invoice_date") <= F.lit(to_date)))
  return daily_sales

def load_outlet_migration() -> PySparkDataFrame:
  outlet_migration = spark.read.table("outletmigration")
  outlet_migration_selected_columns = [
    "prev_outlet_code", "outlet_code","site_code", "dt_code"
  ]
  outlet_migration = outlet_migration.select(*outlet_migration_selected_columns)
  outlet_migration = outlet_migration.withColumnRenamed("outlet_code", "omi_outlet_code") #omi is outlet migration
  return outlet_migration

def load_outlet_master() -> PySparkDataFrame:
  outlet_master = spark.read.table("outletmaster")
  outlet_master_selected_columns = [
    "outlet_code", "group_channel", "channel", "channel_location", "geo_region"
  ]
  outlet_master = outlet_master.select(*outlet_master_selected_columns)
  outlet_master = outlet_master.withColumnRenamed("outlet_code", "oma_outlet_code") #oma is outlet_master
  outlet_master = outlet_master.filter(F.col("group_channel") == "DT")
  outlet_master = outlet_master.filter(F.col("channel") != "1.9. UMP Migration")
  return outlet_master

def update_outletcode_dtcode_sitecode_channel(df: PySparkDataFrame) -> PySparkDataFrame:
  """
  The purpose of this function is to rename the column name after mapping old new outlet/distributor/site
  """
  df = df.withColumn("outlet_code", F.when(F.col("prev_outlet_code").isNull(), F.col("transactional_outlet_code")).otherwise(F.col("omi_outlet_code")))
  df = df.withColumnRenamed("transactional_site_code", "old_site_code")
  df = df.withColumn("new_site_code", F.when(F.col("prev_outlet_code").isNull(), F.col("old_site_code")).otherwise(F.col("site_code")))
  df = df.withColumnRenamed("transactional_distributor_code", "old_distributor_code")
  df = df.withColumn("new_distributor_code", F.when(F.col("prev_outlet_code").isNull(), F.col("old_distributor_code")).otherwise(F.col("dt_code")))
  df = df.withColumn("channel", F.when(F.col("group_channel") == "DT", F.col("channel")).otherwise(F.col("group_channel")))
  return df

def load_master_dataset(from_date: datetime, to_date:datetime):
  """
  This function is to create a master dataset for extracting the input pool as well as calculating the features for modelling
  """
  daily_sales = load_daily_sales(from_date=from_date, to_date=to_date)
  cd_product_mapping = data_processing_utils.load_vn_local_product_master()
  outlet_migration = load_outlet_migration()
  outlet_master = load_outlet_master()
  
  df = daily_sales.join(outlet_migration, on=[daily_sales["transactional_outlet_code"] == outlet_migration["prev_outlet_code"]], how="left")
  df = df.join(outlet_master, on=[df["transactional_outlet_code"] == outlet_master["oma_outlet_code"]], how="inner") # join inner due to outlet_master already filtered and consider only DT channel
  df = df.join(cd_product_mapping, on="product_code", how="inner")

  df = update_outletcode_dtcode_sitecode_channel(df)
  df = df.drop("site_code", "prev_outlet_code", "omi_outlet_code") # drop not used columns
  df = df.withColumnRenamed("new_site_code", "site_code")
  df = df.filter((F.col("small_c") != "Tea") & (F.col("site_code") != "3001"))
  logger.success("Load master dataset from {0} - {1} successfully".format(from_date.strftime("%Y%m%d"), to_date.strftime("%Y%m%d")))
  return df

# COMMAND ----------

def mark_label(df:PySparkDataFrame, date_run: datetime):
  date_run = date_run + relativedelta(day=1)
  keys = ["outlet_code", "site_code", "channel", "small_c"]
  for i in range(4):
    from_date = date_run
    to_date = from_date + relativedelta(days=6)
    master_df = load_master_dataset(from_date=from_date, to_date=to_date)
    agg = master_df.groupby(*keys).agg(F.sum("gross_sales_val").alias("sum_gsv"))
    agg = agg.filter(F.col("sum_gsv") > 0).drop("sum_gsv")
    agg = agg.withColumn("buy_next_{}w".format(i+1), F.lit(1))
    df = df.join(agg, on=keys, how="left")
    df = df.fillna(0, subset=["buy_next_{}w".format(i+1)])
    logger.info("Done from {}-{}".format(from_date.strftime("%Y%m%d"), to_date.strftime("%Y%m%d")))
    date_run += relativedelta(days=7)
  return df

# COMMAND ----------

feature_path = "dbfs:/mnt/adls/ds_vn/feature/automated_insights/{}"
date_run = datetime(2023,11,1)
df_feature = spark.read.parquet(feature_path.format(date_run.strftime("%Y%m")))
df = mark_label(df_feature, date_run)

# COMMAND ----------

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

df_pd = df.toPandas()

# COMMAND ----------

df_pd.buy_next_1w.value_counts()

# COMMAND ----------

365715/(365715 + 1441675)

# COMMAND ----------

df_pd.buy_next_2w.value_counts()

# COMMAND ----------

439880/(439880+1367510)

# COMMAND ----------

df_pd.buy_next_3w.value_counts()

# COMMAND ----------

418784/(418784+1388606)

# COMMAND ----------

df_pd.buy_next_4w.value_counts()

# COMMAND ----------

436650/(436650+1370740)

# COMMAND ----------

# MAGIC %md
# MAGIC # Build model

# COMMAND ----------

date_run = datetime(2023,8,1)
df = pd.DataFrame()
feature_path = "dbfs:/mnt/adls/ds_vn/feature/automated_insights/{}"
while date_run >= datetime(2022,9,1):
  ym = date_run.strftime("%Y%m")
  temp = spark.read.parquet(feature_path.format(ym))
  temp = mark_label(df=temp, date_run=date_run)
  temp = temp.toPandas()
  df = pd.concat([df, temp], ignore_index=True)
  date_run -= relativedelta(months=1)

# COMMAND ----------

df.head()

# COMMAND ----------

df.shape

# COMMAND ----------

channel_arr = []
small_c_arr = []
rows_arr = []
for i, j in df.groupby(["channel", "small_c"]):
  channel, small_c = i
  number_of_rows = j.shape[0]
  channel_arr.append(channel)
  small_c_arr.append(small_c)
  rows_arr.append(number_of_rows)
check_df = pd.DataFrame({"channel": channel_arr, "small_c": small_c_arr, "number_of_rows": rows_arr})

# COMMAND ----------

check_df

# COMMAND ----------

check_df.number_of_rows.describe()

# COMMAND ----------

channels = set(channel_arr)
small_c = set(small_c_arr)

# COMMAND ----------

# define features and label column
features = [
  'total_gsv_L1W', 'total_num_of_purchase_L1W', 'avg_order_value_L1W', 'avg_gsv_L6M',
  'avg_num_of_purchase_L6M', 'avg_order_value_L6M', 'recency_within_1y', 'lifetime',
  'number_of_month_have_bill'
]
label = ['buy_next_1w', 'buy_next_2w', 'buy_next_3w', 'buy_next_4w'] 

for c in channels:
  for cat in small_c:
    df_filter = df[(df['channel'] == c) & (df['small_c'] == cat)]
    logger.info("Shape of training: {}".format(df_filter.shape))
    X = df_filter[features]
    y = df_filter[label]

    lgbm_clf = lgbm.LGBMClassifier()

    multioutput_clf = MultiOutputClassifier(lgbm_clf)
    multioutput_clf.fit(X,y)
    filename = f'/dbfs/mnt/adls/ds_vn/model/automated_insights/v1/multioutput_{c}_{cat}.pkl'
    pickle.dump(multioutput_clf, open(filename, 'wb'))
    logger.success("Done training model for {} - {}".format(c, cat))

# COMMAND ----------

# MAGIC %md
# MAGIC # Model evaluation

# COMMAND ----------

def load_feature_and_label_monthly(date_run:datetime):
  feature_path = 'dbfs:/mnt/adls/ds_vn/feature/automated_insights/{}'
  ym = date_run.strftime("%Y%m")
  df = spark.read.parquet(feature_path.format(ym))
  df = mark_label(df=df, date_run=date_run)
  df = df.toPandas()
  return df

# COMMAND ----------

df_202309 = load_feature_and_label_monthly(date_run=datetime(2023,9,1))

# COMMAND ----------

df_202309.shape

# COMMAND ----------

set(df_202309.channel.unique())

# COMMAND ----------

df_202309.small_c.unique()

# COMMAND ----------

def load_model_channel_cat(channel, category):
  filename = f'/dbfs/mnt/adls/ds_vn/model/automated_insights/v1/multioutput_{channel}_{category}.pkl'
  file = open(filename,'rb')
  model = pickle.load(file)
  logger.info(f"Load model for channel: {channel} - cat: {category}")
  return model

# COMMAND ----------

features = [
  'total_gsv_L1W', 'total_num_of_purchase_L1W', 'avg_order_value_L1W', 'avg_gsv_L6M',
  'avg_num_of_purchase_L6M', 'avg_order_value_L6M', 'recency_within_1y', 'lifetime',
  'number_of_month_have_bill'
]
label = ['buy_next_1w', 'buy_next_2w', 'buy_next_3w', 'buy_next_4w'] 

def infer_action_set(df):
  channels = set(df_202309.channel.unique())
  small_c = set(df_202309.small_c.unique())
  final_df = pd.DataFrame()
  for c in channels:
    for cat in small_c:
      model = load_model_channel_cat(channel=c, category=cat)
      df_filter = df[(df['channel'] == c) & (df['small_c'] == cat)]
      df_filter = df_filter.reset_index() # reset for axis=1 concat, concat base-on index
      y_pred = model.predict(df_filter[features])
      y_pred_proba = model.predict_proba(df_filter[features])
      y_pred_df = pd.DataFrame(y_pred, columns=["pred_w1", "pred_w2", "pred_w3", "pred_w4"])
      y_pred_proba_df = pd.DataFrame(np.array(y_pred_proba)[:,:,1].T, columns=["pred_proba_w1", "pred_proba_w2", "pred_proba_w3", "pred_proba_w4"])
      temp_df = pd.concat([df_filter, y_pred_df, y_pred_proba_df], axis=1)
      final_df = pd.concat([final_df, temp_df], ignore_index=True)
      logger.info(f"Done for channel: {c} - cat: {cat}")
    
  return final_df


# COMMAND ----------

df_202309_result = infer_action_set(df_202309)

# COMMAND ----------

df_202309_result.shape

# COMMAND ----------

df_202309.shape

# COMMAND ----------

df_202309_result.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Weekly precision

# COMMAND ----------

from sklearn.metrics import precision_score, accuracy_score

# COMMAND ----------

w1_precision = precision_score(y_true=df_202309_result["buy_next_1w"], y_pred=df_202309_result["pred_w1"])
w2_precision = precision_score(y_true=df_202309_result["buy_next_2w"], y_pred=df_202309_result["pred_w2"])
w3_precision = precision_score(y_true=df_202309_result["buy_next_3w"], y_pred=df_202309_result["pred_w3"])
w4_precision = precision_score(y_true=df_202309_result["buy_next_4w"], y_pred=df_202309_result["pred_w4"])
print(f"Precision W1: {w1_precision}")
print(f"Precision W2: {w2_precision}")
print(f"Precision W3: {w3_precision}")
print(f"Precision W4: {w4_precision}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Precision by channel + small_c

# COMMAND ----------

channel_arr = []
small_c_arr = []
precision_w1_arr = []
precision_w2_arr = []
precision_w3_arr = []
precision_w4_arr = []
for i, j in df_202309_result.groupby(["channel", "small_c"]):
  channel, small_c = i
  df_filter = df_202309_result[(df_202309_result['channel'] == channel) & (df_202309_result['small_c'] == small_c)]
  w1_precision = precision_score(y_true=df_filter["buy_next_1w"], y_pred=df_filter["pred_w1"])
  w2_precision = precision_score(y_true=df_filter["buy_next_2w"], y_pred=df_filter["pred_w2"])
  w3_precision = precision_score(y_true=df_filter["buy_next_3w"], y_pred=df_filter["pred_w3"])
  w4_precision = precision_score(y_true=df_filter["buy_next_4w"], y_pred=df_filter["pred_w4"])

  channel_arr.append(channel)
  small_c_arr.append(small_c)
  precision_w1_arr.append(w1_precision)
  precision_w2_arr.append(w2_precision)
  precision_w3_arr.append(w3_precision)
  precision_w4_arr.append(w4_precision)

precision_detail_df = pd.DataFrame({"channel": channel_arr, "small_c": small_c_arr, "w1_precision": precision_w1_arr,
                                    "w2_precision": precision_w2_arr, "w3_precision": precision_w3_arr, "w4_precision": precision_w4_arr})

# COMMAND ----------

precision_detail_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Total precision

# COMMAND ----------

total_precision = np.round(precision_score(df_202309_result[label].any(axis=1),
                                           df_202309_result[['pred_w1', 'pred_w2', 'pred_w3', 'pred_w4']].any(axis=1)),2)

# COMMAND ----------

total_precision

# COMMAND ----------

# MAGIC %md
# MAGIC # Adhoc output

# COMMAND ----------

# MAGIC %md
# MAGIC This is the adhoc task to evaluate the peferomance of model as c.Phuong's requirement

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate high_low + MTD GSV

# COMMAND ----------

def load_feature_month_pyspark(date_run:datetime) -> PySparkDataFrame:
  """Just select keys for simplitcity"""
  feature_path = 'dbfs:/mnt/adls/ds_vn/feature/automated_insights/{}'
  ym = date_run.strftime("%Y%m")
  df = spark.read.parquet(feature_path.format(ym))
  df = df.select("outlet_code", "site_code", "channel", "small_c")
  return df

# COMMAND ----------

def mark_high_low(df: PySparkDataFrame, date_run:datetime) -> PySparkDataFrame:
  """
  Input is the feature df
  """
  to_date = date_run - relativedelta(day=1) - relativedelta(days=1)
  from_date = to_date - relativedelta(months=12)
  keys = ["outlet_code", "site_code", "channel", "small_c"]

  master_df = load_master_dataset(from_date=from_date, to_date=to_date)
  agg_gsv = master_df.groupby(*keys).agg((F.sum("gross_sales_val").alias("total_gsv_L12M")))
  w_rank = Window().partitionBy("channel", "small_c")
  # Calculate rank top 30% has highest contribute
  df_rank = agg_gsv.withColumn("pct_rank", F.percent_rank().over(w_rank.orderBy(F.desc("total_gsv_L12M"))))
  df_rank = df_rank.withColumn("is_high_low", F.when(F.col("pct_rank") <= 0.3, "H").otherwise("L")) # top 30%
  result = df.join(df_rank, on=keys, how="left")

  return result

# COMMAND ----------

def calculate_current_MTD_gsv(df:PySparkDataFrame, date_run:datetime):
  """
  Calculate the MTD gsv of the current month. Note that this is for evaluating performance only,
  we do not care this part in production
  """
  from_date = date_run - relativedelta(day=1)
  to_date = from_date + relativedelta(months=1) - relativedelta(days=1)

  keys = ["outlet_code", "site_code", "channel", "small_c"]
  master_df = load_master_dataset(from_date=from_date, to_date=to_date)
  agg_gsv = master_df.groupby(*keys).agg((F.sum("gross_sales_val").alias("current_MTD_gsv")))

  result = df.join(agg_gsv,on=keys, how="left")
  result = result.fillna(0, subset=["current_MTD_gsv"]) # fill na for outlet not having any sales in the month
  return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inference the result

# COMMAND ----------

from sklearn.metrics import precision_score, accuracy_score

# COMMAND ----------

def load_feature_and_label_monthly(date_run:datetime):
  feature_path = 'dbfs:/mnt/adls/ds_vn/feature/automated_insights/{}'
  ym = date_run.strftime("%Y%m")
  df = spark.read.parquet(feature_path.format(ym))
  df = mark_label(df=df, date_run=date_run)
  df = df.toPandas()
  return df

# COMMAND ----------

def load_model_channel_cat(channel, category):
  filename = f'/dbfs/mnt/adls/ds_vn/model/automated_insights/v1/multioutput_{channel}_{category}.pkl'
  file = open(filename,'rb')
  model = pickle.load(file)
  logger.info(f"Load model for channel: {channel} - cat: {category}")
  return model

# COMMAND ----------

features = [
  'total_gsv_L1W', 'total_num_of_purchase_L1W', 'avg_order_value_L1W', 'avg_gsv_L6M',
  'avg_num_of_purchase_L6M', 'avg_order_value_L6M', 'recency_within_1y', 'lifetime',
  'number_of_month_have_bill'
]
label = ['buy_next_1w', 'buy_next_2w', 'buy_next_3w', 'buy_next_4w'] 

def infer_action_set(df: pd.DataFrame) -> pd.DataFrame:
  channels = set(df.channel.unique())
  small_c = set(df.small_c.unique())
  final_df = pd.DataFrame()
  for c in channels:
    for cat in small_c:
      model = load_model_channel_cat(channel=c, category=cat)
      df_filter = df[(df['channel'] == c) & (df['small_c'] == cat)]
      df_filter = df_filter.reset_index() # reset for axis=1 concat, concat base-on index
      y_pred = model.predict(df_filter[features])
      y_pred_proba = model.predict_proba(df_filter[features])
      y_pred_df = pd.DataFrame(y_pred, columns=["pred_w1", "pred_w2", "pred_w3", "pred_w4"])
      y_pred_proba_df = pd.DataFrame(np.array(y_pred_proba)[:,:,1].T, columns=["pred_proba_w1", "pred_proba_w2", "pred_proba_w3", "pred_proba_w4"])
      temp_df = pd.concat([df_filter, y_pred_df, y_pred_proba_df], axis=1)
      final_df = pd.concat([final_df, temp_df], ignore_index=True)
      logger.info(f"Done for channel: {c} - cat: {cat}")
    
  return final_df


# COMMAND ----------

def mark_is_predicted_correctly_inthe_month(df:pd.DataFrame):
  """Consider both 1==1 and 0==0 is True"""
  label_cols = ['buy_next_1w', 'buy_next_2w', 'buy_next_3w', 'buy_next_4w'] 
  predicted_cols = ['pred_w1', 'pred_w2', 'pred_w3', 'pred_w4']
  df["is_buying_in_month"] = df[label_cols].sum(axis=1) > 0
  df["is_predicted_buying_in_month"] = df[predicted_cols].sum(axis=1) > 0
  df["is_predicted_correctly"] = df["is_buying_in_month"] == df["is_predicted_buying_in_month"]
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Predicted true contribution

# COMMAND ----------

def calculate_predicted_correct_contribution(df:pd.DataFrame):
  """
  Calculate the contribution of outlet the predicted correctly for the month
  Rule: Predicted correctly means: aggregate to monthly level + do not consider the match for weekly correction in prediction
  """
  agg1 = df.groupby(["channel", "small_c", "is_high_low","is_predicted_correctly"])["current_MTD_gsv"].sum().reset_index()
  agg1.rename(columns={"current_MTD_gsv": "sum_mtd_gsv_by_corect"}, inplace=True)
  agg2 = df.groupby(["channel", "small_c", "is_high_low"])["current_MTD_gsv"].sum().reset_index()
  agg2.rename(columns={"current_MTD_gsv": "sum_mtd_gsv"}, inplace=True)

  agg = pd.merge(agg1, agg2, on=["channel", "small_c", "is_high_low"])
  agg["predicted_correct_contribution"] = agg["sum_mtd_gsv_by_corect"]/agg["sum_mtd_gsv"]
  result = pd.merge(df, agg, on=["channel", "small_c", "is_high_low", "is_predicted_correctly"])
  return result

# COMMAND ----------

def agg_output_adhoc(date_run:datetime) -> pd.DataFrame:
  df_feature = load_feature_month_pyspark(date_run=date_run)
  df = mark_high_low(df=df_feature, date_run=date_run)
  df = calculate_current_MTD_gsv(df=df, date_run=date_run)
  df = df.toPandas()

  df_feature_pd = load_feature_and_label_monthly(date_run=date_run)
  inference_result = infer_action_set(df=df_feature_pd)
  inference_result2 = mark_is_predicted_correctly_inthe_month(inference_result)
  result = pd.merge(df, inference_result2, on=["outlet_code", "site_code", "channel", "small_c"])
  result_v2 = calculate_predicted_correct_contribution(result)
  return result_v2

# COMMAND ----------

result_v2.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate report

# COMMAND ----------

def create_weekly_and_total_precision(df:pd.DataFrame) -> pd.DataFrame:
  """
  Looping to create the precision for each week
  """
  channel_arr = []
  small_c_arr = []
  high_low_arr = []
  precision_w1_arr = []
  precision_w2_arr = []
  precision_w3_arr = []
  precision_w4_arr = []
  precision_total_arr = []
  for i, j in df.groupby(["channel", "small_c", "is_high_low"]):
    channel, small_c, high_low = i
    df_filter = df[(df['channel'] == channel) & (df['small_c'] == small_c) & (df['is_high_low'] == high_low)]
    w1_precision = precision_score(y_true=df_filter["buy_next_1w"], y_pred=df_filter["pred_w1"])
    w2_precision = precision_score(y_true=df_filter["buy_next_2w"], y_pred=df_filter["pred_w2"])
    w3_precision = precision_score(y_true=df_filter["buy_next_3w"], y_pred=df_filter["pred_w3"])
    w4_precision = precision_score(y_true=df_filter["buy_next_4w"], y_pred=df_filter["pred_w4"])
    # precision_total = np.round(
    #   precision_score(df[label].any(axis=1), df[['pred_w1', 'pred_w2', 'pred_w3', 'pred_w4']].any(axis=1)),2
    # )
    precision_total = precision_score(y_true=df_filter["is_buying_in_month"], y_pred=df_filter["is_predicted_buying_in_month"])

    channel_arr.append(channel)
    small_c_arr.append(small_c)
    high_low_arr.append(high_low)
    precision_w1_arr.append(w1_precision)
    precision_w2_arr.append(w2_precision)
    precision_w3_arr.append(w3_precision)
    precision_w4_arr.append(w4_precision)
    precision_total_arr.append(precision_total)

  precision_detail_df = pd.DataFrame(
    {
      "channel": channel_arr, "small_c": small_c_arr, "is_high_low": high_low_arr, "w1_precision": precision_w1_arr,
      "w2_precision": precision_w2_arr, "w3_precision": precision_w3_arr, "w4_precision": precision_w4_arr,
      "total_precision": precision_total_arr
    }
  )
  return precision_detail_df

# COMMAND ----------

def create_gsv_and_gsv_correct_contribution(df:pd.DataFrame) -> pd.DataFrame:
  agg = df.groupby(["channel", "small_c", "is_high_low"]).agg({
    "current_MTD_gsv": "sum",
    "predicted_correct_contribution": "mean"

  }
  ).reset_index()
  agg.rename(columns={"predicted_correct_contribution": "predicted_correct_MTD_gsv_contribution"}, inplace=True)
  return agg


# COMMAND ----------

def generate_report(df:pd.DataFrame) -> pd.DataFrame:
  """
  Aggregated to channel + small_c + high_low level
  """
  result1 = create_weekly_and_total_precision(df=df)
  result2 = create_gsv_and_gsv_correct_contribution(df=df)
  final_result = pd.merge(result1, result2, on=["channel", "small_c", "is_high_low"])
  final_result["ym"] = date_run.strftime("%Y%m")
  return final_result

# COMMAND ----------

def run_main(date_run:datetime) -> pd.DataFrame:
  master_df = agg_output_adhoc(date_run=date_run)
  report = generate_report(master_df)
  report['ym'] = date_run.strftime("%Y%m")
  report.to_csv("/Workspace/Users/pham-anh.dung@unilever.com/automated_insights/tmp_output/report_{}".format(date_run.strftime("%Y%m")), index=False)
  logger.success("Generate the report for {} successfully".format(date_run.strftime("%Y%m")))

# COMMAND ----------

date_run = datetime(2023,9,1)
run_main(date_run)

# COMMAND ----------

date_run = datetime(2023,11,1)
run_main(date_run)

# COMMAND ----------

import pandas as pd
df1 = pd.read_csv("/Workspace/Users/pham-anh.dung@unilever.com/automated_insights/tmp_output/report_202309")
df2 = pd.read_csv("/Workspace/Users/pham-anh.dung@unilever.com/automated_insights/tmp_output/report_202310")
df3 = pd.read_csv("/Workspace/Users/pham-anh.dung@unilever.com/automated_insights/tmp_output/report_202311")

# COMMAND ----------

df = pd.concat([df1, df2, df3], ignore_index=True)

# COMMAND ----------

df.head()

# COMMAND ----------

df.to_csv("/Workspace/Users/pham-anh.dung@unilever.com/automated_insights/tmp_output/report_combine_2023091011", index=False)

# COMMAND ----------

display(df)

# COMMAND ----------


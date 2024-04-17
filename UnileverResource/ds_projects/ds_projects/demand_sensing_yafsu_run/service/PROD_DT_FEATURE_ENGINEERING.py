# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

from pyspark.sql.functions import explode, split, col, dayofweek, to_date, when, count, upper, concat, lit, countDistinct, collect_set, size, collect_list, row_number
from pyspark.sql.window import Window
from pyspark.conf import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Row, Window, DataFrame, SparkSession
from pyspark.sql.types import *
from sklearn.preprocessing import LabelEncoder
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC # LOAD CAL DATA

# COMMAND ----------

#load cal data
df_datetime = pd.read_excel("/dbfs/mnt/adls/COMPLIANCE_MANUAL_ANALYSIS/calendar.xlsx", header = 0, sheet_name = "BASEWEEK-CALENDAR")

win_holiday = Window().orderBy("YEARWEEK")

win_quarter = Window().partitionBy("YEARQUARTER").orderBy("YEARQUARTER")

df_datetime = spark.createDataFrame(df_datetime).withColumnRenamed("WEEKORDER", "WEEK_ORDER_MONTH").withColumn("IS_HOLIDAY_WEEK", F.when(F.col("HOLIDAYNAME") == "NORMAL", False).otherwise(True)).withColumn("WAS_LAST_WEEK_HOLIDAY", F.lag(F.col("IS_HOLIDAY_WEEK"), 1).over(win_holiday)).withColumn("IS_NEXT_WEEK_HOLIDAY", F.lead(F.col("IS_HOLIDAY_WEEK"), 1).over(win_holiday)).withColumn("WEEK_ORDER_QUARTER", F.row_number().over(win_quarter)).select("YEARWEEK", "QUARTER","MONTH", "WEEK_ORDER_QUARTER", "WEEK_ORDER_MONTH", "HOLIDAYNAME", "WEEKTYPE", "IS_HOLIDAY_WEEK", "WAS_LAST_WEEK_HOLIDAY", "IS_NEXT_WEEK_HOLIDAY").dropna()

display(df_datetime)

# COMMAND ----------

display(calData)

# COMMAND ----------

# MAGIC %md
# MAGIC # LOAD PRO CHANNEL DATA

# COMMAND ----------

#load pro channel data
channel_data_link = "/mnt/adls/DT_PREDICTEDORDER/LE_SEC_SALES_DEPENDENT_FILES/promotion_pro.csv" #xin link
channel_data = spark.read.csv(channel_data_link, header = True)

#rule out only pro channel promo IDs
channel_data = channel_data.fillna({"Pro Channel ?": "TBD"})
exclude_only_pro = channel_data.filter(~(col("Pro Channel ?") == "Only Pro Channel"))


# COMMAND ----------

# display(exclude_only_pro)

# COMMAND ----------

# MAGIC %md
# MAGIC # LOAD PROMOTION DATA
# MAGIC

# COMMAND ----------

dt_promotion = spark.read.parquet("/mnt/adls/DT_PREDICTEDORDER/PROMOTION_TIMING")

dt_promotion = dt_promotion.drop(col("Category"))
# dt_promotion = dt_promotion.filter(col("Banded") == "No")
dt_promotion = dt_promotion.filter((col("SKU").isNotNull()))
dt_promotion = (
    dt_promotion.withColumnRenamed("SKU", "MATERIAL")
    .withColumnRenamed("Week", "YEARWEEK")
    .withColumn("BANNER", F.trim(F.upper(F.col("banner"))))
    .withColumn("DPNAME", F.trim(F.upper(F.col("DP Name"))))
    .withColumn(
        "YEARWEEK",
        F.when(F.col("YEARWEEK") == 202100, 202053).otherwise(
            F.when(F.col("YEARWEEK") == 202200, 202152).otherwise(
                F.when(F.col("YEARWEEK") == 202300, 202252).otherwise(F.col('YEARWEEK'))
            )
        )
    )
    .drop("Week", "DP Name")
)
dt_promotion = dt_promotion.where(" `Discount type` is not null")

dt_promotion = dt_promotion.join(
    exclude_only_pro, on=["Promotion ID"], how="inner"
).join(df_datetime, on=["YEARWEEK"], how="full")

# COMMAND ----------

display(dt_promotion)

# COMMAND ----------

display(dt_promotion.select("Promotion type").distinct())

# COMMAND ----------

display(dt_promotion.select("Discount Type").distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC # ONE HOT DF's

# COMMAND ----------

df_discount_type_agg = dt_promotion.groupby(
    "YEARWEEK", "DPNAME", "BANNER", "Discount Type"
).agg(
    F.countDistinct(F.col("Promotion ID")).alias("scheme_unique_count"),
    F.count(F.col("Promotion ID")).alias("scheme_count"),
)

df_discount_type_agg = (
    df_discount_type_agg.withColumn(
        "multi_scheme_promotion",
        F.when(
            (F.col("scheme_unique_count") == 1) & (F.col("scheme_count") == 1), False
        ).otherwise(True),
    )
    .withColumn(
        "multi_scheme_promotion_type",
        F.when(
            F.col("multi_scheme_promotion") == False,
            F.concat(F.lit("SINGLE_"), F.col("Discount Type")),
        ).otherwise(
            F.when(
                (F.col("scheme_unique_count") == 1) & (F.col("scheme_count") > 1),
                F.concat(F.lit("PROGRESSIVE_"), F.col("Discount Type")),
            ).otherwise(
                F.when(
                    (F.col("scheme_unique_count") < F.col("scheme_count")),
                    F.concat(F.lit("HYBRID_"), F.col("Discount Type")),
                ).otherwise(F.concat(F.lit("ON_TOP_"), F.col("Discount Type")))
            )
        ),
    )
    .withColumn(
        "scheme_type",
        F.when(
            F.col("multi_scheme_promotion_type").like("%HYBRID%"), "HYBRID"
        ).otherwise(
            F.when(
                F.col("multi_scheme_promotion_type").like("%SINGLE%"), "SINGLE"
            ).otherwise(
                F.when(
                    F.col("multi_scheme_promotion_type").like("%ON_TOP%"), "ON_TOP"
                ).otherwise(
                    F.when(
                        F.col("multi_scheme_promotion_type").like("%PROGRESSIVE%"),
                        "PROGRESSIVE"
                    )
                )
            )
        )
    )
)

# display(df_discount_type_agg)

# COMMAND ----------

# display(df_discount_type_agg.where(F.col("multi_scheme_promotion_type").like("%PROGRESSIVE%")))

# COMMAND ----------

# display(df_discount_type_agg.where(F.col("multi_scheme_promotion_type").like("%ON_TOP%")))

# COMMAND ----------

# display(df_discount_type_agg.where(F.col("multi_scheme_promotion_type").like("%HYBRID%")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ONE HOT DISCOUNT TYPE

# COMMAND ----------

# Pivot the table
df_discount_type = (
    df_discount_type_agg.groupby("YEARWEEK", "DPNAME", "BANNER")
    .pivot("Discount Type")
    .sum("scheme_unique_count")
    .fillna(0)
    .withColumn(
        "NUMBER_OF_PROMO",
        F.col("NON-U GIFT") + F.col("PERCENTAGE") + F.col("U GIFT") + F.col("VALUE"),
    )
)


# display(df_discount_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ONE HOT SCHEME TYPE

# COMMAND ----------

# df_scheme_type = df_discount_type_agg.groupby("YEARWEEK", "DPNAME", "BANNER", "multi_scheme_promotion_type").agg(F.count(F.col("multi_scheme_promotion_type")).alias("scheme_type_count"))

# df_scheme_type = df_scheme_type.groupby("YEARWEEK", "DPNAME", "BANNER").pivot("multi_scheme_promotion_type").sum("scheme_type_count").fillna(0)

# display(df_scheme_type)

# COMMAND ----------

def cat_var_one_hot(var_name):
    """
    Function to one hot encode categorical variables, countDistinct
    """
    list_groupby_og = ["YEARWEEK", "DPNAME", "BANNER"]
    list_groupby_custom = list_groupby_og + [var_name]
    var_count_name = var_name + "_count"
    df_type = dt_promotion.groupby(list_groupby_custom).agg(
        F.countDistinct(F.col(var_name)).alias(var_count_name)
    )
    df_type = (
        df_type.groupby(list_groupby_og).pivot(var_name).sum(var_count_name).fillna(0)
    )
    return df_type

# COMMAND ----------

# MAGIC %md
# MAGIC ## ONE HOT PROMOTION TYPE

# COMMAND ----------

dt_promotion.columns

# COMMAND ----------

var_name = "Promotion type"
list_groupby_custom = ["YEARWEEK", "DPNAME", "BANNER", "Promotion ID", var_name]
var_count_name = "promo_type" + "_count"
df_type = dt_promotion.groupby(list_groupby_custom).agg(
    F.countDistinct(F.col(var_name)).alias(var_count_name)
)
df_promotion_type = (
    df_type.groupby(["YEARWEEK", "DPNAME", "BANNER"])
    .pivot(var_name)
    .sum(var_count_name)
    .fillna(0)
)


# display(df_promotion_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ONE HOT WEEK TYPE

# COMMAND ----------

df_week_type = cat_var_one_hot("WEEKTYPE")

# display(df_week_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ONE HOT HOLIDAY

# COMMAND ----------

df_holiday_type = cat_var_one_hot("IS_HOLIDAY_WEEK")

# display(df_holiday_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ONE HOT BUY TYPE 

# COMMAND ----------

var_name = "Buy type"
list_groupby_custom = ["YEARWEEK", "DPNAME", "BANNER", "Promotion ID", var_name] 
var_count_name = "promo_type" + "_count"
df_type = dt_promotion.groupby(list_groupby_custom).agg(F.countDistinct(F.col(var_name)).alias(var_count_name))
df_buy_type = df_type.groupby(["YEARWEEK", "DPNAME", "BANNER"]).pivot(var_name).sum(var_count_name).fillna(0).withColumnRenamed("VALUE","BUY_VALUE").withColumnRenamed("VOLUME", "BUY_VOLUME")


# display(df_buy_type)

# COMMAND ----------

def num_var_one_hot(var_name):
    """
    Function to take avg, max and min of numeric variables
    """
    list_groupby = ["YEARWEEK", "DPNAME", "BANNER"]
    df_type = (
        dt_promotion.groupby(list_groupby)
        .agg(F.min(F.col(var_name)), 
             F.max(F.col(var_name)), 
             F.mean(F.col(var_name)))
        .fillna(0)
    )
    return df_type

# COMMAND ----------

# MAGIC %md
# MAGIC ## ONE HOT VALUE DISCOUNT

# COMMAND ----------

df_value_discount = num_var_one_hot("Value Discount")

# display(df_value_discount)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ONE HOT % DISCOUNT

# COMMAND ----------

df_percent_discount = num_var_one_hot("% Discount")

# display(df_percent_discount)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ONE HOT MINIMUM BUY

# COMMAND ----------


'''
STATUS: PENDING
'''

# df_test = (
#     dt_promotion.groupby("YEARWEEK", "BANNER", "MATERIAL", "Buy type")
#     .agg(F.min(F.col("Minimum Buy")), 
#          F.max(F.col("Minimum Buy")), 
#          F.mean(F.col("Minimum Buy")))
#     .fillna(0)
# )

# df_test_min = df_test.groupby("YEARWEEK", "BANNER", "MATERIAL").pivot("Buy type").sum("min(Minimum Buy)").fillna(0).withColumnsRenamed({"VALUE":"min_minimum_buy"})

# display(df_test_min)

# COMMAND ----------

# MAGIC %md
# MAGIC # JOIN ALL FEATURE DATASETS

# COMMAND ----------

list_features = [df_discount_type, df_promotion_type, df_week_type, df_holiday_type, df_buy_type, df_value_discount, df_percent_discount]

df_features_full = df_discount_type

for feature_df in list_features[1:]:
  df_features_full = df_features_full.join(feature_df, on = ["DPNAME", "BANNER", "YEARWEEK"], how = "outer") # 

df_features_full = df_features_full.drop("null")

# COMMAND ----------

display(df_features_full)

# COMMAND ----------

from collections import Counter

df_features_full_pd = df_features_full.toPandas()

# Sample list
my_list = df_features_full_pd.columns

# Use Counter to count occurrences
element_counts = Counter(my_list)

# Print the result
print(element_counts)

# COMMAND ----------

df_features_full_pd = df_features_full.toPandas()
df_features_full_pd.to_parquet("/dbfs/mnt/adls/DT_PREDICTEDORDER/LE_SEC_SALE_DAILY/DT_PROMOTION_FEATURES_DP_BANNER_YW.parquet")

# COMMAND ----------

# Perform one-hot encoding
# df_encoded = pd.get_dummies(dt_promotion_one_sku_pd['Promotion type'], prefix='has')

# # Group by 'CATEGORaY' and sum the one-hot encoded columns
# df_encoded = df_encoded.groupby(dt_promotion_one_sku_pd[['MATERIAL', 'YEARWEEK']]).sum().reset_index()

# # Concatenate the one-hot encoded columns with the original DataFrame
# df = df.merge(df_encoded, on = ["MATERIAL", "YEARWEEK"], how = "left")

# display(df_encoded)

# COMMAND ----------

# DBTITLE 1,XGBOOST
# import pandas as pd
# import numpy as np
# import matplotlib.pyplot as plt
# import seaborn as sns
# import xgboost as xgb
# from sklearn.metrics import mean_squared_error
# color_pal = sns.color_palette()
# from sklearn.preprocessing import LabelEncoder



# COMMAND ----------

# train = data.loc[data_visual["YEARWEEK"] <= 202314]
# test = data.loc[data_visual["YEARWEEK"] > 202314]

# fig, ax = plt.subplots(figsize=(5, 5))

# ax.set_ylim(0, 10)

# train.plot(ax=ax, label='Training Set', title='Data Train/Test Split')
# test.plot(ax=ax, label='Test Set')
# ax.axvline('202314', color='black', ls='--')
# ax.legend(['Training Set', 'Test Set'])
# plt.show()

# COMMAND ----------

# train = data_new.loc[data["YEARWEEK"].astype(int) <= 202314]
# test = data_new.loc[data["YEARWEEK"].astype(int) > 202314]

# FEATURES =  ['BANNER_DUMMY',  'CATEGORY_DUMMY',  'WEEKTYPE_DUMMY', 'IS_HOLIDAY_DUMMY', 
#        '#PROMO',  'isMULTI_PERCENT_DISCOUNT_DUMMY',
#        'percent_promoID_count', 
#        'PERCENT_DISCOUNT_TYPE_DUMMY', 'PROMOTION_TYPE_LIST_DUMMY', 'BUY_TYPE_LIST_DUMMY',
#        'DISCOUNT_TYPE_LIST_DUMMY']
# TARGET = "CS_Week"

# X_train = train[FEATURES]
# y_train = train[TARGET]

# X_test = test[FEATURES]
# y_test = test[TARGET]

# COMMAND ----------

# reg = xgb.XGBRegressor(base_score=0.5, booster='gbtree',    
#                        n_estimators=1000,
#                        early_stopping_rounds=50,
#                        objective='reg:linear',
#                        max_depth=3,
#                        learning_rate=0.01)
# reg.fit(X_train, y_train,
#         eval_set=[(X_train, y_train), (X_test, y_test)],
#         verbose=100)

# COMMAND ----------

# fi = pd.DataFrame(data=reg.feature_importances_,
#              index=reg.feature_names_in_,
#              columns=['importance'])
# fi.sort_values('importance').plot(kind='barh', title='Feature Importance')
# plt.show()

# COMMAND ----------

from datetime import date
run_year = date.today().isocalendar().year
run_week_order = date.today().isocalendar().week
run_yearweek = run_year*100 + run_week_order
print(run_yearweek)

# COMMAND ----------


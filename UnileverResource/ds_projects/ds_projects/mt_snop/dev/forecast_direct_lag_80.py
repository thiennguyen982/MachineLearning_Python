# Databricks notebook source
# MAGIC %pip install ray openpyxl catboost

# COMMAND ----------

import pandas as pd
import numpy as np
import ray
import catboost as cb
import matplotlib.pyplot as plt

import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

ray.init()

# COMMAND ----------

# MAGIC %matplotlib inline

# COMMAND ----------

df_sales_original = spark.read.parquet('/mnt/adls/MT_FC/DATA/TMP/231222_HANA_PRIMARY.parquet')

# COMMAND ----------

df_sales = df_sales_original

# COMMAND ----------

to_drop = (df_sales.groupBy('BANNER', 'DP_NAME')
                   .agg(F.min('DATE').alias('MIN_DATE'), F.max('DATE').alias('MAX_DATE'))
                   .withColumn('DURATION', F.round(F.datediff('MAX_DATE', 'MIN_DATE') / 7))
                   .withColumn('IS_VALID', F.col('DURATION') > 88)
                   .select('BANNER', 'DP_NAME', 'IS_VALID'))

# COMMAND ----------

df_sales = (df_sales.join(to_drop, [df_sales.BANNER == to_drop.BANNER, df_sales.DP_NAME == to_drop.DP_NAME], 'left')
                    .filter(F.col('IS_VALID') == True)
                    .drop(to_drop.BANNER, to_drop.DP_NAME, 'IS_VALID')
                    .select('BANNER', 'DP_NAME', 'DATE', 'SALES_QTY'))

# COMMAND ----------

# df_sales.write.mode('overwrite').parquet('/mnt/adls/MT_FC/DATA/TMP/231226_HANA_PRIMARY_BY_DAY.parquet')
df_sales = spark.read.parquet('/mnt/adls/MT_FC/DATA/TMP/231226_HANA_PRIMARY_BY_DAY.parquet')

# COMMAND ----------

# DBTITLE 1,Explode Date
date_exploded = (df_sales.groupBy('BANNER', 'DP_NAME')
                         .agg(
                              F.min('DATE').alias('MIN_DATE'),
                              F.max('DATE').alias('MAX_DATE')
                             )
                         .withColumn('EXPLODED_DATE', F.explode(F.expr('sequence(MIN_DATE, MAX_DATE, interval 1 day)')))
                         .select('BANNER', 'DP_NAME', 'EXPLODED_DATE')
                         .withColumnRenamed('EXPLODED_DATE', 'DATE'))

df_sales = (date_exploded.join(df_sales, [date_exploded.BANNER == df_sales.BANNER, date_exploded.DP_NAME == df_sales.DP_NAME, date_exploded.DATE == df_sales.DATE], 'left')
                         .select(date_exploded.BANNER, date_exploded.DP_NAME, date_exploded.DATE, 'SALES_QTY')
                         .na.fill({'SALES_QTY': 0}))

# COMMAND ----------

# DBTITLE 1,Aggregate to Week
df_sales = (df_sales.withColumns({'YEARWEEK': F.year('DATE') * 100 + F.ceil(F.dayofyear('DATE') / 7), 'SALES_QTY': F.col('SALES_QTY').cast('int')})
                    .withColumn('SALES_QTY', F.when(F.col('SALES_QTY') < 0, 0).otherwise(F.col('SALES_QTY')))
                    .groupBy('BANNER', 'DP_NAME', 'YEARWEEK')
                    .agg(F.sum('SALES_QTY').alias('SALES_QTY')))

# COMMAND ----------

# DBTITLE 1,Save data to storage
# df_sales.write.mode('overwrite').parquet('/mnt/adls/MT_FC/DATA/TMP/231226_HANA_PRIMARY_BY_WEEK.parquet')
df_sales = spark.read.parquet('/mnt/adls/MT_FC/DATA/TMP/231226_HANA_PRIMARY_BY_WEEK.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Sinh lag features trong data MT Primary Data
# MAGIC
# MAGIC **Hướng giải quyết hiện tại:**
# MAGIC
# MAGIC - Từ data gốc -> filter ra tất cả data của 52 + 4 = 56 tuần trước đó -> union với data sinh từ ngày hiện tại cần dự đoán -> data của 57 tuần
# MAGIC
# MAGIC Vd: Data gốc có từ W-300 đến W0. Sau khi dự đoán ra W1, lấy tất cả data từ W-55 đến W0, union với data dự đoán được từ W1 -> 1 dataset từ W-55 đến W1
# MAGIC
# MAGIC - Sinh lag features + rolling features cho dataset vừa tạo. Lag lớn nhất là 52, rolling 4, vậy nên khi tạo thì chỉ có tuần vừa được dự đoán là có Lag-52 rolling 4 -> những tuần trước sẽ null vì thiếu dữ liệu -> drop null
# MAGIC
# MAGIC Vd: W1 lấy lag 52, rolling 4 sẽ là W-51 -> W-55, có data. W0 lấy lag 52, rolling 4 sẽ là W-52 -> W-56, không có data của W-56 -> bị null. Sau khi drop null thì chỉ còn lại data của W1 only.
# MAGIC
# MAGIC - Union data gốc với data vừa sinh lag - rolling để có được data mới, có tuần vừa dự đoán
# MAGIC
# MAGIC Vd: Union data gốc với data vừa sinh chỉ có W1 -> được data W-300 -> W1

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Vấn đề:** Nếu dùng cách này hay cách sinh lag features của bài BHX thì nếu data không đủ 56w thì đều sẽ bị null do không có data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Đồng ý là có thể trong tương lai 1 sản phẩm sẽ đủ 56w để sinh dữ liệu, nhưng từ hiện tại tới lúc đó vẫn không có dữ liệu để predict -> tốt nhất vẫn là model chịu được null.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Datetime Mapping

# COMMAND ----------

"""
The following code is to generate the datetime mapping for this project and it has the data at week level from 2016 to 2116 with columns:
 - YEARWEEK
 - WEEK_OF_YEAR
 - MONTH
 - QUARTER
 - YEAR
 - WEEK_OF_MONTH
"""

# datetime_mapping = (spark.createDataFrame([{'START_DATE': '2016-01-01', 'END_DATE': '2116-01-01'}])
#                         .withColumns({
#                                       'START_DATE': F.to_date('START_DATE'),
#                                       'END_DATE': F.to_date('END_DATE'),
#                                     })
#                         .withColumn('DATE', F.explode(F.expr('sequence(START_DATE, END_DATE, interval 1 day)')))
#                         .select('DATE')
#                         .withColumns({
#                                       'YEARWEEK': F.year('DATE') * 100 + F.ceil(F.dayofyear('DATE') / 7),
#                                       'UNIX_TIMESTAMP': F.unix_timestamp('DATE'),
#                                       'DAY_OF_WEEK': F.dayofweek('DATE'),
#                                       'WEEK_OF_YEAR': F.ceil(F.dayofyear('DATE') / 7),
#                                       'MONTH': F.month('DATE'),
#                                       'QUARTER': F.ceil(F.col('MONTH') / 3),
#                                       'YEAR': F.year('DATE')
#                                     })
#                         .filter(F.col('DAY_OF_WEEK') == 5)
#                         .drop('DATE', 'DAY_OF_WEEK')
#                         .orderBy('YEARWEEK'))
# datetime_mapping = datetime_mapping.withColumns({'WEEK_OF_MONTH': F.row_number().over(Window().partitionBy('YEAR', 'MONTH').orderBy('WEEK_OF_YEAR'))})
# datetime_mapping.write.mode('overwrite').parquet('/mnt/adls/MT_FC/DATA/DATETIME_MAPPING_2016_2116.parquet')

# COMMAND ----------

datetime_mapping = spark.read.parquet('/mnt/adls/MT_FC/DATA/DATETIME_MAPPING_2016_2116.parquet')

# COMMAND ----------

def datetime_map(data, datetime_mapping = datetime_mapping):
  return (data.join(datetime_mapping, data.YEARWEEK == datetime_mapping.YEARWEEK, 'left')
              .drop(datetime_mapping.YEARWEEK)
              .na.drop())

# COMMAND ----------

df_sales = datetime_map(df_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Promotion Mapping

# COMMAND ----------

promo_mapping = spark.read.parquet('/mnt/adls/MT_FC/DATA/TMP/231223_PROMO_COUNT.parquet')

# COMMAND ----------

def promo_map(data, promo_mapping = promo_mapping):
  return (data.join(promo_mapping, [data.BANNER == promo_mapping.BANNER, data.DP_NAME == promo_mapping.DP_NAME, data.YEARWEEK == promo_mapping.YEARWEEK], 'left')
              .drop(promo_mapping.BANNER, promo_mapping.DP_NAME, promo_mapping.YEARWEEK)
              .na.fill(0))

# COMMAND ----------

df_sales = promo_map(df_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Calendar Mapping

# COMMAND ----------

calendar_mapping = spark.read.parquet('/mnt/adls/MT_FC/DATA/CALENDAR_REWORK.parquet')

# COMMAND ----------

def calendar_map(data, calendar_mapping = calendar_mapping):
  return (data.join(calendar_mapping, data.YEARWEEK == calendar_mapping.YEARWEEK, 'left')
              .drop(calendar_mapping.YEARWEEK))

# COMMAND ----------

df_sales = calendar_map(df_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Product Information Mapping

# COMMAND ----------

# PRODUCT_HIERARCHY_PATH = '/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/LANDING-MASTER/Master Data Total Cat.xlsx'

# code_mapping = spark.createDataFrame(pd.read_excel(PRODUCT_HIERARCHY_PATH, sheet_name = 'Code Master'))
# dp_mapping = spark.createDataFrame(pd.read_excel(PRODUCT_HIERARCHY_PATH, sheet_name = 'DP Name Master'))

# code_mapping = (code_mapping.select('Category', 'DP name', 'NW per CS (selling-kg)')
#                             .na.drop()
#                             .withColumnsRenamed({
#                                                     'Category': 'CATEGORY',
#                                                     'DP name': 'DP_NAME',
#                                                     'NW per CS (selling-kg)': 'WEIGHT_PER_CASE'
#                                                 })
#                             .withColumns({
#                                             'CATEGORY': F.upper('CATEGORY'),
#                                             'DP_NAME': F.upper('DP_NAME')
#                                           })
#                             .dropDuplicates()
#                             .filter((~F.col('DP_NAME').contains('NOT ASSIGNED')) & (~F.col('DP_NAME').contains('DELETED')) & (~F.col('DP_NAME').contains('DELISTED')))
#                             .groupBy('CATEGORY', 'DP_NAME')
#                             .agg(F.mean('WEIGHT_PER_CASE').alias('WEIGHT_PER_CASE')))

# dp_mapping = (dp_mapping.select('Category', 'DP Name', 'DP Name Current', 'Packsize', 'Packgroup', 'Segment')
#                         .withColumnsRenamed({
#                                                 'Category': 'CATEGORY',
#                                                 'DP Name': 'DP_NAME',
#                                                 'DP Name Current': 'DP_NAME_CURRENT',
#                                                 'Packsize': 'PACK_SIZE',
#                                                 'Packgroup': 'PACK_GROUP',
#                                                 'Segment': 'SEGMENT'
#                                             })
#                         .withColumns({
#                                         'CATEGORY': F.upper('CATEGORY'),
#                                         'DP_NAME': F.upper('DP_NAME'),
#                                         'DP_NAME_CURRENT': F.upper('DP_NAME_CURRENT'),
#                                         'PACK_GROUP': F.upper('PACK_GROUP'),
#                                         'SEGMENT': F.upper('SEGMENT')
#                                       })
#                         .dropDuplicates())

# code_mapping = (code_mapping.join(dp_mapping, code_mapping.DP_NAME == dp_mapping.DP_NAME, 'left')
#                             .withColumn('DP_NAME_LATEST', F.when(~F.col('DP_NAME_CURRENT').isNull(), F.col('DP_NAME_CURRENT')).otherwise(code_mapping.DP_NAME))
#                             .drop('DP_NAME_CURRENT', code_mapping.DP_NAME, dp_mapping.DP_NAME, dp_mapping.CATEGORY)
#                             .na.drop()
#                             .withColumnRenamed('DP_NAME_LATEST', 'DP_NAME')
#                             .filter((~F.col('DP_NAME').contains('NOT ASSIGNED')) & (~F.col('DP_NAME').contains('DELETED')) & (~F.col('DP_NAME').contains('DELISTED')) & (~F.col('DP_NAME').contains('SAMPLING')))
#                             .groupBy('CATEGORY', 'SEGMENT', 'PACK_GROUP', 'DP_NAME')
#                             .agg(
#                                   F.mean('WEIGHT_PER_CASE').alias('WEIGHT_PER_CASE'),
#                                   F.mode('PACK_SIZE').alias('PACK_SIZE')
#                                 ))

# code_mapping.write.mode('overwrite').parquet('/mnt/adls/MT_FC/DATA/PRODUCT_INFO_MAPPING.parquet')

# COMMAND ----------

info_mapping = spark.read.parquet('/mnt/adls/MT_FC/DATA/PRODUCT_INFO_MAPPING.parquet')

# COMMAND ----------

def info_map(data, info_mapping = info_mapping):
  return (data.join(info_mapping, data.DP_NAME == info_mapping.DP_NAME, 'left')
              .drop(info_mapping.DP_NAME)
              .na.drop())

# COMMAND ----------

df_sales = info_map(df_sales)

# COMMAND ----------

# df_sales.write.mode('overwrite').parquet('/mnt/adls/MT_FC/DATA/TMP/231226_TMP_PRI_SALES.parquet')
df_sales = spark.read.parquet('/mnt/adls/MT_FC/DATA/TMP/231226_TMP_PRI_SALES.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Direct Flow and Forecast

# COMMAND ----------

horizon = 80

lag_rolling = [
                (horizon, 4), (horizon + 4, 4), (horizon + 8, 4),
                (horizon, 12)
              ]

week = lambda x: x * 604800

for lr in lag_rolling:
  lag = lr[0]
  rolling = lr[1]

  w = Window().partitionBy('BANNER', 'DP_NAME').orderBy('UNIX_TIMESTAMP').rangeBetween(-week(lag + rolling), -week(lag + 1))

  df_sales = (df_sales.withColumn('R_SUM_' + str(lag) + '_' + str(rolling), F.sum('SALES_QTY').over(w))
                      .withColumn('R_MEAN_' + str(lag) + '_' + str(rolling), F.avg('SALES_QTY').over(w))
                      .withColumn('R_MEDIAN_' + str(lag) + '_' + str(rolling), F.expr('percentile(SALES_QTY, 0.5)').over(w))
                      .withColumn('R_MAX_' + str(lag) + '_' + str(rolling), F.max('SALES_QTY').over(w)))

w = Window().partitionBy('BANNER', 'DP_NAME').orderBy('YEARWEEK')

lags = [horizon, horizon + 1, horizon + 2, horizon + 3, horizon + 4, horizon + 8, horizon + 12]

for i in lags:
  df_sales = df_sales.withColumn('LAG_' + str(i), F.lag('SALES_QTY', i).over(w))

df_sales = df_sales.na.drop()

# COMMAND ----------

rolling = 12

tmp = spark.read.parquet('/mnt/adls/MT_FC/DATA/TMP/231226_TMP_PRI_SALES.parquet').select('BANNER', 'DP_NAME', 'CATEGORY', 'YEARWEEK', 'UNIX_TIMESTAMP', 'SALES_QTY')

w = (Window().partitionBy('BANNER', 'CATEGORY')
             .orderBy('UNIX_TIMESTAMP')
             .rangeBetween(-week(horizon + rolling), -week(horizon + 1)))

banner_diversity = (tmp.withColumn('BANNER_DIVERSITY', F.size(F.collect_set('DP_NAME').over(w)))
                       .select('BANNER', 'CATEGORY', 'YEARWEEK', 'BANNER_DIVERSITY')
                       .withColumnsRenamed({
                                              'BANNER': 'BANNER_x',
                                              'CATEGORY': 'CATEGORY_x',
                                              'YEARWEEK': 'YEARWEEK_x'
                                           })
                       .dropDuplicates())

w = (Window().partitionBy('DP_NAME')
             .orderBy('UNIX_TIMESTAMP')
             .rangeBetween(-week(horizon + rolling), -week(horizon + 1)))

product_coverage = (tmp.withColumn('PRODUCT_COVERAGE', F.size(F.collect_set('BANNER').over(w)))
                       .select('DP_NAME', 'YEARWEEK', 'PRODUCT_COVERAGE')
                       .withColumnsRenamed({
                                              'DP_NAME': 'DP_NAME_x',
                                              'YEARWEEK': 'YEARWEEK_x'
                                           })
                       .dropDuplicates())

# COMMAND ----------

df_sales = (df_sales.join(banner_diversity, [df_sales.BANNER == banner_diversity.BANNER_x, df_sales.CATEGORY == banner_diversity.CATEGORY_x, df_sales.YEARWEEK == banner_diversity.YEARWEEK_x], 'left')
                    .drop('BANNER_x', 'CATEGORY_x', 'YEARWEEK_x')
                    .join(product_coverage, [df_sales.DP_NAME == product_coverage.DP_NAME_x, df_sales.YEARWEEK == product_coverage.YEARWEEK_x], 'left')
                    .drop('DP_NAME_x', 'YEARWEEK_x'))

# COMMAND ----------

# df_sales.write.mode('overwrite').parquet('/mnt/adls/MT_FC/DATA/TMP/231226_TMP_PRI_SALES_DIVERSITY.parquet')
df_sales = spark.read.parquet('/mnt/adls/MT_FC/DATA/TMP/231226_TMP_PRI_SALES_DIVERSITY.parquet')

# COMMAND ----------

banner_performance = (df_sales.select('BANNER', 'CATEGORY', 'YEARWEEK', 'R_SUM_80_4')
                              .groupBy('BANNER', 'CATEGORY', 'YEARWEEK')
                              .agg(
                                    F.sum(F.col('R_SUM_80_4')).alias('STORE_PERFORMANCE')
                                  )
                              .withColumnsRenamed({
                                                    'BANNER': 'BANNER_x',
                                                    'CATEGORY': 'CATEGORY_x',
                                                    'YEARWEEK': 'YEARWEEK_x'
                                                  }))

product_performance = (df_sales.select('DP_NAME', 'YEARWEEK', 'R_SUM_80_4')
                               .groupBy('DP_NAME', 'YEARWEEK')
                               .agg(
                                    F.sum(F.col('R_SUM_80_4')).alias('PRODUCT_PERFORMANCE')
                                   )
                              .withColumnsRenamed({
                                                    'DP_NAME': 'DP_NAME_x',
                                                    'YEARWEEK': 'YEARWEEK_x'
                                                  }))

# COMMAND ----------

df_sales = (df_sales.join(banner_performance, [df_sales.BANNER == banner_performance.BANNER_x, df_sales.CATEGORY == banner_performance.CATEGORY_x, df_sales.YEARWEEK == banner_performance.YEARWEEK_x], 'left')
                    .drop('BANNER_x', 'CATEGORY_x', 'YEARWEEK_x')
                    .join(product_performance, [df_sales.DP_NAME == product_performance.DP_NAME_x, df_sales.YEARWEEK == product_performance.YEARWEEK_x], 'left')
                    .drop('DP_NAME_x', 'YEARWEEK_x'))

# COMMAND ----------

# df_sales.write.mode('overwrite').parquet('/mnt/adls/MT_FC/DATA/TMP/231226_TMP_PRI_SALES_PERFORMANCE.parquet')
df_sales = spark.read.parquet('/mnt/adls/MT_FC/DATA/TMP/231226_TMP_PRI_SALES_PERFORMANCE.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # First-try Model

# COMMAND ----------

data = df_sales.toPandas()

# COMMAND ----------

upper_test_threshold = 202340
val_test_threshold = 202212
train_val_threshold = 202112

train = data[data['YEARWEEK'] < train_val_threshold].reset_index(drop = True)
val = data[data['YEARWEEK'].between(train_val_threshold, val_test_threshold - 1)].reset_index(drop = True)
test = data[data['YEARWEEK'].between(val_test_threshold, upper_test_threshold)].reset_index(drop = True)

# COMMAND ----------

model = cb.CatBoostRegressor(iterations = 100, depth = 9, learning_rate = 0.2, random_seed = 1204, thread_count = -1, loss_function = 'MAE')

cat_cols = ['BANNER', 'DP_NAME', 'WEEK_OF_YEAR', 'MONTH', 'QUARTER', 'YEAR', 'WEEK_OF_MONTH', 'HOLIDAY', 'SPECIAL_EVENT', 'CATEGORY', 'SEGMENT', 'PACK_GROUP', 'PACK_SIZE']

X_train = train.drop(columns = ['SALES_QTY', 'UNIX_TIMESTAMP'])
y_train = train['SALES_QTY']

X_val = val.drop(columns = ['SALES_QTY', 'UNIX_TIMESTAMP'])
y_val = val['SALES_QTY']

X_test = test.drop(columns = ['SALES_QTY', 'UNIX_TIMESTAMP'])
y_test = test['SALES_QTY']

# COMMAND ----------

model.fit(X_train, y_train, cat_features = cat_cols, eval_set = (X_val, y_val), plot = True)

# COMMAND ----------

fi = pd.DataFrame()
fi['FEATURE'] = X_train.columns
fi['IMPORTANCE'] = model.feature_importances_

fi.sort_values('IMPORTANCE', ascending = False)

# COMMAND ----------

data.corr()['SALES_QTY'].sort_values(ascending = False)

# COMMAND ----------

pred = model.predict(X_test)
pred_actual = pred.copy()
pred = [round(i) for i in pred]

true = list(y_test)

# COMMAND ----------

def fa(y_true, y_pred):
  sum_err = sum([abs(pred - true) for true, pred in zip(y_true, y_pred)])
  sum_act = sum(y_true)

  return 1 - sum_err / sum_act

def fb(y_true, y_pred):
  sum_err = sum([pred - true for true, pred in zip(y_true, y_pred)])
  sum_act = sum(y_true)

  return sum_err / sum_act

# COMMAND ----------

fa(list(y_test), pred)

# COMMAND ----------

# DBTITLE 1,Sum all by Month -> Acc
pred_test = test.copy()[['BANNER', 'DP_NAME', 'CATEGORY', 'YEARWEEK', 'SALES_QTY']]
pred_test['PREDICTION'] = pred

pred_test['TO_GROUP'] = pred_test['BANNER'].isin(['BACH HOA XANH', 'BIG_C', 'SAIGON COOP', 'ECUSTOMER', 'VINMART'])
pred_test['TO_GROUP'] = np.where(pred_test['TO_GROUP'], pred_test['BANNER'], 'MT OTHER')

pred_test = pred_test[pred_test['YEARWEEK'].between(202301, 202308)]
pred_test['MONTH'] = np.where(pred_test['YEARWEEK'] < 202305, 1, 2)

pred_test = pred_test.groupby('MONTH').agg({'SALES_QTY': 'sum', 'PREDICTION': 'sum'})

pred_test['FE'] = abs(pred_test['PREDICTION'] - pred_test['SALES_QTY'])
pred_test['FA'] = 1 - (pred_test['FE'] / (pred_test['SALES_QTY'] + 1))

pred_test

# COMMAND ----------

# DBTITLE 1,Sum all by Cat by Month -> Acc
pred_test = test.copy()[['BANNER', 'DP_NAME', 'CATEGORY', 'YEARWEEK', 'SALES_QTY']]
pred_test['PREDICTION'] = pred

pred_test['TO_GROUP'] = pred_test['BANNER'].isin(['BACH HOA XANH', 'BIG_C', 'SAIGON COOP', 'ECUSTOMER', 'VINMART'])
pred_test['TO_GROUP'] = np.where(pred_test['TO_GROUP'], pred_test['BANNER'], 'MT OTHER')

pred_test = pred_test[pred_test['YEARWEEK'].between(202301, 202308)]
pred_test['MONTH'] = np.where(pred_test['YEARWEEK'] < 202305, 1, 2)

pred_test = pred_test.groupby(['CATEGORY', 'MONTH']).agg({'SALES_QTY': 'sum', 'PREDICTION': 'sum'})

pred_test['FE'] = abs(pred_test['PREDICTION'] - pred_test['SALES_QTY'])
pred_test['FA'] = 1 - (pred_test['FE'] / (pred_test['SALES_QTY'] + 1))

pred_test.reset_index(inplace = True)

pred_test.pivot('CATEGORY', 'MONTH', 'FA')

# COMMAND ----------

# DBTITLE 1,Sum all by Customer by Month
pred_test = test.copy()[['BANNER', 'DP_NAME', 'CATEGORY', 'YEARWEEK', 'SALES_QTY']]
pred_test['PREDICTION'] = pred

pred_test['TO_GROUP'] = pred_test['BANNER'].isin(['BACH HOA XANH', 'BIG_C', 'SAIGON COOP', 'ECUSTOMER', 'VINMART'])
pred_test['TO_GROUP'] = np.where(pred_test['TO_GROUP'], pred_test['BANNER'], 'MT OTHER')

pred_test = pred_test[pred_test['YEARWEEK'].between(202301, 202308)]
# pred_test['MONTH'] = np.where(pred_test['YEARWEEK'] < 202305, 1, 2)

# pred_test = pred_test.groupby(['TO_GROUP', 'MONTH']).agg({'SALES_QTY': 'sum', 'PREDICTION': 'sum'})
pred_test = pred_test.groupby(['TO_GROUP']).agg({'SALES_QTY': 'sum', 'PREDICTION': 'sum'})

pred_test['FE'] = abs(pred_test['PREDICTION'] - pred_test['SALES_QTY'])
pred_test['FA'] = 1 - (pred_test['FE'] / (pred_test['SALES_QTY'] + 1))

pred_test.reset_index(inplace = True)

# pred_test.pivot('TO_GROUP', 'MONTH', 'FA')
pred_test

# COMMAND ----------

pred_test = test.copy()[['BANNER', 'DP_NAME', 'CATEGORY', 'YEARWEEK', 'SALES_QTY']]
pred_test['PREDICTION'] = pred

pred_test['TO_GROUP'] = pred_test['BANNER'].isin(['BACH HOA XANH', 'BIG_C', 'SAIGON COOP', 'ECUSTOMER', 'VINMART'])
pred_test['TO_GROUP'] = np.where(pred_test['TO_GROUP'], pred_test['BANNER'], 'MT OTHER')

pred_test = pred_test[pred_test['YEARWEEK'].between(202301, 202308)]
# pred_test['MONTH'] = np.where(pred_test['YEARWEEK'] < 202305, 1, 2)

# pred_test = pred_test.groupby(['CATEGORY', 'MONTH']).agg({'SALES_QTY': 'sum', 'PREDICTION': 'sum'})
pred_test = pred_test.groupby(['CATEGORY']).agg({'SALES_QTY': 'sum', 'PREDICTION': 'sum'})

pred_test['FE'] = abs(pred_test['PREDICTION'] - pred_test['SALES_QTY'])
pred_test['FA'] = 1 - (pred_test['FE'] / (pred_test['SALES_QTY'] + 1))

pred_test.reset_index(inplace = True)

# pred_test.pivot('CATEGORY', 'FA')
pred_test

# COMMAND ----------

pred_test = test.copy()[['BANNER', 'DP_NAME', 'CATEGORY', 'YEARWEEK', 'SALES_QTY']]
pred_test['PREDICTION'] = pred

pred_test['TO_GROUP'] = pred_test['BANNER'].isin(['BACH HOA XANH', 'BIG_C', 'SAIGON COOP', 'ECUSTOMER', 'VINMART'])
pred_test['TO_GROUP'] = np.where(pred_test['TO_GROUP'], pred_test['BANNER'], 'MT OTHER')

pred_test = pred_test[pred_test['YEARWEEK'].between(202301, 202308)]
pred_test['MONTH'] = np.where(pred_test['YEARWEEK'] < 202305, 1, 2)

pred_test = pred_test.groupby(['TO_GROUP', 'DP_NAME', 'CATEGORY', 'MONTH']).agg({'SALES_QTY': 'sum', 'PREDICTION': 'sum'})

pred_test['FE'] = abs(pred_test['PREDICTION'] - pred_test['SALES_QTY'])
pred_test['FA'] = 1 - (pred_test['FE'] / (pred_test['SALES_QTY'] + 1))

pred_test = pred_test.groupby(['TO_GROUP', 'CATEGORY', 'MONTH']).agg({'FA': 'mean'})
pred_test.reset_index(inplace = True)

pred_test.pivot(['TO_GROUP', 'CATEGORY'], 'MONTH', 'FA')

# COMMAND ----------

pred_test = test.copy()[['BANNER', 'DP_NAME', 'CATEGORY', 'YEARWEEK', 'SALES_QTY']]
pred_test['PREDICTION'] = pred

pred_test['FE'] = abs(pred_test['PREDICTION'] - pred_test['SALES_QTY'])

pred_test['TO_GROUP'] = pred_test['BANNER'].isin(['BACH HOA XANH', 'BIG_C', 'SAIGON COOP', 'ECUSTOMER', 'VINMART'])
pred_test['TO_GROUP'] = np.where(pred_test['TO_GROUP'], pred_test['BANNER'], 'MT OTHER') 

pred_test = pred_test[pred_test['YEARWEEK'].between(202301, 202309)]

pred_test = pred_test.groupby(['TO_GROUP', 'CATEGORY']).agg({'FE': 'sum', 'SALES_QTY': 'sum'})
pred_test.reset_index(inplace = True)

pred_test['FA'] = 1 - (pred_test['FE'] / pred_test['SALES_QTY'])

pred_test.pivot('TO_GROUP', 'CATEGORY', 'FA')

# COMMAND ----------

pred_test = test.copy()[['BANNER', 'DP_NAME', 'CATEGORY', 'YEARWEEK', 'SALES_QTY']]
pred_test['PREDICTION'] = pred

pred_test['FE'] = abs(pred_test['PREDICTION'] - pred_test['SALES_QTY'])

pred_test['TO_GROUP'] = pred_test['BANNER'].isin(['BACH HOA XANH', 'BIG_C', 'SAIGON COOP', 'ECUSTOMER', 'VINMART'])
pred_test['TO_GROUP'] = np.where(pred_test['TO_GROUP'], pred_test['BANNER'], 'MT OTHER')

pred_test = pred_test[pred_test['YEARWEEK'].between(202301, 202309)]

pred_test = pred_test.groupby(['TO_GROUP']).agg({'FE': 'sum', 'SALES_QTY': 'sum'})
pred_test.reset_index(inplace = True)

pred_test['FA'] = 1 - (pred_test['FE'] / pred_test['SALES_QTY'])

pred_test

# COMMAND ----------

pred_test = test.copy()[['BANNER', 'DP_NAME', 'CATEGORY', 'YEARWEEK', 'SALES_QTY']]
pred_test['PREDICTION'] = pred

pred_test['FE'] = abs(pred_test['PREDICTION'] - pred_test['SALES_QTY'])

pred_test['TO_GROUP'] = pred_test['BANNER'].isin(['BACH HOA XANH', 'BIG_C', 'SAIGON COOP', 'ECUSTOMER', 'VINMART'])
pred_test['TO_GROUP'] = np.where(pred_test['TO_GROUP'], pred_test['BANNER'], 'MT OTHER')

pred_test = pred_test[pred_test['YEARWEEK'].between(202301, 202309)]

pred_test = pred_test.groupby(['CATEGORY']).agg({'FE': 'sum', 'SALES_QTY': 'sum'})
pred_test.reset_index(inplace = True)

pred_test['FA'] = 1 - (pred_test['FE'] / pred_test['SALES_QTY'])

pred_test

# COMMAND ----------

df_promo
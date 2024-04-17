# Databricks notebook source
# MAGIC %pip install ray openpyxl catboost

# COMMAND ----------

# General
import pandas as pd
from pandas.tseries.offsets import DateOffset
import numpy as np
from tqdm.auto import tqdm
from pyspark.sql.types import DoubleType

# Parallel
import ray
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Model
import catboost as cb

# COMMAND ----------

pd.options.display.float_format = "{:,.2f}".format

if not ray.is_initialized():
  ray.init()

# COMMAND ----------

# MAGIC %run ../Modules/CONFIG

# COMMAND ----------

# MAGIC %run ../Modules/PRE-PROCESSING

# COMMAND ----------

def null_check():
    for col in date_exploded.columns:
        print(f"{col}: {date_exploded.filter(F.col(col).isNull()).count()}")


def check():
    print(f"Size: {date_exploded.count()}")
    print(
        f"ID Num: {date_exploded.select('DC_ID', 'DP_NAME').dropDuplicates().count()}"
    )
    date_exploded.agg(F.min("DATE"), F.max("DATE")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Read Data

# COMMAND ----------

read_data(SALES_OUT_PATH, PRODUCT_HIERARCHY_PATH, PRODUCT_MASTER_PATH)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW product_code AS
# MAGIC SELECT 
# MAGIC       customer_sku_code, 
# MAGIC       sku_code
# MAGIC FROM (
# MAGIC       SELECT 
# MAGIC             customer_sku_code, 
# MAGIC             sku_code, 
# MAGIC             ROW_NUMBER() OVER (PARTITION BY customer_sku_code ORDER BY sku_code) AS `num`
# MAGIC       FROM product_mapping
# MAGIC       WHERE customer_code = 'CUS-BHX'
# MAGIC       ORDER BY customer_sku_code
# MAGIC      )
# MAGIC WHERE `num` = 1;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW tmp_data AS
# MAGIC SELECT 
# MAGIC       T1.STORE_ID, 
# MAGIC       T1.STORE_NAME, 
# MAGIC       T1.PRODUCT_ID, 
# MAGIC       T1.PRODUCT_NAME, 
# MAGIC       T1.SALES_QTY, 
# MAGIC       T1.`DATE`, 
# MAGIC       T2.sku_code AS SKUID
# MAGIC FROM sales_df AS T1
# MAGIC LEFT JOIN product_code AS T2
# MAGIC ON T1.PRODUCT_ID = T2.customer_sku_code;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW product_master_rework AS
# MAGIC SELECT *
# MAGIC FROM product_master
# MAGIC WHERE `Small C` NOT IN ('Ice Cream');
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW sales_data AS
# MAGIC SELECT * 
# MAGIC FROM (
# MAGIC       SELECT 
# MAGIC             T1.*, 
# MAGIC             T2.`CD DP Name` AS DP_NAME,
# MAGIC             T2.`Small C` AS CATEGORY
# MAGIC       FROM tmp_data AS T1
# MAGIC       LEFT JOIN product_master_rework AS T2
# MAGIC       ON T1.SKUID = T2.Material
# MAGIC      )
# MAGIC WHERE (SKUID IS NOT NULL) AND (CATEGORY IS NOT NULL) AND (STORE_NAME NOT LIKE ('% Kho %'));

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # DC Mapping

# COMMAND ----------

dc_mapping = (pd.read_excel('/dbfs/mnt/adls/BHX_FC/FBFC/DATA/STORE_DC_MAPPING.xlsx', sheet_name = 'Phân bổ', header = 1)
                .rename(columns = {
                                    'MST': 'STORE_ID',
                                    'Mã DC': 'DC_ID',
                                    'Tên DC': 'DC_NAME',
                                  }))

dc_mapping = dc_mapping[['STORE_ID', 'DC_ID', 'DC_NAME']]

# COMMAND ----------

dc_mapping = spark.createDataFrame(dc_mapping)

# COMMAND ----------

dc_mapping = (dc_mapping.withColumn('DC_PROVINCE', F.substring('DC_NAME', 5, 3))
                        .withColumn('DC_DISTRICT', F.substring('DC_NAME', 9, 3))
                        .drop('DC_NAME')
                        .withColumnRenamed('STORE_ID', 'STORE_ID_x'))

# COMMAND ----------

data = (spark.sql('SELECT * FROM sales_data')
             .select('STORE_ID', 'STORE_NAME', 'DP_NAME', 'CATEGORY', 'DATE', 'SALES_QTY')
             .withColumn('SALES_QTY', F.when(F.col('SALES_QTY') < 0, 0).otherwise(F.col('SALES_QTY')))
             .orderBy('STORE_ID', 'DP_NAME', 'DATE')
             .withColumn('SALES_QTY', F.col('SALES_QTY').cast('int')))
data = (data.join(dc_mapping, data.STORE_ID == dc_mapping.STORE_ID_x, 'left')
            .drop('STORE_ID_x'))

# COMMAND ----------

data = data.na.drop()

# COMMAND ----------

data = data.groupBy('DC_ID', 'DC_PROVINCE', 'DC_DISTRICT', 'DP_NAME', 'CATEGORY', 'DATE').agg(F.sum('SALES_QTY').alias('SALES_QTY')).orderBy('DC_ID', 'DP_NAME', 'DATE')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Historical Features

# COMMAND ----------

# data.write.mode('overwrite').parquet('/mnt/adls/BHX_FC/FBFC/DAILY/TEMP_FILES/231121_TMP_DATA_DC.parquet')
data = spark.read.parquet('/mnt/adls/BHX_FC/FBFC/DAILY/TEMP_FILES/231121_TMP_DATA_DC.parquet')

# COMMAND ----------

def custom_median(*args):
  return float(np.median(list(args)))

udf_median = F.udf(custom_median, DoubleType())

# COMMAND ----------

sales_original = data.select('DC_ID', 'DP_NAME', 'DATE', 'SALES_QTY')

date_exploded = (sales_original.groupBy(['DC_ID', 'DP_NAME'])
                               .agg(
                                      F.min('DATE').alias('MIN_DATE'),
                                      F.max('DATE').alias('MAX_DATE')
                                   )
                               .withColumn('EXPLODED_DATE', F.explode(F.expr('sequence(MIN_DATE, MAX_DATE, interval 1 day)'))))

sales_original_tmp = (sales_original.withColumnRenamed('DC_ID', 'DC_ID_x')
                                    .withColumnRenamed('DP_NAME', 'DP_NAME_x'))

cond = [date_exploded.DC_ID == sales_original_tmp.DC_ID_x, date_exploded.DP_NAME == sales_original_tmp.DP_NAME_x, date_exploded.EXPLODED_DATE == sales_original_tmp.DATE]
date_exploded = (date_exploded.join(sales_original_tmp, cond, 'left')
                              .select('DC_ID', 'DP_NAME', 'EXPLODED_DATE', 'SALES_QTY')
                              .withColumnRenamed('EXPLODED_DATE', 'DATE')
                              .na.fill({"SALES_QTY": 0})
                              .orderBy(["DC_ID", "DP_NAME", "DATE"]))

# COMMAND ----------

lag_rolling = [
                (7, 7), (14, 7), (21, 7), (28, 7),
                (7, 30), (37, 30), (67, 30)
              ]

date_exploded = date_exploded.withColumn('UNIX_TIMESTAMP', F.unix_timestamp('DATE', 'yyyy-MM-dd'))

for lr in lag_rolling:
  lag = lr[0]
  rolling = lr[1]

  w = Window().partitionBy('DC_ID', 'DP_NAME').orderBy('UNIX_TIMESTAMP').rangeBetween(-(lag + rolling) * 86400, -(lag + 1) * 86400)

  date_exploded = (date_exploded.withColumn('R_SUM_' + str(lag) + '_' + str(rolling), F.sum('SALES_QTY').over(w))
                                .withColumn('R_MEAN_' + str(lag) + '_' + str(rolling), F.avg('SALES_QTY').over(w))
                                .withColumn('R_MEDIAN_' + str(lag) + '_' + str(rolling), F.expr('percentile(SALES_QTY, 0.5)').over(w))
                                .withColumn('R_MAX_' + str(lag) + '_' + str(rolling), F.max('SALES_QTY').over(w)))

w = Window().partitionBy('DC_ID', 'DP_NAME').orderBy('DATE')

lags = [7, 8, 9, 10, 11, 12, 13, 14, 21, 28, 30, 60, 90]

for i in lags:
  date_exploded = date_exploded.withColumn('LAG_' + str(i), F.lag('SALES_QTY', i).over(w))

date_exploded = date_exploded.na.drop()

# COMMAND ----------

data = data.withColumnRenamed('DC_ID', 'DC_ID_x').withColumnRenamed('DP_NAME', 'DP_NAME_x').drop('DATE', 'SALES_QTY').dropDuplicates()

cond = [date_exploded.DC_ID == data.DC_ID_x, date_exploded.DP_NAME == data.DP_NAME_x]

date_exploded = date_exploded.join(data, cond, 'left').drop('DC_ID_x', 'DP_NAME_x').na.drop()

# COMMAND ----------

# date_exploded.write.mode('overwrite').parquet('/mnt/adls/BHX_FC/FBFC/DAILY/TEMP_FILES/231218_TMP_DE_WITH_LAG.parquet')
date_exploded = spark.read.parquet('/mnt/adls/BHX_FC/FBFC/DAILY/TEMP_FILES/231218_TMP_DE_WITH_LAG.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Store/Product Diversity

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Store/Product Diversity

# COMMAND ----------

lag = 7
rolling = 30

w = (Window().partitionBy('DC_ID', 'CATEGORY')
             .orderBy('UNIX_TIMESTAMP')
             .rangeBetween(-(lag + rolling) * 86400, -(lag + 1) * 86400))

store_diversity = (date_exploded.withColumn('DC_DIVERSITY', F.size(F.collect_set('DP_NAME').over(w)))
                                .select('DC_ID', 'CATEGORY', 'DATE', 'DC_DIVERSITY')
                                .dropDuplicates())

w = (Window().partitionBy('DP_NAME', 'DC_PROVINCE')
             .orderBy('UNIX_TIMESTAMP')
             .rangeBetween(-(lag + rolling) * 86400, -(lag + 1) * 86400))

product_diversity = (date_exploded.withColumn('PRODUCT_DIVERSITY', F.size(F.collect_set('DC_ID').over(w)))
                                  .select('DP_NAME', 'DC_PROVINCE', 'DATE', 'PRODUCT_DIVERSITY')
                                  .dropDuplicates())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Store/Product Diversity Mapping

# COMMAND ----------

store_diversity = (store_diversity.withColumnRenamed('DC_ID', 'DC_ID_x')
                                  .withColumnRenamed('CATEGORY', 'CATEGORY_x')
                                  .withColumnRenamed('DATE', 'DATE_x'))

conds = [
          date_exploded.DC_ID == store_diversity.DC_ID_x,
          date_exploded.CATEGORY == store_diversity.CATEGORY_x,
          date_exploded.DATE == store_diversity.DATE_x
        ]

date_exploded = (date_exploded.join(store_diversity, conds, 'left')
                              .drop('DC_ID_x', 'CATEGORY_x', 'DATE_x'))

# COMMAND ----------

product_diversity = (product_diversity.withColumnRenamed('DP_NAME', 'DP_NAME_x')
                                      .withColumnRenamed('DC_PROVINCE', 'DC_PROVINCE_x')
                                      .withColumnRenamed('DATE', 'DATE_x'))

conds = [
          date_exploded.DP_NAME == product_diversity.DP_NAME_x,
          date_exploded.DC_PROVINCE == product_diversity.DC_PROVINCE_x,
          date_exploded.DATE == product_diversity.DATE_x
        ]

date_exploded = (date_exploded.join(product_diversity, conds, 'left')
                              .drop('DP_NAME_x', 'DC_PROVINCE_x', 'DATE_x'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Store/Product Performance

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Store/Product Performance

# COMMAND ----------

dc_performance = (date_exploded.select('DC_ID', 'CATEGORY', 'DATE', 'R_SUM_7_30')
                               .groupBy('DC_ID', 'CATEGORY', 'DATE')
                               .agg(
                                    F.sum(F.col('R_SUM_7_30')).alias('DC_PERFORMANCE')
                                   )
                               .withColumnRenamed('DC_ID', 'DC_ID_x')
                               .withColumnRenamed('CATEGORY', 'CATEGORY_x')
                               .withColumnRenamed('DATE', 'DATE_x'))

# COMMAND ----------

product_performance = (date_exploded.select('DP_NAME', 'DC_PROVINCE', 'DATE', 'R_SUM_7_30')
                                    .groupBy('DP_NAME', 'DC_PROVINCE', 'DATE')
                                    .agg(
                                          F.sum(F.col('R_SUM_7_30')).alias('PRODUCT_PERFORMANCE')
                                        )
                                    .withColumnRenamed('DP_NAME', 'DP_NAME_x')
                                    .withColumnRenamed('DC_PROVINCE', 'DC_PROVINCE_x')
                                    .withColumnRenamed('DATE', 'DATE_x'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Store/Product Performance Mapping

# COMMAND ----------

cond = [
        date_exploded.DC_ID == dc_performance.DC_ID_x,
        date_exploded.CATEGORY == dc_performance.CATEGORY_x,
        date_exploded.DATE == dc_performance.DATE_x
       ]
date_exploded = date_exploded.join(dc_performance, cond, 'left').drop('DC_ID_x', 'CATEGORY_x', 'DATE_x')

cond = [
        date_exploded.DP_NAME == product_performance.DP_NAME_x,
        date_exploded.DC_PROVINCE == product_performance.DC_PROVINCE_x,
        date_exploded.DATE == product_performance.DATE_x
       ]
date_exploded = date_exploded.join(product_performance, cond, 'left').drop('DP_NAME_x', 'DC_PROVINCE_x', 'DATE_x')

# COMMAND ----------

# date_exploded.write.mode('overwrite').parquet('/mnt/adls/BHX_FC/FBFC/DAILY/TEMP_FILES/231218_TMP_DE_LAG_PERFORMANCE.parquet')
date_exploded = spark.read.parquet('/mnt/adls/BHX_FC/FBFC/DAILY/TEMP_FILES/231218_TMP_DE_LAG_PERFORMANCE.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Promotion

# COMMAND ----------

promotion = pd.read_csv('/dbfs/mnt/adls/BHX_FC/FBFC/DATA/PROMOTION_UPDATE_DEC_23.csv').dropna(subset = ['SKUID'])

promotion['SKUID'] = promotion['SKUID'].astype('int')

# COMMAND ----------

spark.createDataFrame(promotion).createOrReplaceTempView('promotion')

# COMMAND ----------

promotion = spark.sql("""
                        select t1.*, t2.`CD DP Name` as DP_NAME
                        from promotion as t1
                        left join product_master as t2
                        on t1.SKUID = t2.Material
                      """).toPandas()

# COMMAND ----------

def explode(row):
  start_date = row['START_DATE']
  end_date = row['END_DATE']

  res = pd.DataFrame()
  res['DATE'] = pd.date_range(start_date, end_date, freq = 'd')
  res['DP_NAME'] = [row['DP_NAME']] * len(res)
  res['DISCOUNT'] = [row['DISCOUNT']] * len(res)

  return res[['DP_NAME', 'DATE', 'DISCOUNT']]

@ray.remote
def REMOTE_explode(row):
  return explode(row)

# COMMAND ----------

tasks = [REMOTE_explode.remote(promotion.iloc[i]) for i in tqdm(range(len(promotion)))]
promotion = pd.concat(ray.get(tasks))

# COMMAND ----------

promotion = promotion.groupby(['DP_NAME', 'DATE']).agg('max')
promotion.reset_index(inplace = True)

# COMMAND ----------

promotion = spark.createDataFrame(promotion)

promotion = promotion.withColumn('DATE', F.to_date('DATE')).withColumnRenamed('DP_NAME', 'DP_NAME_x').withColumnRenamed('DATE', 'DATE_x')

date_exploded = date_exploded.join(promotion, [date_exploded.DP_NAME == promotion.DP_NAME_x, date_exploded.DATE == promotion.DATE_x], 'left').drop('DP_NAME_x', 'DATE_x').na.fill(0)

# COMMAND ----------

date_exploded = (date_exploded.withColumn('YEAR', F.year(F.col('DATE')))
                              .withColumn('MONTH', F.month(F.col('DATE')))
                              .withColumn('DAY_OF_WEEK', F.dayofweek(F.col('DATE')))
                              .withColumn('DAY_OF_MONTH', F.dayofmonth(F.col('DATE')))
                              .withColumn('DAY_OF_YEAR', F.dayofyear(F.col('DATE')))
                              .withColumn('WEEK_OF_YEAR', F.ceil(F.dayofyear(F.col('DATE')) / 7))
                              .withColumn('YEARWEEK', F.col('YEAR') * 100 + F.col('WEEK_OF_YEAR')))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Holiday

# COMMAND ----------

calendar = spark.read.format('csv').option('header', True).load('/mnt/adls/BHX_FC/FBFC/DATA/CALENDAR.csv')

calendar = (calendar.select('YEARWEEK', 'YEARQUARTER', 'YEARHALF', 'HOLIDAYNAME', 'SPECIALEVENTNAME', 'WEEKTYPE',
                            'MONTHENDFACTOR', 'QUARTERTYPE', 'DAYTYPE_CLOSING', 'DAYTYPE_OPENING', 'DAYTYPE_NORMAL')
                    .withColumnRenamed('YEARWEEK', 'YEARWEEK_x'))

# COMMAND ----------

date_exploded = date_exploded.join(calendar, date_exploded.YEARWEEK == calendar.YEARWEEK_x, 'left').drop('YEARWEEK_x').drop('STORE_NAME')

# COMMAND ----------

date_exploded = date_exploded.na.drop()

# COMMAND ----------

# date_exploded.write.mode('overwrite').parquet('/mnt/adls/BHX_FC/FBFC/DAILY/231218_DE_ALL_FEATURES.parquet')
date_exploded = spark.read.parquet('/mnt/adls/BHX_FC/FBFC/DAILY/231218_DE_ALL_FEATURES.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Model

# COMMAND ----------

# data = date_exploded.toPandas()
# data = data[['YEAR', 'MONTH', 'SALES_QTY']]

# report = pd.DataFrame()
# report['SALES_QTY'] = [i for i in range(0, 50)]

# for key, group in data.groupby(['YEAR', 'MONTH']):
#   report[str(key[1]) + '/' + str(key[0])] = group['SALES_QTY'].value_counts(normalize = True)

# report.display()

# COMMAND ----------

date_exploded = date_exploded.filter((F.col('DATE') >= '2023-03-01'))

# COMMAND ----------

data = date_exploded.toPandas()

data['YEARQUARTER'] = data['YEARQUARTER'].astype('int') % 10
data['YEARHALF'] = data['YEARHALF'].astype('int') % 10

# COMMAND ----------

def fa(y_true, y_pred):
  sum_err = sum([abs(pred - true) for true, pred in zip(y_true, y_pred)])
  sum_act = max(sum(y_true), 1)

  return 1 - sum_err / sum_act

def fb(y_true, y_pred):
  sum_err = sum([pred - true for true, pred in zip(y_true, y_pred)])
  sum_act = max(sum(y_true), 1)

  return sum_err / sum_act

# COMMAND ----------

# model = cb.CatBoostRegressor(iterations = 100, depth = 7, learning_rate = 0.03, random_seed = 1204, thread_count = -1, loss_function = 'MAE') - Model 1: 66% FA - train 30s
# model = cb.CatBoostRegressor(iterations = 100, depth = 13, learning_rate = 0.2, random_seed = 1204, thread_count = -1, loss_function = 'MAE') - Model 2: 69% FA - train 30s
model = cb.CatBoostRegressor(iterations = 100, depth = 13, learning_rate = 0.2, random_seed = 1204, thread_count = -1, loss_function = 'MAE')

cat_cols = ['DC_ID', 'DP_NAME', 'DC_PROVINCE', 'DC_DISTRICT', 'CATEGORY', 'YEAR', 'MONTH', 'DAY_OF_WEEK', 
            'DAY_OF_MONTH', 'DAY_OF_YEAR', 'WEEK_OF_YEAR', 'YEARWEEK', 'YEARQUARTER', 'YEARHALF', 'HOLIDAYNAME', 
            'SPECIALEVENTNAME', 'WEEKTYPE', 'MONTHENDFACTOR', 'QUARTERTYPE', 'DAYTYPE_CLOSING', 'DAYTYPE_OPENING', 'DAYTYPE_NORMAL']

time_threshold = pd.to_datetime('2023-09-01')
num_months = 1

data_df = data[data['DATE'] < time_threshold + DateOffset(months = num_months)].drop(columns = ['UNIX_TIMESTAMP'])

train = data_df[data_df['DATE'] < time_threshold - DateOffset(months = num_months)]
val = data_df[data_df['DATE'].between(time_threshold - DateOffset(months = num_months), time_threshold - DateOffset(days = 1))]
test = data_df[data_df['DATE'].between(time_threshold, time_threshold + DateOffset(days = 31))]

train = train.drop(columns = ['DATE'])
val = val.drop(columns = ['DATE'])
test = test.drop(columns = ['DATE'])

train = train.dropna()
val = val.dropna()
test = test.dropna()

X_train = train.drop(columns = ['SALES_QTY'])
y_train = train['SALES_QTY']

X_val = val.drop(columns = ['SALES_QTY'])
y_val = val['SALES_QTY']

X_test = test.drop(columns = ['SALES_QTY'])
y_test = test['SALES_QTY']

model = cb.CatBoostRegressor(iterations = 100, depth = 13, learning_rate = 0.2, random_seed = 1204, thread_count = -1, loss_function = 'MAE')

# COMMAND ----------

model.fit(X_train, y_train, cat_features = cat_cols, eval_set = (X_val, y_val), plot = True)

# COMMAND ----------

pred = model.predict(X_test)
pred_actual = pred.copy()
pred = [round(i) for i in pred]

true = list(y_test)

# COMMAND ----------

pred_test = test.copy()
pred_test['PREDICTION'] = pred
pred_test['PREDICTION_ACT'] = pred_actual

pred_test = pred_test[['DC_ID', 'DP_NAME', 'CATEGORY', 'DAY_OF_MONTH', 'SALES_QTY', 'PREDICTION']]

pred_test

# COMMAND ----------

(spark.createDataFrame(pred_test)
      .select('DC_ID', 'DP_NAME', 'DAY_OF_MONTH', 'SALES_QTY', 'PREDICTION')
      .withColumn('FE', F.abs(F.col('PREDICTION') - F.col('SALES_QTY')))
      .withColumn('FA', F.when(F.col('SALES_QTY') == 0, 1 - (F.col('FE') / (F.col('SALES_QTY') + 1))).otherwise(1 - (F.col('FE') / F.col('SALES_QTY'))))
      .orderBy('DC_ID', 'DP_NAME', 'DAY_OF_MONTH')
      .display())

# COMMAND ----------

(spark.createDataFrame(pred_test)
      .select('DC_ID', 'DP_NAME', 'DAY_OF_MONTH', 'SALES_QTY', 'PREDICTION')
      .withColumn('WEEK', F.ceil(F.col('DAY_OF_MONTH') / 7))
      .groupBy('DC_ID', 'DP_NAME', 'WEEK')
      .agg(F.sum('SALES_QTY').alias('SALES_QTY'), F.sum('PREDICTION').alias('PREDICTION'))
      .withColumn('FE', F.abs(F.col('PREDICTION') - F.col('SALES_QTY')))
      .withColumn('FA', F.when(F.col('SALES_QTY') == 0, 1 - (F.col('FE') / (F.col('SALES_QTY') + 1))).otherwise(1 - (F.col('FE') / F.col('SALES_QTY'))))
      .orderBy('DC_ID', 'DP_NAME', 'WEEK')
      .display())

# COMMAND ----------

pred_test = pred_test.groupby(['DP_NAME', 'DAY_OF_MONTH']).agg({'SALES_QTY': 'sum', 'PREDICTION': 'sum'})
pred_test.reset_index(inplace = True)
pred_test

# COMMAND ----------

(spark.createDataFrame(pred_test)
      .withColumn('FE', F.abs(F.col('PREDICTION') - F.col('SALES_QTY')))
      .withColumn('FA', F.when(F.col('SALES_QTY') == 0, 1 - (F.col('FE') / (F.col('SALES_QTY') + 1))).otherwise(1 - (F.col('FE') / F.col('SALES_QTY'))))
      .orderBy('DP_NAME', 'DAY_OF_MONTH')
      .display())

# COMMAND ----------

(spark.createDataFrame(pred_test)
      .withColumn('WEEK', F.ceil(F.col('DAY_OF_MONTH') / 7))
      .groupBy('DP_NAME', 'WEEK')
      .agg(F.sum('SALES_QTY').alias('SALES_QTY'), F.sum('PREDICTION').alias('PREDICTION'))
      .withColumn('FE', F.abs(F.col('PREDICTION') - F.col('SALES_QTY')))
      .withColumn('FA', F.when(F.col('SALES_QTY') == 0, 1 - (F.col('FE') / (F.col('SALES_QTY') + 1))).otherwise(1 - (F.col('FE') / F.col('SALES_QTY'))))
      .orderBy('DP_NAME', 'WEEK')
      .display())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Evaluation

# COMMAND ----------

tmp_true = []
tmp_pred = []

for a, b in zip(true, pred):
  if a != 0:
    tmp_true.append(a)
    tmp_pred.append(b)

print('CatBoost Score - with Promotion:')
print(f"FA Score: {fa(tmp_true, tmp_pred):.2f}")
print(f"FB Score: {fb(tmp_true, tmp_pred):.2f}")

# COMMAND ----------

fi = pd.DataFrame()
fi['FEATURE'] = X_train.columns
fi['IMPORTANCE'] = model.feature_importances_

fi.sort_values('IMPORTANCE', ascending = False).display()

# COMMAND ----------

pred_test = test.copy()
pred_test['PREDICTION'] = pred
pred_test['PREDICTION_ACT'] = pred_actual

pred_test = pred_test[['DC_ID', 'DP_NAME', 'CATEGORY', 'DAY_OF_MONTH', 'SALES_QTY', 'PREDICTION', 'PREDICTION_ACT']]

pred_test

# COMMAND ----------

pred_test = pred_test.drop(columns = ['PREDICTION_ACT'])

# COMMAND ----------

pred_test = pred_test.groupby(['DP_NAME', 'DAY_OF_MONTH']).agg({'SALES_QTY': 'sum', 'PREDICTION': 'sum'})
pred_test.reset_index(inplace = True)
pred_test

# COMMAND ----------

tmp = (spark.createDataFrame(pred_test)
      .withColumn('FE', F.abs(F.col('PREDICTION') - F.col('SALES_QTY')))
      .withColumn('FA', F.when(F.col('SALES_QTY') == 0, 1 - (F.col('FE') / (F.col('SALES_QTY') + 1))).otherwise(1 - (F.col('FE') / F.col('SALES_QTY'))))
      .orderBy('DP_NAME', 'DAY_OF_MONTH'))

# COMMAND ----------

total = 2907635

rank = (tmp.groupBy('DP_NAME')
          .agg(F.sum('SALES_QTY').alias('SALES_QTY'))
          .withColumn('RANK', F.when(F.col('SALES_QTY') > 50000, 50000)
                               .when(F.col('SALES_QTY') > 20000, 20000)
                               .when(F.col('SALES_QTY') > 10000, 10000)
                               .when(F.col('SALES_QTY') > 5000, 5000)
                               .when(F.col('SALES_QTY') > 2000, 2000)
                               .when(F.col('SALES_QTY') > 1000, 1000)
                               .when(F.col('SALES_QTY') > 500, 500)
                               .when(F.col('SALES_QTY') > 200, 200)
                               .otherwise(0))
          .select('DP_NAME', 'RANK'))

tmp.join(rank, tmp.DP_NAME == rank.DP_NAME, 'left').drop(rank.DP_NAME).groupBy('RANK').agg(F.mean('FA').alias('MEAN_FA')).orderBy(F.col('RANK').desc()).display()

# COMMAND ----------

tmp = (spark.createDataFrame(pred_test)
            .withColumn('WEEK', F.ceil(F.col('DAY_OF_MONTH') / 7))
            .groupBy('DP_NAME', 'WEEK')
            .agg(F.sum('SALES_QTY').alias('SALES_QTY'), F.sum('PREDICTION').alias('PREDICTION'))
            .withColumn('FE', F.abs(F.col('PREDICTION') - F.col('SALES_QTY')))
            .withColumn('FA', F.when(F.col('SALES_QTY') == 0, 1 - (F.col('FE') / (F.col('SALES_QTY') + 1))).otherwise(1 - (F.col('FE') / F.col('SALES_QTY'))))
            .orderBy('DP_NAME', 'WEEK'))

# COMMAND ----------

tmp.join(rank, tmp.DP_NAME == rank.DP_NAME, 'left').drop(rank.DP_NAME).groupBy('RANK').agg(F.mean('FA').alias('MEAN_FA')).orderBy(F.col('RANK').desc()).display()

# COMMAND ----------

pred_test['SEGMENT'] = np.where(pred_test['SALES_QTY'] < 100, pred_test['SALES_QTY'], '>= 100')

# COMMAND ----------

tmp = pred_test.copy()

tmp['NEG_BIAS'] = np.where(tmp['PREDICTION'] - tmp['SALES_QTY'] < 0, tmp['PREDICTION'] - tmp['SALES_QTY'], 0)
tmp['POS_BIAS'] = np.where(tmp['PREDICTION'] - tmp['SALES_QTY'] > 0, tmp['PREDICTION'] - tmp['SALES_QTY'], 0)

data_report = tmp.groupby('SEGMENT').agg(
                                          TOTAL_RECORDS = ('SALES_QTY', 'count'),
                                          TOTAL_ACTUAL_VOL = ('SALES_QTY', 'sum'),
                                          AVG_ACTUAL_VOL = ('SALES_QTY', 'mean'),
                                          AVG_PREDICT_VOL = ('PREDICTION', 'mean'),
                                          TOTAL_NEGATIVE_BIAS = ('NEG_BIAS', 'sum'),
                                          TOTAL_POSITIVE_BIAS = ('POS_BIAS', 'sum')
                                         )

data_report['TOTAL_CONTRIBUTION'] = data_report['TOTAL_ACTUAL_VOL'] / pred_test['SALES_QTY'].sum()

data_report.reset_index(inplace = True)

data_report = data_report[['SEGMENT', 'TOTAL_RECORDS', 'TOTAL_ACTUAL_VOL', 'TOTAL_CONTRIBUTION',
                           'AVG_ACTUAL_VOL', 'AVG_PREDICT_VOL', 'TOTAL_NEGATIVE_BIAS', 'TOTAL_POSITIVE_BIAS']]

data_report = data_report.loc[pd.to_numeric(data_report['SEGMENT'], errors = 'coerce').sort_values().index].set_index('SEGMENT')

data_report.display()

# COMMAND ----------

tmp = pred_test.copy()

tmp['FA_SCORE'] = abs(tmp['PREDICTION'] - tmp['SALES_QTY'])
tmp['FB_SCORE'] = tmp['PREDICTION'] - tmp['SALES_QTY']

tmp = tmp.groupby('SEGMENT').agg(SUM_ACTUAL = ('SALES_QTY', 'sum'),
                                 COUNT_ACTUAL = ('SALES_QTY', 'count'),
                                 FA_SCORE = ('FA_SCORE', 'sum'),
                                 FB_SCORE = ('FB_SCORE', 'sum'))

tmp['FA_SCORE'] = 1 - tmp['FA_SCORE'] / tmp['SUM_ACTUAL']
tmp['FB_SCORE'] = tmp['FB_SCORE'] / tmp['COUNT_ACTUAL']

tmp.reset_index(inplace = True)

tmp = tmp.loc[pd.to_numeric(tmp['SEGMENT'], errors = 'coerce').sort_values().index].set_index('SEGMENT').rename(columns = {'SUM_ACTUAL': 'TOTAL_ACTUAL_VOL'}).drop(columns = ['COUNT_ACTUAL'])

tmp

# COMMAND ----------

pred_test = test.copy()
pred_test['PREDICTION'] = pred
pred_test = pred_test[['DC_ID', 'DP_NAME', 'CATEGORY', 'DAY_OF_MONTH', 'SALES_QTY', 'PREDICTION']].sort_values(['DC_ID', 'DP_NAME', 'DAY_OF_MONTH'])
pred_test['SALES_SEGMENT'] = np.where(pred_test['SALES_QTY'] < 100, pred_test['SALES_QTY'], '>= 100')

col = 'DAY_OF_MONTH'

conds = [
          pred_test[col] == 1,
          pred_test[col] == 2,
          pred_test[col] == 3,
          pred_test[col] == 4,
          pred_test[col] == 5,
          pred_test[col] == 6,
          pred_test[col] == 7,
        ]

choices = [
            'D1', 'D2', 'D3', 'D4', 'D5', 'D6', 'D7',
          ]

pred_test['DATE_SEGMENT'] = np.select(conds, choices, default = None)
pred_test = pred_test[~pred_test['DATE_SEGMENT'].isna()]

pred_test['FA_ERR'] = pred_test.apply(lambda x: abs(x['SALES_QTY'] - x['PREDICTION']), axis = 1)
# pred_test['FA_ERR'] = np.where(pred_test['SALES_QTY'] == 0, 0, pred_test['FA_ERR'])

# COMMAND ----------

col = 'CATEGORY'

report = pred_test.groupby([col, 'DATE_SEGMENT']).agg({'FA_ERR': 'sum', 'SALES_QTY': 'sum'})
report.reset_index(inplace = True)
report['FA'] = 1 - (report['FA_ERR'] / report['SALES_QTY'])
report = report.drop(columns = ['FA_ERR', 'SALES_QTY'])

report.pivot(index = col, columns = 'DATE_SEGMENT', values = 'FA')

# COMMAND ----------

contri = pred_test.groupby(col).agg(VOLUME_COUNT = ('SALES_QTY', 'sum'), RECORD_COUNT = ('SALES_QTY', 'count'))

contri['VOLUME_CONTRIBUTION'] = contri['VOLUME_COUNT'] / contri['VOLUME_COUNT'].sum()
contri['RECORD_CONTRIBUTION'] = contri['RECORD_COUNT'] / contri['RECORD_COUNT'].sum()

contri

# COMMAND ----------

col = 'DC_ID'

report = pred_test.groupby([col, 'DATE_SEGMENT']).agg({'FA_ERR': 'sum', 'SALES_QTY': 'sum'})
report.reset_index(inplace = True)
report['FA'] = 1 - (report['FA_ERR'] / report['SALES_QTY'])
report = report.drop(columns = ['FA_ERR', 'SALES_QTY'])

report.pivot(index = col, columns = 'DATE_SEGMENT', values = 'FA')

# COMMAND ----------

contri = pred_test.groupby(col).agg(VOLUME_COUNT = ('SALES_QTY', 'sum'), RECORD_COUNT = ('SALES_QTY', 'count'))

contri['VOLUME_CONTRIBUTION'] = contri['VOLUME_COUNT'] / contri['VOLUME_COUNT'].sum()
contri['RECORD_CONTRIBUTION'] = contri['RECORD_COUNT'] / contri['RECORD_COUNT'].sum()

contri

# COMMAND ----------

col = 'SALES_SEGMENT'

report = pred_test.groupby([col, 'DATE_SEGMENT']).agg({'FA_ERR': 'sum', 'SALES_QTY': 'sum'})
report.reset_index(inplace = True)
report['FA'] = 1 - (report['FA_ERR'] / report['SALES_QTY'])
report = report.drop(columns = ['FA_ERR', 'SALES_QTY'])

a = report.pivot(index = col, columns = 'DATE_SEGMENT', values = 'FA')
a.reset_index(inplace = True)

a = a.loc[pd.to_numeric(a['SALES_SEGMENT'], errors = 'coerce').sort_values().index].set_index('SALES_SEGMENT')

a.display()

# COMMAND ----------

contri = pred_test.groupby(col).agg(VOLUME_COUNT = ('SALES_QTY', 'sum'), RECORD_COUNT = ('SALES_QTY', 'count'))

contri['VOLUME_CONTRIBUTION'] = contri['VOLUME_COUNT'] / contri['VOLUME_COUNT'].sum()
contri['RECORD_CONTRIBUTION'] = contri['RECORD_COUNT'] / contri['RECORD_COUNT'].sum()

contri.reset_index(inplace = True)

contri = contri.loc[pd.to_numeric(contri['SALES_SEGMENT'], errors = 'coerce').sort_values().index].set_index('SALES_SEGMENT')

contri.display()

# COMMAND ----------

pred_test = test.copy()
pred_test['PREDICTION'] = pred
pred_test = pred_test[['DC_ID', 'DP_NAME', 'CATEGORY', 'DAY_OF_MONTH', 'SALES_QTY', 'PREDICTION']].sort_values(['DC_ID', 'DP_NAME', 'DAY_OF_MONTH'])
pred_test['SALES_SEGMENT'] = np.where(pred_test['SALES_QTY'] < 100, pred_test['SALES_QTY'], '>= 100')

col = 'DAY_OF_MONTH'

conds = [
          pred_test[col] == 1,
          pred_test[col] == 2,
          pred_test[col] == 3,
          pred_test[col] == 4,
          pred_test[col] == 5,
          pred_test[col] == 6,
          pred_test[col] == 7,
        ]

choices = [
            'D1', 'D2', 'D3', 'D4', 'D5', 'D6', 'D7',
          ]

pred_test['DATE_SEGMENT'] = np.select(conds, choices, default = None)
pred_test = pred_test[~pred_test['DATE_SEGMENT'].isna()]

pred_test['FB_ERR'] = pred_test.apply(lambda x: x['PREDICTION'] - x['SALES_QTY'], axis = 1)

# COMMAND ----------

col = 'CATEGORY'

report = pred_test.groupby([col, 'DATE_SEGMENT']).agg({'FB_ERR': 'sum', 'SALES_QTY': 'sum'})
report.reset_index(inplace = True)
report['FB'] = report['FB_ERR'] / report['SALES_QTY']
report = report.drop(columns = ['FB_ERR', 'SALES_QTY'])

report.pivot(index = col, columns = 'DATE_SEGMENT', values = 'FB')

# COMMAND ----------

col = 'DC_ID'

report = pred_test.groupby([col, 'DATE_SEGMENT']).agg({'FB_ERR': 'sum', 'SALES_QTY': 'sum'})
report.reset_index(inplace = True)
report['FB'] = report['FB_ERR'] / report['SALES_QTY']
report = report.drop(columns = ['FB_ERR', 'SALES_QTY'])

report.pivot(index = col, columns = 'DATE_SEGMENT', values = 'FB')

# COMMAND ----------

col = 'SALES_SEGMENT'

report = pred_test.groupby([col, 'DATE_SEGMENT']).agg({'FB_ERR': 'sum', 'SALES_QTY': 'sum'})
report.reset_index(inplace = True)
report['FB'] = report['FB_ERR'] / report['SALES_QTY']
report = report.drop(columns = ['FB_ERR', 'SALES_QTY'])

report.pivot(index = col, columns = 'DATE_SEGMENT', values = 'FB')

# COMMAND ----------

# spark.createDataFrame(pred_test.drop(columns = ['FB_ERR'])).write.parquet('/mnt/adls/BHX_FC/FBFC/DAILY/231025_PREDICTION.parquet')

# COMMAND ----------

data = spark.read.parquet('/mnt/adls/BHX_FC/FBFC/DAILY/231025_PREDICTION.parquet')

# COMMAND ----------

import matplotlib.pyplot as plt
%matplotlib inline

# plt.rcParams['figure.figsize'] = [40, 20]
plt.rcParams['font.size'] = 30

# COMMAND ----------

data = data.toPandas()

# COMMAND ----------

plt.figure(figsize = (40, 20))
# plt.set_axisbelow(True)
# plt.grid(visible = True)

fig, ax = plt.subplots(figsize = (40, 20))
ax.scatter(data['SALES_QTY'].values.tolist(), data['PREDICTION'].values.tolist())
ax.plot(data['SALES_QTY'].values.tolist(), data['SALES_QTY'].values.tolist(), color = 'red')
ax.set_xlabel('ACTUAL SALES')
ax.set_ylabel('PREDICTED SALES')
ax.set_title('SCATTER PLOT OF ACTUAL SALES BY PREDICTED SALES')
ax.grid(visible = True)
ax.set_axisbelow(True)

plt.show()

# COMMAND ----------

tmp = {}

l = [[str(i) for i in range(j * 20, j * 20 + 20)] for j in range(5)]

for key, group in data.groupby('SALES_SEGMENT'):
  tmp[key] = group['PREDICTION'].values.tolist()

fig, ax = plt.subplots(5, figsize = (40, 100))

for gr in range(len(l)):
  k = l[gr]
  v = [tmp[i] for i in l[gr]]

  ax[gr].boxplot(v, labels = k)
  ax[gr].set_xlabel('ACTUAL SALES')
  ax[gr].set_ylabel('PREDICTION')
  ax[gr].set_title('Boxplot of Prediction by Actual Sales - ' + str(gr * 20) + ' to ' + str(gr * 20 + 19))

plt.show()

# COMMAND ----------

data[(data['SALES_QTY'] == 7) & (data['PREDICTION'] > 100)]

# COMMAND ----------

dc_id = 5060
dp_name = 'Knorr Fish Sauce Mainstream 750Ml'

to_draw = date_exploded.filter((F.col('DC_ID') == dc_id) & (F.col('DP_NAME') == dp_name)).orderBy('DATE').toPandas()

plt.figure(figsize = (40, 20))
plt.plot(to_draw['DATE'].values.tolist(), to_draw['SALES_QTY'].values.tolist())
plt.twinx().plot(to_draw['DATE'].values.tolist(), to_draw['DISCOUNT'].values.tolist(), color = 'red')

# COMMAND ----------


# Databricks notebook source
# MAGIC %pip install openpyxl ray

# COMMAND ----------

import pandas as pd
import numpy as np
import ray
import matplotlib.pyplot as plt

import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

ray.init()

# COMMAND ----------

# MAGIC %matplotlib inline

# COMMAND ----------

df_sales = spark.read.parquet('/mnt/adls/MT_FC/DATA/TMP/231226_TMP_PRI_SALES_PERFORMANCE.parquet').orderBy('BANNER', 'DP_NAME', 'YEARWEEK').toPandas()

# COMMAND ----------

PRODUCT_HIERARCHY_PATH = '/mnt/adls/BHX_FC/FBFC/DATA/PRODUCT_MASTER.csv'

(spark.read.format("csv")
            .options(delimiter = ",", header = "True")
            .load(PRODUCT_HIERARCHY_PATH)
            .createOrReplaceTempView('product_master'))

promotion = pd.read_csv('/dbfs/mnt/adls/BHX_FC/FBFC/DATA/PROMOTION_UPDATE_DEC_23.csv').dropna(subset = ['SKUID'])

promotion['SKUID'] = promotion['SKUID'].astype('int')

spark.createDataFrame(promotion).createOrReplaceTempView('promotion')

promotion = spark.sql("""
                        select t1.*, t2.`CD DP Name` as DP_NAME
                        from promotion as t1
                        left join product_master as t2
                        on t1.SKUID = t2.Material
                      """).toPandas()

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

tasks = [REMOTE_explode.remote(promotion.iloc[i]) for i in range(len(promotion))]
promotion = pd.concat(ray.get(tasks))

promotion = promotion.groupby(['DP_NAME', 'DATE']).agg('max')
promotion.reset_index(inplace = True)

promotion = spark.createDataFrame(promotion)

promotion = (promotion.withColumn('DATE', F.to_date('DATE'))
                      .withColumn('DP_NAME', F.upper('DP_NAME'))
                      .withColumn('YEARWEEK', F.year('DATE') * 100 + F.ceil(F.dayofyear('DATE') / 7))
                      .groupBy('DP_NAME', 'YEARWEEK')
                      .agg(F.mean('DISCOUNT').alias('DISCOUNT'))
                      .orderBy('DP_NAME', 'YEARWEEK')
                      .toPandas())

# COMMAND ----------

df_sales['TO_GROUP'] = df_sales['BANNER'].isin(['BACH HOA XANH', 'BIG_C', 'SAIGON COOP', 'ECUSTOMER', 'VINMART'])
df_sales['BANNER'] = np.where(df_sales['TO_GROUP'], df_sales['BANNER'], 'MT OTHER')

df_sales = df_sales.drop(columns = ['TO_GROUP'])

# COMMAND ----------

df_result = spark.read.parquet('/mnt/adls/MT_FC/DATA/TMP/231227_TMP_DIRECT_80_RESULT.parquet').toPandas()

df_result['TO_GROUP'] = df_result['BANNER'].isin(['BACH HOA XANH', 'BIG_C', 'SAIGON COOP', 'ECUSTOMER', 'VINMART'])
df_result['BANNER'] = np.where(df_result['TO_GROUP'], df_result['BANNER'], 'MT OTHER')

df_result = df_result.drop(columns = ['TO_GROUP'])

# COMMAND ----------

def fa(y_true, y_pred):
  sum_err = sum([abs(pred - true) for true, pred in zip(y_true, y_pred)]) + 1
  sum_act = sum(y_true) + 1

  return 1 - sum_err / sum_act

# COMMAND ----------

df_promo = spark.read.parquet('/mnt/adls/MT_FC/DATA/TMP/231223_PROMO_COUNT.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Random DP of each Customer

# COMMAND ----------

def plot(banner, dp, promotion_col):
  to_draw = df_sales[(df_sales['BANNER'] == banner) & (df_sales['DP_NAME'] == dp) & (df_sales['YEARWEEK'] > 202100)].reset_index(drop = True)
  to_eval = df_result[(df_result['BANNER'] == banner) & (df_result['DP_NAME'] == dp)]

  if len(to_eval) > 0:
    title = f"SALES QUANTITY BY YEARWEEK - {banner} - {dp} - ACCURACY: {fa(to_eval['SALES_QTY'].values.tolist(), to_eval['PREDICTION'].values.tolist()):.2f}"
  else:
    title = f"SALES QUANTITY BY YEARWEEK - {banner} - {dp} - ACCURACY: CANNOT PREDICT"

  plt.figure(figsize = (40, 20))
  plt.xticks(rotation = 90)
  plt.rcParams['font.size'] = 12

  plt.bar(to_draw['YEARWEEK'].astype('str').values.tolist(), to_draw['SALES_QTY'].values.tolist(), color = np.where(to_draw['YEARWEEK'].between(202212, 202340), 'tomato', 'dodgerblue'))

  plt.xlabel('YEARWEEK', fontsize = 25)
  plt.ylabel('SALES QUANTITY', fontsize = 25)
  plt.title(title, fontsize = 30)

  plt.twinx().plot(to_draw['YEARWEEK'].astype('str').values.tolist(), to_draw[promotion_col].values.tolist())

  plt.show()

# COMMAND ----------

df_sales.columns

# COMMAND ----------

df_sales['BANNER'].unique()

# COMMAND ----------

df_sales[df_sales['BANNER'] == 'BIG_C']['DP_NAME'].unique().tolist()

# COMMAND ----------

banner = 'SAIGON COOP'
dp = 'DOVE SHAMPOO ANTI HAIRFALL 880G'

plot(banner, dp, 'COUNT_OK')

# COMMAND ----------

df_promo.filter((F.col('BANNER') == banner) & (F.col('DP_NAME') == dp) & (F.col('YEARWEEK').between(202243, 202245))).display()

# COMMAND ----------


# Databricks notebook source
# MAGIC %pip install ray openpyxl ray catboost

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

df_sales_original = spark.read.parquet('/mnt/adls/MT_FC/DATA/TMP/240111_HANA_PRIMARY.parquet')

# COMMAND ----------

df_sales = df_sales_original

# COMMAND ----------

to_drop = (df_sales.groupBy('BANNER', 'DP_NAME')
                   .agg(F.min('DATE').alias('MIN_DATE'), F.max('DATE').alias('MAX_DATE'))
                   .withColumn('DURATION', F.round(F.datediff('MAX_DATE', 'MIN_DATE') / 7))
                   .withColumn('IS_VALID', F.col('DURATION') > 20)
                   .select('BANNER', 'DP_NAME', 'IS_VALID'))

# COMMAND ----------

df_sales = (df_sales.join(to_drop, [df_sales.BANNER == to_drop.BANNER, df_sales.DP_NAME == to_drop.DP_NAME], 'left')
                    .filter(F.col('IS_VALID') == True)
                    .drop(to_drop.BANNER, to_drop.DP_NAME, 'IS_VALID')
                    .select('BANNER', 'DP_NAME', 'DATE', 'SALES_QTY'))

# COMMAND ----------

# df_sales.write.mode('overwrite').parquet('/mnt/adls/MT_FC/DATA/TMP/240111_HANA_PRIMARY_BY_DAY_LAG1.parquet')
df_sales = spark.read.parquet('/mnt/adls/MT_FC/DATA/TMP/240111_HANA_PRIMARY_BY_DAY_LAG1.parquet')

# COMMAND ----------

# DBTITLE 1,Explode Date
max_date = df_sales.agg(F.max('DATE').alias('MAX_DATE')).toPandas()['MAX_DATE'].values[0]

date_exploded = (df_sales.groupBy('BANNER', 'DP_NAME')
                         .agg(
                              F.min('DATE').alias('MIN_DATE')
                             )
                         .withColumn('MAX_DATE', F.lit(max_date))
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
# df_sales.write.mode('overwrite').parquet('/mnt/adls/MT_FC/DATA/TMP/240111_HANA_PRIMARY_BY_WEEK_LAG1.parquet')
df_sales = spark.read.parquet('/mnt/adls/MT_FC/DATA/TMP/240111_HANA_PRIMARY_BY_WEEK_LAG1.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Datetime Mapping

# COMMAND ----------

# """
# The following code is to generate the datetime mapping for this project and it has the data at week level from 2016 to 2116 with columns:
#  - YEARWEEK
#  - WEEK_OF_YEAR
#  - MONTH
#  - QUARTER
#  - YEAR
#  - WEEK_OF_MONTH
# """

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

# df_sales.write.mode('overwrite').parquet('/mnt/adls/MT_FC/DATA/TMP/241011_TMP_PRI_SALES_LAG1.parquet')
df_sales = spark.read.parquet('/mnt/adls/MT_FC/DATA/TMP/241011_TMP_PRI_SALES_LAG1.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Lag-Rolling Generator

# COMMAND ----------

test = (df_sales.filter((F.col('BANNER').isin(['AEON', 'BIG_C'])) & (F.col('DP_NAME').isin(['AXE DEO BODYSPRAY DARK TMPTTN 12X150ML', 'CF. INTENSE CARE ANTI MALODOR POU 3.8L'])))
                .select('BANNER', 'DP_NAME', 'YEARWEEK', 'UNIX_TIMESTAMP', 'SALES_QTY')
                .orderBy('BANNER', 'DP_NAME', 'YEARWEEK'))

# COMMAND ----------

week = lambda x: x * 604800
test = df_sales.select('BANNER', 'DP_NAME', 'YEARWEEK', 'UNIX_TIMESTAMP', 'SALES_QTY')

for lag in range(1, 16):
  window = Window().partitionBy('BANNER', 'DP_NAME').orderBy('UNIX_TIMESTAMP')

  test = test.withColumn('LAG_' + str(lag), F.lag('SALES_QTY', lag).over(window))

test.filter((F.col('BANNER') == 'AEON') & (F.col('DP_NAME') == 'CF. INTENSE CARE ANTI MALODOR POU 3.8L')).orderBy('BANNER', 'DP_NAME', 'YEARWEEK').display()

# COMMAND ----------

(df_sales_original.filter((F.col('BANNER') == 'AEON') & (F.col('DP_NAME') == 'CF. INTENSE CARE ANTI MALODOR POU 3.8L'))
                  .select('BANNER', 'DP_NAME', 'DATE', 'SALES_QTY')
                  .withColumn('YEARWEEK', F.year('DATE') * 100 + F.ceil(F.dayofyear('DATE') / 7))
                  .groupBy('BANNER', 'DP_NAME', 'YEARWEEK')
                  .agg(F.sum('SALES_QTY').alias('SALES_QTY'))
                  .orderBy('BANNER', 'DP_NAME', 'YEARWEEK')
                  .display())

# COMMAND ----------

(df_sales_original.filter((F.col('BANNER') == 'AEON') & (F.col('DP_NAME') == 'CF. INTENSE CARE ANTI MALODOR POU 3.8L'))
                  .select('BANNER', 'DP_NAME', 'DATE', 'SALES_QTY')
                  .orderBy('BANNER', 'DP_NAME', 'DATE')
                  .display())

# COMMAND ----------

test.display()

# COMMAND ----------


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

df_sales_original = spark.read.parquet('/mnt/adls/MT_FC/DATA/TMP/231222_HANA_PRIMARY.parquet')

# COMMAND ----------

df_sales = df_sales_original

# COMMAND ----------

to_drop = (df_sales.groupBy('BANNER', 'DP_NAME')
                   .agg(F.min('DATE').alias('MIN_DATE'), F.max('DATE').alias('MAX_DATE'))
                   .withColumn('DURATION', F.round(F.datediff('MAX_DATE', 'MIN_DATE') / 7))
                   .withColumn('IS_VALID', F.col('DURATION') > 56)
                   .select('BANNER', 'DP_NAME', 'IS_VALID'))

# COMMAND ----------

df_sales = (df_sales.join(to_drop, [df_sales.BANNER == to_drop.BANNER, df_sales.DP_NAME == to_drop.DP_NAME], 'left')
                    .filter(F.col('IS_VALID') == True)
                    .drop(to_drop.BANNER, to_drop.DP_NAME, 'IS_VALID')
                    .select('BANNER', 'DP_NAME', 'DATE', 'SALES_QTY'))

# COMMAND ----------

# df_sales.write.mode('overwrite').parquet('/mnt/adls/MT_FC/DATA/TMP/231222_HANA_PRIMARY_BY_DAY.parquet')
df_sales = spark.read.parquet('/mnt/adls/MT_FC/DATA/TMP/231222_HANA_PRIMARY_BY_DAY.parquet')

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
df_sales = (df_sales.withColumn('YEARWEEK', F.year('DATE') * 100 + F.ceil(F.dayofyear('DATE') / 7))
                    .groupBy('BANNER', 'DP_NAME', 'YEARWEEK')
                    .agg(F.sum('SALES_QTY').alias('SALES_QTY')))

# COMMAND ----------

# DBTITLE 1,Save data to storage
# df_sales.write.mode('overwrite').parquet('/mnt/adls/MT_FC/DATA/TMP/231222_HANA_PRIMARY_BY_WEEK.parquet')
df_sales = spark.read.parquet('/mnt/adls/MT_FC/DATA/TMP/231222_HANA_PRIMARY_BY_WEEK.parquet')

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

df_sales.display()

# COMMAND ----------


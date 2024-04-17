# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

import pandas as pd
import numpy as np

import pyspark.sql.functions as F
from pyspark.sql.window import Window

import matplotlib.pyplot as plt

# COMMAND ----------

df_promo_original = spark.read.format('delta').load('/mnt/adls/MDL_Prod/Bronze/PPM/PIDReport/VN/Processed/')
df_promo = df_promo_original

# COMMAND ----------

df_promo.display()

# COMMAND ----------

PRODUCT_HIERARCHY_PATH = '/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/LANDING-MASTER/Master Data Total Cat.xlsx'

code_mapping_original = spark.createDataFrame(pd.read_excel(PRODUCT_HIERARCHY_PATH, sheet_name = 'Code Master'))
dp_mapping_original = spark.createDataFrame(pd.read_excel(PRODUCT_HIERARCHY_PATH, sheet_name = 'DP Name Master'))

code_mapping = code_mapping_original
dp_mapping = dp_mapping_original

# COMMAND ----------

customer_mapping = (spark.read.option('header', True)
                         .csv('/mnt/adls/MT_FC/DATA/CUSTOMER_MAPPING.csv')
                         .filter(F.col('Banner Standard') != 'GENERAL TRADE')
                         .withColumnRenamed('Banner Standard', 'BANNER')
                         .withColumnRenamed('OMR Customer', 'CUSTOMER'))

dp_mapping = (dp_mapping.select('DP Name', 'DP Name Current')
                        .withColumnRenamed('DP Name', 'DP_NAME')
                        .withColumnRenamed('DP Name Current', 'DP_NAME_CURRENT')
                        .dropDuplicates())

code_mapping = (code_mapping.select('SAP Code', 'DP name')
                            .withColumnRenamed('SAP Code', 'SKUID')
                            .withColumnRenamed('DP name', 'DP_NAME')
                            .dropDuplicates())

code_mapping = (code_mapping.join(dp_mapping, code_mapping.DP_NAME == dp_mapping.DP_NAME, 'left')
                            .withColumn('DP_NAME_LATEST', F.when(~F.col('DP_NAME_CURRENT').isNull(), F.col('DP_NAME_CURRENT')).otherwise(code_mapping.DP_NAME))
                            .drop('DP_NAME_CURRENT', code_mapping.DP_NAME, dp_mapping.DP_NAME)
                            .withColumnRenamed('DP_NAME_LATEST', 'DP_NAME'))

# COMMAND ----------

df_promo = (df_promo_original.join(customer_mapping, df_promo_original.PlanningCustomerDescription == customer_mapping.CUSTOMER, 'left').drop('CUSTOMER').na.drop(subset = 'BANNER')
                            .join(code_mapping, df_promo_original.PlanningProductCode == code_mapping.SKUID, 'left').drop('SKUID').na.drop(subset = 'DP_NAME')
                            .select('BANNER', 'DP_NAME', 'SellOutStartDate', 'SellOutendDate', 'InvestmentType', 'PromotionId')
                            .withColumn('BANNER', F.upper('BANNER'))
                            .withColumn('DP_NAME', F.upper('DP_NAME'))
                            .withColumn('START_DATE', F.to_date('SellOutStartDate', 'M/d/yyyy'))
                            .withColumn('END_DATE', F.to_date('SellOutendDate', 'M/d/yyyy'))
                            .withColumnRenamed('InvestmentType', 'PROMOTION_TYPE')
                            .drop('SellOutStartDate', 'SellOutendDate')
                            .na.drop()
                            .dropDuplicates()
                            .orderBy('BANNER', 'DP_NAME', 'START_DATE', 'END_DATE'))

# COMMAND ----------

df_promo = df_promo.withColumn('DATE', F.explode(F.expr('sequence(START_DATE, END_DATE, interval 1 day)')))

# COMMAND ----------

# df_promo.write.mode('overwrite').parquet('/mnt/adls/MT_FC/DATA/TMP/231223_TMP_PROMOTION_PPM.parquet')
df_promo = spark.read.parquet('/mnt/adls/MT_FC/DATA/TMP/231223_TMP_PROMOTION_PPM.parquet')

# COMMAND ----------

groups = ['OK', 'II', 'ER', 'AA', 'JP', 'IZ', 'JR', 'AK', 'GI', 'MK', 'GW', 'GV', 'QK', 'EQ']

for gr in groups:
  df_promo = df_promo.withColumn('PROMO_' + gr, F.when(F.col('PROMOTION_TYPE') == gr, 1).otherwise(0))

df_promo = df_promo.withColumn('PROMO_OTHER', F.when(F.expr('+'.join(['PROMO_' + gr for gr in groups])) == 0, 1).otherwise(0))

# COMMAND ----------

df_promo = (df_promo.withColumn('YEARWEEK', F.year('DATE') * 100 + F.ceil(F.dayofyear('DATE') / 7))
                    .groupBy('BANNER', 'DP_NAME', 'PromotionId', 'YEARWEEK')
                    .agg(
                        *[F.when(F.sum('PROMO_' + gr) > 0, 1).otherwise(0).alias('PROMO_' + gr) for gr in groups],
                        F.when(F.sum('PROMO_OTHER') > 0, 1).otherwise(0).alias('PROMO_OTHER')
                        )
                    .groupBy('BANNER', 'DP_NAME', 'YEARWEEK')
                    .agg(
                        *[F.count(F.when(F.col('PROMO_' + gr) == 1, True)).alias('COUNT_' + gr) for gr in groups],
                        F.count(F.when(F.col('PROMO_OTHER') == 1, True)).alias('COUNT_OTHER'),
                        F.count('YEARWEEK').alias('NUM_PROMO')
                        )
                    .orderBy('BANNER', 'DP_NAME', 'YEARWEEK'))

# COMMAND ----------

df_promo.display()

# COMMAND ----------

df_promo.write.mode('overwrite').parquet('/mnt/adls/MT_FC/DATA/TMP/231223_PROMO_COUNT.parquet')

# COMMAND ----------

df_promo.select('InvestmentType').dropDuplicates().display()

# COMMAND ----------

df_promo.display()

# COMMAND ----------

df_promo.printSchema()

# COMMAND ----------


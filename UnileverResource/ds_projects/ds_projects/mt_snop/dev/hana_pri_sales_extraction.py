# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

import pandas as pd
import numpy as np

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

# COMMAND ----------

HANA_PRI_SALES_PATH = '/mnt/adls/Prod_UDL/TechDebt/InternalSources/U2K2BW/OpenHubFileDestination/PrimarySales/Vietnam/Processed'
HANA_CUSTOMER_MASTER_PATH = '/mnt/adls/Prod_UDL/TechDebt/InternalSources/U2K2BW/OpenHubFileDestination/CustomerMaster/SouthEastAsiaAustralasia/Processed_Parquet'

PRODUCT_HIERARCHY_PATH = '/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/LANDING-MASTER/Master Data Total Cat.xlsx'

hana_primary_original = spark.read.csv(HANA_PRI_SALES_PATH, sep = '|', header = True)
hana_customer_original = spark.read.format("delta").load(HANA_CUSTOMER_MASTER_PATH)

code_mapping_original = spark.createDataFrame(pd.read_excel(PRODUCT_HIERARCHY_PATH, sheet_name = 'Code Master'))
dp_mapping_original = spark.createDataFrame(pd.read_excel(PRODUCT_HIERARCHY_PATH, sheet_name = 'DP Name Master'))

# COMMAND ----------

hana_primary = hana_primary_original
hana_customer = hana_customer_original

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

hana_customer = (hana_customer.drop('PLANT')
                              .filter((F.col('COUNTRY') == 'VN') & (F.col('ZCUSTNAME8') == 'MODERN TRADE'))
                              .select('/BIC/ZPA_HIE04', 'ZCUSTNAME4')
                              .withColumnRenamed('/BIC/ZPA_HIE04', 'BANNER_CODE')
                              .na.drop()
                              .dropDuplicates())

hana_customer = (hana_customer.join(customer_mapping, hana_customer.ZCUSTNAME4 == customer_mapping.CUSTOMER, 'left')
                              .drop('ZCUSTNAME4', 'CUSTOMER')
                              .na.drop())

# COMMAND ----------

hana_primary = (hana_primary_original.select('/BIC/ZPA_HIE04', 'MATERIAL', 'CALDAY', 'G_QABSMG', 'G_AVV010')
                                     .withColumnRenamed('/BIC/ZPA_HIE04', 'CODE')
                                     .withColumn('MATERIAL_TMP', F.col('MATERIAL').cast('int')))

hana_primary = (hana_primary.join(hana_customer, hana_primary.CODE == hana_customer.BANNER_CODE, 'left')
                            .join(code_mapping, hana_primary.MATERIAL_TMP == code_mapping.SKUID, 'left')
                            .na.drop(subset = ['BANNER', 'DP_NAME'])
                            .withColumns({
                                            'DATE': F.to_date('CALDAY', 'yyyyMMdd'),
                                            'DP_NAME': F.upper('DP_NAME')
                                         })
                            .withColumnsRenamed({
                                                  'G_QABSMG': 'SALES_QTY',
                                                  'G_AVV010': 'SALES_GSV'
                                                })
                            .select('BANNER', 'BANNER_CODE', 'DP_NAME', 'MATERIAL_TMP', 'DATE', 'SALES_QTY', 'SALES_GSV')
                            .withColumnRenamed('MATERIAL_TMP', 'MATERIAL'))

# COMMAND ----------

to_drop = (hana_primary.groupBy('BANNER', 'DP_NAME')
                       .agg(
                             F.max('DATE').alias('LAST_DATE')
                           )
                       .withColumns({
                                      'CURRENT_DATE': F.lit(F.current_date()),
                                      'DATE_DIFF': F.datediff('CURRENT_DATE', 'LAST_DATE'),
                                      'TO_DROP': F.col('DATE_DIFF') > 90
                                    })
                       .groupBy('DP_NAME')
                       .agg(
                            F.count(F.when(F.col('TO_DROP') == True, True)).alias('COUNT_DROP'),
                           )
                       .filter(
                                (F.col('COUNT_DROP') > 0)
                              )
                       .select('DP_NAME')
                       .toPandas()['DP_NAME']
                       .values.tolist())

# COMMAND ----------

hana_primary = (hana_primary.filter(~F.col('DP_NAME').isin(to_drop)).orderBy('BANNER', 'DP_NAME', 'DATE'))

# COMMAND ----------

# hana_customer.write.mode('overwrite').parquet('/mnt/adls/MT_FC/DATA/TMP/240111_HANA_CUSTOMER.parquet')
hana_customer = spark.read.parquet('/mnt/adls/MT_FC/DATA/TMP/240111_HANA_CUSTOMER.parquet')

# COMMAND ----------

# hana_primary.write.mode('overwrite').parquet('/mnt/adls/MT_FC/DATA/TMP/240111_HANA_PRIMARY.parquet')
hana_primary = spark.read.parquet('/mnt/adls/MT_FC/DATA/TMP/240111_HANA_PRIMARY.parquet')

# COMMAND ----------

def hana_pri_filter(banners = None, dp_names = None):
  to_return = hana_primary

  if dp_names != None:
    dp_names = dp_names if isinstance(dp_names, list) else [dp_names]

    to_return = to_return.filter(F.col('DP_NAME').isin(dp_names))

  if banners != None:
    banners = banners if isinstance(banners, list) else [banners]

    to_return = to_return.filter(F.col('BANNER').isin(banners))

  (to_return.groupBy('BANNER', 'DP_NAME')
            .agg(F.max('DATE').alias('LAST_DATE'))
            .withColumns({
                          'CURRENT_DATE': F.lit(F.current_date()),
                          'DATE_DIFF': F.date_diff('CURRENT_DATE', 'LAST_DATE'),
                          'TO_DROP': F.col('DATE_DIFF') > 90
                        })
            .orderBy('DP_NAME', 'BANNER')
            .display())

# COMMAND ----------

banners = 'BACH HOA XANH'
dp_names = 'OMO LIQUID MATIC FLL BEAUTY CARE (POU) 2KG'

hana_pri_filter(banners, dp_names)

# COMMAND ----------

(hana_primary.groupBy('BANNER', 'DP_NAME')
             .agg(F.max('DATE').alias('LAST_DATE'))
             .withColumn('CURRENT_DATE', F.lit(F.current_date()))
             .withColumn('DATE_DIFF', F.date_diff('CURRENT_DATE', 'LAST_DATE'))
             .withColumn('TO_DROP', F.col('DATE_DIFF') > 90)
             .orderBy('BANNER', 'DP_NAME')
             .display())

# COMMAND ----------

(hana_primary.groupBy('BANNER', 'DP_NAME')
             .agg(F.max('DATE').alias('LAST_DATE'))
             .withColumn('CURRENT_DATE', F.lit(F.current_date()))
             .withColumn('DATE_DIFF', F.date_diff('CURRENT_DATE', 'LAST_DATE'))
             .withColumn('TO_DROP', F.col('DATE_DIFF') > 90)
             .orderBy('BANNER', 'DP_NAME')
             .filter(F.col('TO_DROP') == True)
             .count())

# COMMAND ----------

(hana_primary.groupBy('BANNER', 'DP_NAME')
             .agg(F.max('DATE').alias('LAST_DATE'))
             .withColumn('CURRENT_DATE', F.lit(F.current_date()))
             .withColumn('DATE_DIFF', F.date_diff('CURRENT_DATE', 'LAST_DATE'))
             .withColumn('TO_DROP', F.col('DATE_DIFF') > 90)
             .orderBy('BANNER', 'DP_NAME')
             .count())

# COMMAND ----------

7911 / 15583

# COMMAND ----------

hana_primary_check = (hana_primary.groupBy('BANNER', 'DP_NAME')
                                  .agg(F.max('DATE').alias('LAST_DATE'))
                                  .withColumn('CURRENT_DATE', F.lit(F.current_date()))
                                  .withColumn('DATE_DIFF', F.date_diff('CURRENT_DATE', 'LAST_DATE'))
                                  .withColumn('TO_DROP', F.col('DATE_DIFF') > 90)
                                  .orderBy('BANNER', 'DP_NAME')
                                  .cache())

# COMMAND ----------

(hana_primary_check.groupBy('DP_NAME')
                   .agg(
                        F.min('DATE_DIFF').alias('MIN_DIFF'),
                        F.max('DATE_DIFF').alias('MAX_DIFF'),
                        F.count(F.when(F.col('DATE_DIFF') <= 90, True)).alias('NUM_BANNER_TO_KEEP'),
                        F.count(F.when(F.col('DATE_DIFF') > 90, True)).alias('NUM_BANNER_TO_DROP')
                       )
                   .orderBy('DP_NAME')
                   .display())

# COMMAND ----------

(hana_primary_check.groupBy('DP_NAME')
                   .agg(
                        F.min('DATE_DIFF').alias('MIN_DIFF'),
                        F.max('DATE_DIFF').alias('MAX_DIFF'),
                        F.count(F.when(F.col('DATE_DIFF') <= 90, True)).alias('NUM_BANNER_TO_KEEP'),
                        F.count(F.when(F.col('DATE_DIFF') > 90, True)).alias('NUM_BANNER_TO_DROP')
                       )
                   .filter(
                            (F.col('NUM_BANNER_TO_KEEP') > 0) &
                            (F.col('NUM_BANNER_TO_DROP') > 0)
                          )
                   .orderBy('DP_NAME')
                   .display())

# COMMAND ----------

dp_list = (hana_primary_check.groupBy('DP_NAME')
                   .agg(
                        F.min('DATE_DIFF').alias('MIN_DIFF'),
                        F.max('DATE_DIFF').alias('MAX_DIFF'),
                        F.count(F.when(F.col('DATE_DIFF') <= 90, True)).alias('NUM_BANNER_TO_KEEP'),
                        F.count(F.when(F.col('DATE_DIFF') > 90, True)).alias('NUM_BANNER_TO_DROP')
                       )
                   .filter(
                            (F.col('NUM_BANNER_TO_KEEP') > 0) &
                            (F.col('NUM_BANNER_TO_DROP') > 0)
                          )
                   .orderBy('DP_NAME').select('DP_NAME').toPandas()['DP_NAME'].values.tolist())

# COMMAND ----------

len(dp_list)

# COMMAND ----------

hana_pri_filter(dp_names = dp_list)

# COMMAND ----------

banner = 'BIG_C'
dp_name = 'DOVE SHAMPOO ANTI HAIRFALL 325G'

(hana_primary.filter((F.col('BANNER') == banner) & (F.col('DP_NAME') == dp_name))
             .groupBy('BANNER', 'BANNER_CODE', 'DP_NAME', 'MATERIAL', 'DATE')
             .agg(F.sum('SALES_QTY').alias('SALES_QTY'), F.sum('SALES_GSV').alias('SALES_GSV'))
             .orderBy(F.col('DATE').desc())
             .display())

hana_primary.filter((F.col('BANNER') == banner) & (F.col('DP_NAME') == dp_name)).select('MATERIAL').distinct().display()

# COMMAND ----------


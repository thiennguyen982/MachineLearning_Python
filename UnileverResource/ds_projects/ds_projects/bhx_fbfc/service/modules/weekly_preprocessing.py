# Databricks notebook source
# Install libraries
%pip install openpyxl ray

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Import essential libraries for further usages

# COMMAND ----------

# Runtime-related libraries
import logging
import warnings

from tqdm.auto import tqdm

# General libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from pandas.tseries.offsets import DateOffset

# Parallel-related libraries
import ray
import pyspark.sql.types as types
import pyspark.sql.functions as F

from pyspark.sql.window import Window

# COMMAND ----------

# First-step configurations

# Disable warnings
logging.getLogger("prophet").setLevel(logging.ERROR)
logging.getLogger("cmdstanpy").disabled = True
logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)
logging.getLogger("py4j.clientserver").setLevel(logging.ERROR)
warnings.filterwarnings("ignore")

# Config matplotlib's graphs
plt.rcParams['figure.figsize'] = [40, 20]
plt.rcParams['font.size'] = 20

%matplotlib inline

# Config Pandas's float to .xx
pd.options.display.float_format = "{:,.2f}".format

# Turn on Ray
if not ray.is_initialized():
  ray.init()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Define paths to data

# COMMAND ----------

SALES_OUT_PATH = '/mnt/adls/MDL_Prod/Bronze/BHX/SaleOutReport/VN/Processed/'
PRODUCT_HIERARCHY_PATH = '/mnt/adls/BHX_FC/FBFC/DATA/PRODUCT_MASTER.csv'
PRODUCT_MASTER_PATH = '/mnt/adls/MDL_Prod/Silver/ModernTrade/Dimension/dim_customer_product_mapping'

PROMOTION_PATH = '/dbfs/mnt/adls/BHX_FC/FBFC/DATA/PROMOTION_UPDATE_AUG_23.csv'
CALENDAR_PATH = '/mnt/adls/BHX_FC/FBFC/DATA/CALENDAR.csv'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Functions to read data

# COMMAND ----------

def read_sales_data(SALES_OUT_PATH, PRODUCT_HIERARCHY_PATH, PRODUCT_MASTER_PATH):
  (spark.read.load(SALES_OUT_PATH)
             .where("YYYY = 2023")
             .createOrReplaceTempView('sales_df'))

  (spark.read.format("csv")
             .options(delimiter = ",", header = "True")
             .load(PRODUCT_HIERARCHY_PATH)
             .createOrReplaceTempView('product_master'))

  (spark.read.format('delta')
             .load(PRODUCT_MASTER_PATH)
             .where("Country = 'VN'")
             .createOrReplaceTempView('product_mapping'))

# COMMAND ----------

def read_promotion_data(PROMOTION_PATH):
  return pd.read_csv(PROMOTION_PATH).drop(columns = ['Unnamed: 0', 'S_E', 'E_E']).dropna(subset = ['SKUID'])

# COMMAND ----------

def read_calendar_data(CALENDAR_PATH):
  return spark.read.format('csv').option('header', True).load(CALENDAR_PATH)

# COMMAND ----------

def read_data():
  read_sales_data(SALES_OUT_PATH, PRODUCT_HIERARCHY_PATH, PRODUCT_MASTER_PATH)
  return read_promotion_data(PROMOTION_PATH), read_calendar_data(CALENDAR_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Functions to pre-process data

# COMMAND ----------



# COMMAND ----------

def pp_sales_data():
  spark.sql("""
                CREATE OR REPLACE TEMP VIEW product_code AS
                SELECT 
                      customer_sku_code, 
                      sku_code
                FROM (
                      SELECT 
                            customer_sku_code, 
                            sku_code, 
                            ROW_NUMBER() OVER (PARTITION BY customer_sku_code ORDER BY sku_code) AS `num`
                      FROM product_mapping
                      WHERE customer_code = 'CUS-BHX'
                      ORDER BY customer_sku_code
                    )
                WHERE `num` = 1
            """)

  spark.sql("""
                CREATE OR REPLACE TEMP VIEW tmp_data AS
                SELECT 
                      T1.STORE_ID, 
                      T1.STORE_NAME, 
                      T1.PRODUCT_ID, 
                      T1.PRODUCT_NAME, 
                      T1.SALES_QTY, 
                      T1.`DATE`, 
                      T2.sku_code AS SKUID
                FROM sales_df AS T1
                LEFT JOIN product_code AS T2
                ON T1.PRODUCT_ID = T2.customer_sku_code
            """)
  
  spark.sql("""
                CREATE OR REPLACE TEMP VIEW product_master_rework AS
                SELECT *
                FROM product_master
                WHERE `Small C` NOT IN ('Ice Cream')
            """)
  
  spark.sql("""
                CREATE OR REPLACE TEMP VIEW sales_data AS
                SELECT * 
                FROM (
                      SELECT 
                            T1.*, 
                            T2.`CD DP Name` AS DP_NAME,
                            T2.`Small C` AS CATEGORY,
                            T2.`Segment` AS SEGMENT,
                            T2.`Packsize` AS PACKSIZE
                      FROM tmp_data AS T1
                      LEFT JOIN product_master_rework AS T2
                      ON T1.SKUID = T2.Material
                    )
                WHERE (SKUID IS NOT NULL) AND (CATEGORY IS NOT NULL) AND (STORE_NAME NOT LIKE ('% Kho %'))
            """)

# COMMAND ----------

def __explode(row):
  start_date = row['START_DATE']
  end_date = row['END_DATE']

  res = pd.DataFrame()
  res['DATE'] = pd.date_range(start_date, end_date, freq = 'd')
  res['DP_NAME'] = [row['DP_NAME']] * len(res)
  res['DISCOUNT'] = [row['DISCOUNT']] * len(res)

  return res[['DP_NAME', 'DATE', 'DISCOUNT']]

@ray.remote
def __REMOTE_explode(row):
  return __explode(row)

def pp_promotion_data(promotion):
  promotion['SKUID'] = promotion['SKUID'].astype('int')
  promotion = promotion[['SKUID', 'START_DATE', 'END_DATE', 'DISCOUNT']]

  spark.createDataFrame(promotion).createOrReplaceTempView('promotion')

  promotion = (spark.sql("""
                          SELECT t1.*, t2.`CD DP Name` AS DP_NAME
                          FROM promotion AS t1
                          LEFT JOIN product_master AS t2
                          ON t1.SKUID = t2.Material
                         """)
                    .toPandas())
  
  tasks = [__REMOTE_explode.remote(promotion.iloc[i]) for i in range(len(promotion))]
  promotion = pd.concat(ray.get(tasks))

  promotion = (spark.createDataFrame(promotion)
                  .withColumn('YEARWEEK', F.year('DATE') * 100 + F.ceil(F.dayofyear('DATE') / 7))
                  .groupBy('DP_NAME', 'YEARWEEK')
                  .agg(F.max('DISCOUNT').alias('DISCOUNT')))
  
  return promotion

# COMMAND ----------

def pp_calendar_data(calendar):
  calendar = calendar.select('YEARWEEK', 'YEARQUARTER', 'YEARHALF', 'HOLIDAYNAME', 'SPECIALEVENTNAME', 'WEEKTYPE','QUARTERTYPE')

  cols = ['YEARQUARTER', 'YEARHALF']

  for col in cols:
    calendar = calendar.withColumn(col, F.col(col).cast('int'))

  calendar = (calendar.withColumn('YEARQUARTER', F.col('YEARQUARTER') % 10)
                      .withColumn('YEARHALF', F.col('YEARHALF') % 10))
  
  return calendar

# COMMAND ----------

def pp():
  promotion, calendar = read_data()

  pp_sales_data()
  promotion = pp_promotion_data(promotion)
  calendar = pp_calendar_data(calendar)

  return promotion, calendar

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Run functions

# COMMAND ----------

promotion, calendar = pp()
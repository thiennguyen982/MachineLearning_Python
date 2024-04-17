# Databricks notebook source
pip install openpyxl pandas --upgrade

# COMMAND ----------

import re
import pandas as pd
import datetime as dt
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking data update

# COMMAND ----------

# """ Shopee check """
# FileInfo =  dbutils.fs.ls("dbfs:/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_ProductPerformance/")
# file_paths = [FileInfo[i][0] for i in range(0,len(FileInfo))]
# file_names = [FileInfo[i][1] for i in range(0,len(FileInfo)) if re.match(r'shopee_product_performance_20.*\.snappy', FileInfo[i][1])]
# lastest_file = max(file_names)
# shopee_max_date = re.search("shopee_product_performance_(.*).snappy", lastest_file).group(1).replace("_", "-")

# yesterday = dt.datetime.now() + dt.timedelta(hours=7) + dt.timedelta(days=-1)

# if dt.datetime.strftime(yesterday, "%Y-%m-%d") == shopee_max_date:
#   True
#   print("Shopee data updated")
# else:
#   print("Shopee data has NOT updated till yesterday!")
#   pass


# yesterday = dt.datetime.now() + dt.timedelta(hours=7) + dt.timedelta(days=-1)

# if not shopee_max_date == dt.datetime.strftime(yesterday, "%Y-%m-%d"):
#   raise Exception("\nJob exit because data is not updated!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Master mapping

# COMMAND ----------

master_latest_version = spark.sql("SELECT max(version) FROM (DESCRIBE HISTORY delta.`dbfs:/mnt/adls/staging/ecom/master_customer/df_master_customer_delta/`)").collect()[0][0]

df_master_customer = spark.read.format("delta").option("versionAsOf", master_latest_version).load("dbfs:/mnt/adls/staging/ecom/master_customer/df_master_customer_delta").filter(col('platform') == 'shopee')

display(df_master_customer)
df_master_customer.createOrReplaceTempView("df_master_customer")

# COMMAND ----------

# %sql
# SELECT DISTINCT *
# FROM df_master_customer
# WHERE itemid = 2588286996


# COMMAND ----------

query_master_mapping = """
WITH data_single  AS (
  SELECT DISTINCT 
    platform, 
    SUBSTRING_INDEX(platform_sku,'_',1) as product_id,  
    combo_gift_single,
  --  prop, 
  --  IF(DP_name LIKE '%Not match', 0, 1) as is_match,
    Brand,  Small_C, Big_C
  FROM df_master_customer
  WHERE 
  platform = 'shopee'
  AND combo_gift_single != "Combo"
  -- AND  IF(DP_name LIKE '%Not match', 0, 1) = 1 -- filter out not-matched single SKU 
  ),

  data_combo AS (
  SELECT DISTINCT 
    platform, 
    SUBSTRING_INDEX(platform_sku,'_', 1) as product_id,  
    combo_gift_single,
  --  prop, 
  --  IF(DP_name LIKE '%Not match', 0, 1) as is_match,
    Brand,  Small_C, Big_C
  FROM df_master_customer
  WHERE 
  platform = 'shopee'
  AND combo_gift_single = "Combo"
  ),
  final AS (
    SELECT DISTINCT *
    FROM data_single
    UNION ALL
    SELECT DISTINCT *
    FROM data_combo
  ),
  final_rank_data AS (
    SELECT DISTINCT
      product_id, 
      platform, 
      combo_gift_single,
      Brand, 
      ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY Brand) as rank
    FROM final
  )
SELECT DISTINCT 
  platform, product_id, combo_gift_single, Brand
FROM final_rank_data
WHERE rank = 1
"""

df_master_mapping = spark.sql(query_master_mapping)

display(df_master_mapping)
df_master_mapping.createOrReplaceTempView("df_master_mapping")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load master Local PH mapping

# COMMAND ----------

# # Load Unilever master data
filepath = '/mnt/adls/UDL_Gen2/UniversalDataLake/InternalSources/FileFMT/BlobFileShare/SecondarySales/Vietnam/ProdHierarchy/Processed/'
master_data_unilever = spark.read.format("csv").options(delimiter=",", header="True").load(filepath)

display(master_data_unilever)
master_data_unilever.createOrReplaceTempView("master_data_unilever")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Master period time

# COMMAND ----------

query_date_period = """
SELECT 
  DATE(current_timestamp + interval '7' hours) as today,
  DATE(current_timestamp + interval '7' hours - interval '1' days) as yesterday,
  DATE(current_timestamp + interval '7' hours - interval '7' days) as last7days_begin,
  DATE(current_timestamp + interval '7' hours - interval '30' days) as last30days_begin,

  DATE(current_timestamp + interval '7' hours - interval '2' days) as previous_yesterday,
  DATE(current_timestamp + interval '7' hours - interval '14' days) as previous_last7days_begin,
  DATE(current_timestamp + interval '7' hours - interval '8' days) as previous_last7days_end,
  DATE(current_timestamp + interval '7' hours - interval '60' days) as previous_last30days_begin,
  DATE(current_timestamp + interval '7' hours - interval '31' days) as previous_last30days_end,
  DATE(DATE_TRUNC('MONTH', current_timestamp + interval '7' hours )) as MTD_begin, 
  DATE(DATE_TRUNC('MONTH', current_timestamp + interval '7' hours - interval '1' months)) as previous_MTD_begin, 
  IF( DAY(DATE(current_timestamp + interval '7' hours - interval '1' days)) >
DAY(DATE(DATE_TRUNC('MONTH', current_timestamp + interval '7' hours) - interval '1' days)) ,

DATE(DATE_TRUNC('MONTH', current_timestamp + interval '7' hours) - interval '1' days), 
DATE_ADD(DATE_TRUNC('MONTH', current_timestamp + interval '7' hours - interval '1' months),
DATEDIFF(DATE(current_timestamp + interval '7' hours - interval '1' days), DATE_TRUNC('MONTH', current_timestamp + interval '7' hours)))) as previous_MTD_end
"""

df_date_period = spark.sql(query_date_period)
df_date_period.createOrReplaceTempView("df_date_period")
display(df_date_period)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Shopee Data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Daily seperate date data

# COMMAND ----------

df_shopee_product_performmance_date_separate = spark.read.format("parquet").load("dbfs:/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_ProductPerformance/")

df_shopee_product_performmance_date_separate.createOrReplaceTempView("df_shopee_product_performmance_date_separate")
display(df_shopee_product_performmance_date_separate)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT Date
# MAGIC FROM df_shopee_product_performmance_date_separate
# MAGIC -- WHERE date >= '2021-01-01' AND date <= '2021-08-31'
# MAGIC -- GROUP BY 1
# MAGIC ORDER BY Date DESC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create master tree mapping data for Shopee

# COMMAND ----------

query_shopee_master_tree = """
WITH data AS (
SELECT DISTINCT 
    SUBSTRING_INDEX(platform_sku, "_",1) AS product_id, platform_name, combo_gift_single, Brand, 
    CASE WHEN (LOCATE('CIF',UPPER(platform_name)) > 0) THEN 'CIF' 
      WHEN (LOCATE('KNORR',UPPER(platform_name)) > 0) THEN 'KNORR' 
      WHEN (LOCATE('VIM',UPPER(platform_name)) > 0) THEN 'VIM' 
      WHEN (LOCATE('LOVE BEAUTY', UPPER(platform_name)) > 0) THEN 'LOVE BEAUTY AND PLANET'
      WHEN (LOCATE('SEVENTH GENERATION',UPPER(platform_name)) > 0) THEN 'SEVENTH GENERATION'
      WHEN (LOCATE('LIPTON',UPPER(platform_name)) > 0) THEN 'LIPTON' 
      WHEN (LOCATE('P/S',UPPER(platform_name)) > 0) THEN 'P/S' 
      WHEN (LOCATE("PONDS",UPPER(platform_name)) > 0) OR (LOCATE("POND'S",UPPER(platform_name)) > 0) OR (LOCATE("POND’S", UPPER(platform_name)) > 0)  THEN "POND'S" 
      WHEN (LOCATE('HAZELINE',UPPER(platform_name)) > 0) THEN 'HAZELINE' 
      WHEN (LOCATE('VASELINE',UPPER(platform_name)) > 0) THEN 'VASELINE' 
      WHEN (LOCATE('DOVE',UPPER(platform_name)) > 0) THEN 'DOVE' 
      WHEN (LOCATE('TRESEMME',UPPER(platform_name)) > 0) OR (LOCATE('TRESEMMÉ',UPPER(platform_name)) > 0) THEN 'TRESEMME' 
      WHEN (LOCATE('CLOSEUP',UPPER(platform_name)) > 0) OR (LOCATE('CLOSE UP',UPPER(platform_name)) > 0) THEN 'CLOSEUP' 
      WHEN (LOCATE('Gift - Not match',UPPER(platform_name)) > 0) THEN 'Gift - Not match' 
      WHEN (LOCATE('SUNLIGHT',UPPER(platform_name)) > 0) AND (LOCATE('RỬA CHÉN',UPPER(platform_name)) > 0) THEN 'SUNLIGHT DW' 
      WHEN (LOCATE('SUNLIGHT',UPPER(platform_name)) > 0) AND (LOCATE('LAU SÀN',UPPER(platform_name)) > 0) THEN 'SUNLIGHT FLC'
      WHEN (LOCATE('OMO',UPPER(platform_name)) > 0) THEN 'OMO' 
      WHEN (LOCATE('COMFORT',UPPER(platform_name)) > 0) THEN 'COMFORT' 
      WHEN (LOCATE('SURF',UPPER(platform_name)) > 0) THEN 'SURF' 
      WHEN (LOCATE('DOVE DERMASERIES',UPPER(platform_name)) > 0) THEN 'DOVE DERMASERIES' 
      WHEN (LOCATE('REXONA',UPPER(platform_name)) > 0) THEN 'REXONA' 
      WHEN (LOCATE('AXE',UPPER(platform_name)) > 0) THEN 'AXE' 
      WHEN (LOCATE('LIFEBUOY',UPPER(platform_name)) > 0) THEN 'LIFEBUOY' 
      WHEN (LOCATE('SUNSILK',UPPER(platform_name)) > 0) THEN 'SUNSILK' 
      WHEN (LOCATE('MICHIRU',UPPER(platform_name)) > 0) THEN 'MICHIRU' 
      WHEN (LOCATE('LUX',UPPER(platform_name)) > 0) THEN 'LUX'
      WHEN (LOCATE('SIMPLE',UPPER(platform_name)) > 0) THEN 'SIMPLE'
      WHEN (LOCATE('ST.IVES',UPPER(platform_name)) > 0) OR (LOCATE('ST. IVES',UPPER(platform_name)) > 0) THEN 'ST.IVES'
      WHEN (LOCATE('BABY DOVE',UPPER(platform_name)) > 0) THEN 'BABY DOVE'
      WHEN (LOCATE('CLEAR MEN',UPPER(platform_name)) > 0) THEN 'CLEAR MEN' 
      WHEN (LOCATE('CLEAR', UPPER(platform_name)) > 0) THEN 'CLEAR' 
    END AS brand_detect,
    Big_C, Small_C,
    CASE WHEN (LOCATE("DẦU GỘI", UPPER(platform_name)) > 0) OR (LOCATE("GỘI XẢ", UPPER(platform_name)) > 0 ) OR (LOCATE("DẦU XẢ", UPPER(platform_name)) > 0 ) OR (LOCATE("TÓC", UPPER(platform_name)) > 0 ) OR (LOCATE("KEM XẢ", UPPER(platform_name)) > 0) AND (LOCATE("SERUM", UPPER(platform_name)) =0) AND (LOCATE("SỮA RỬA MẶT", UPPER(platform_name)) =0) THEN "Hair"
       WHEN (LOCATE("SỮA TẮM", UPPER(platform_name)) > 0) OR (LOCATE("TẮM", UPPER(platform_name)) > 0) OR (LOCATE("RỬA TAY", UPPER(platform_name)) > 0) OR (LOCATE("DIỆT KHUẨN", UPPER(platform_name)) > 0 ) OR (LOCATE("XÀ PHÒNG", UPPER(platform_name)) > 0) AND LOCATE("RỬA CHÉN", UPPER(platform_name)) = 0 AND LOCATE("VIÊN TẨY", UPPER(platform_name)) = 0 AND LOCATE("NƯỚC XỊT VỆ SINH", UPPER(platform_name)) = 0 AND LOCATE("VIM", UPPER(platform_name)) = 0 THEN "SCL"
       WHEN (LOCATE("RỬA CHÉN", UPPER(platform_name)) > 0) OR (LOCATE("TẨY RỬA", UPPER(platform_name)) > 0) OR (LOCATE("LAU SÀN", UPPER(platform_name)) > 0) OR (LOCATE("TẨY BỒN CẦU", UPPER(platform_name)) > 0) OR (LOCATE("NƯỚC LAU ĐA NĂNG", UPPER(platform_name)) > 0) OR (LOCATE("NƯỚC XỊT VỆ SINH", UPPER(platform_name)) > 0) THEN "HNH" 
       WHEN (LOCATE("KEM ĐÁNH RĂNG", UPPER(platform_name)) > 0) THEN "Oral" 
       WHEN (LOCATE("NƯỚC GIẶT", UPPER(platform_name)) > 0) OR (LOCATE("BỘT GIẶT", UPPER(platform_name)) > 0) THEN "Fabclean" 
       WHEN (LOCATE("NƯỚC XẢ", UPPER(platform_name)) > 0) THEN "Fabenc" 
       WHEN (LOCATE("HẠT NÊM", UPPER(platform_name)) > 0) THEN "Food" 
       WHEN (LOCATE("DƯỠNG THỂ", UPPER(platform_name)) > 0) OR (LOCATE("DƯỠNG", UPPER(platform_name)) > 0) OR (LOCATE("SERUM", UPPER(platform_name)) > 0) OR (LOCATE("NƯỚC HOA HỒNG", UPPER(platform_name)) > 0) OR (LOCATE("RỬA MẶT", UPPER(platform_name)) > 0) OR (LOCATE("NƯỚC TẨY TRANG", UPPER(platform_name)) > 0) OR (LOCATE("LÃO HÓA", UPPER(platform_name)) > 0) OR (LOCATE("TRẮNG DA", UPPER(platform_name)) > 0) OR (LOCATE("PHẤN PHỦ", UPPER(platform_name)) > 0) OR (LOCATE("MẶT NẠ", UPPER(platform_name)) > 0) OR (LOCATE("KEM TẨY", UPPER(platform_name)) > 0) AND LOCATE("CIF", UPPER(platform_name)) = 0 AND LOCATE("KHỬ MÙI", UPPER(platform_name)) = 0 THEN "Skincare" 
       WHEN (LOCATE("MÙI", UPPER(platform_name)) > 0) OR (LOCATE("NƯỚC HOA", UPPER(platform_name)) > 0) AND ((LOCATE("AXE", UPPER(platform_name)) > 0) OR (LOCATE("REXONA", UPPER(platform_name)) > 0))  THEN "Deo" 
    END AS Small_C_detect, 
    ROW_NUMBER() OVER(PARTITION BY platform_sku ORDER BY Brand, Big_C, Small_C) as rank
  FROM 
    df_master_customer 
  WHERE 
    platform = "shopee"
  ),
  data_clean AS (
  SELECT product_id, platform_name, combo_gift_single, Big_C, 
    IF(Brand LIKE '%Not match', brand_detect, COALESCE(Brand, brand_detect)) as Brand, 
    IF(Small_C LIKE '%Not match', brand_detect, COALESCE(Small_C, Small_C_detect)) as Small_C
  FROM data
  WHERE rank = 1
  ),
  data_master AS (
SELECT DISTINCT  
  product_id,  platform_name, Brand, Big_C, Small_C,
  CASE WHEN (Big_C LIKE '%Not match' OR Big_C IS NULL) AND Small_C = 'HNH' THEN 'Homecare' 
    WHEN (Big_C LIKE '%Not match' OR Big_C IS NULL) AND Small_C = 'Combo - Not match' THEN 'Combo - Not match' 
    WHEN (Big_C LIKE '%Not match' OR Big_C IS NULL) AND Small_C = 'Food' THEN 'F&RFs' 
    WHEN (Big_C LIKE '%Not match' OR Big_C IS NULL) AND Small_C = 'Hair' THEN 'Personal Care' 
    WHEN (Big_C LIKE '%Not match' OR Big_C IS NULL) AND Small_C = 'SCL' THEN 'Personal Care' 
    WHEN (Big_C LIKE '%Not match' OR Big_C IS NULL) AND Small_C = 'Skincare' THEN 'Personal Care' 
    WHEN (Big_C LIKE '%Not match' OR Big_C IS NULL) AND Small_C = 'Single - Not match' THEN 'Single - Not match' 
    WHEN (Big_C LIKE '%Not match' OR Big_C IS NULL) AND Small_C = 'Tea' THEN 'F&RFs' 
    WHEN (Big_C LIKE '%Not match' OR Big_C IS NULL) AND Small_C = 'Oral' THEN 'Personal Care' 
    WHEN (Big_C LIKE '%Not match' OR Big_C IS NULL) AND Small_C = 'Gift - Not match' THEN 'Gift - Not match' 
    WHEN (Big_C LIKE '%Not match' OR Big_C IS NULL) AND Small_C = 'Fabclean' THEN 'Homecare' 
    WHEN (Big_C LIKE '%Not match' OR Big_C IS NULL) AND Small_C = 'Fabenc' THEN 'Homecare' 
    WHEN (Big_C LIKE '%Not match' OR Big_C IS NULL) AND Small_C = 'Deo' THEN 'Personal Care' 
    WHEN (Big_C LIKE '%Not match' OR Big_C IS NULL) AND Small_C = 'Combo' THEN 'Combo'
    WHEN (Big_C LIKE '%Not match' OR Big_C IS NULL) AND Small_C = 'Gift' THEN 'Gift'
  END AS Big_C_detect
FROM data_clean
)
SELECT DISTINCT 
  product_id, Brand, Small_C, COALESCE(Big_C, Big_C_detect) AS Big_C
FROM data_master
ORDER BY 1
"""

df_shopee_master_tree = spark.sql(query_shopee_master_tree)
df_shopee_master_tree.createOrReplaceTempView("df_shopee_master_tree")
display(df_shopee_master_tree)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write YTD Shopee Performance data

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Load data sales traffic

# COMMAND ----------

df_shopee_sales_performance_date_separate = spark.read.format("csv").option("header","True").option("inferSchema", "True").load("dbfs:/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_SalesPerformance/")

df_shopee_sales_performance_date_separate

# COMMAND ----------

cols_name = df_shopee_sales_performance_date_separate.columns

if 'Gross Product Views' in cols_name:
  df_shopee_sales_performance_date_separate = df_shopee_sales_performance_date_separate.withColumnRenamed("Gross Product Views", "Product Views")\
  .withColumnRenamed("Gross Unique Visitors","Product Visitors")
else:
  pass

### Check column name
df_shopee_sales_performance_date_separate.columns


# COMMAND ----------

### Create temp
df_shopee_sales_performance_date_separate.createOrReplaceTempView("df_shopee_sales_performance_date_separate")
display(df_shopee_sales_performance_date_separate)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write YTD data sales traffic

# COMMAND ----------

# Sales Traffic data
df_shopee_sales_traffic_ytd = spark.sql("""
WITH df_shopee_product_date_seperate_grouped AS (
  SELECT 
    Date, Name as platform_name, t1.product_id, shop_name, t2.Brand, Big_C, Small_C,
    SUM(current_stock) as current_stock, 
    SUM(gross_unique_buyers) as gross_unique_buyers, 
    SUM(gross_orders) as gross_orders, 
    SUM(gross_units_sold) as gross_units_sold, 
    SUM(gmv) as gmv, 
    SUM(net_unique_buyers) as net_unique_buyers, 
    SUM(net_orders) as net_orders,
    SUM(net_units_sold) as net_units_sold, 
    SUM(nmv) as nmv
  FROM df_shopee_product_performmance_date_separate t1
  LEFT JOIN df_shopee_master_tree t2
  ON t1.product_id = t2.product_id
  GROUP BY 1,2,3,4,5,6,7
  )
SELECT 
  p.Date,
  p.platform_name, 
  p.product_id, 
  p.shop_name, 
  IF(Brand IS NULL, 
  CASE WHEN LOCATE('GIFT', UPPER(platform_name)) > 0 OR LOCATE('GIIFT', UPPER(platform_name))> 0 OR LOCATE('HÀNG TẶNG', UPPER(platform_name)) > 0 THEN 'Gift'
      WHEN (LOCATE('CIF',UPPER(platform_name)) > 0) THEN 'CIF' 
      WHEN (LOCATE('KNORR',UPPER(platform_name)) > 0) THEN 'KNORR' 
      WHEN (LOCATE('VIM',UPPER(platform_name)) > 0) THEN 'VIM' 
      WHEN (LOCATE('LOVE BEAUTY',UPPER(platform_name)) > 0) THEN 'LOVE BEAUTY AND PLANET' 
      WHEN (LOCATE('LIPTON',UPPER(platform_name)) > 0) THEN 'LIPTON' 
      WHEN (LOCATE('CLEAR',UPPER(platform_name)) > 0) THEN 'CLEAR' 
      WHEN (LOCATE('P/S',UPPER(platform_name)) > 0) THEN 'P/S' 
      WHEN (LOCATE("PONDS",UPPER(platform_name)) > 0) OR (LOCATE("POND'S",UPPER(platform_name)) > 0) OR (LOCATE("POND’S",UPPER(platform_name)) > 0)  THEN "POND'S" 
      WHEN (LOCATE('HAZELINE',UPPER(platform_name)) > 0) THEN 'HAZELINE' 
      WHEN (LOCATE('VASELINE',UPPER(platform_name)) > 0) THEN 'VASELINE' 
      WHEN (LOCATE('DOVE',UPPER(platform_name)) > 0) THEN 'DOVE' 
      WHEN (LOCATE('TRESEMME',UPPER(platform_name)) > 0) OR (LOCATE('TRESEMMÉ',UPPER(platform_name)) > 0) THEN 'TRESEMME' 
      WHEN (LOCATE('CLOSEUP',UPPER(platform_name)) > 0) OR (LOCATE('CLOSE UP',UPPER(platform_name)) > 0) THEN 'CLOSEUP' 
      WHEN (LOCATE('Gift - Not match',UPPER(platform_name)) > 0) THEN 'Gift - Not match' 
      WHEN (LOCATE('SUNLIGHT',UPPER(platform_name)) > 0) AND (LOCATE('RỬA CHÉN',UPPER(platform_name)) > 0) THEN 'SUNLIGHT DW' 
      WHEN (LOCATE('SUNLIGHT',UPPER(platform_name)) > 0) AND (LOCATE('LAU SÀN',UPPER(platform_name)) > 0) THEN 'SUNLIGHT FLC' 
      WHEN (LOCATE('OMO',UPPER(platform_name)) > 0) THEN 'OMO' 
      WHEN (LOCATE('COMFORT',UPPER(platform_name)) > 0) THEN 'COMFORT' 
      WHEN (LOCATE('SURF',UPPER(platform_name)) > 0) THEN 'SURF' 
      WHEN (LOCATE('SEVENTH GENERATION',UPPER(platform_name)) > 0) THEN 'SEVENTH GENERATION' 
      WHEN (LOCATE('DOVE DERMASERIES',UPPER(platform_name)) > 0) THEN 'DOVE DERMASERIES' 
      WHEN (LOCATE('REXONA',UPPER(platform_name)) > 0) THEN 'REXONA' 
      WHEN (LOCATE('AXE',UPPER(platform_name)) > 0) THEN 'AXE' 
      WHEN (LOCATE('LIFEBUOY',UPPER(platform_name)) > 0) THEN 'LIFEBUOY' 
      WHEN (LOCATE('SUNSILK',UPPER(platform_name)) > 0) THEN 'SUNSILK' 
      WHEN (LOCATE('CLEAR MEN',UPPER(platform_name)) > 0) THEN 'CLEAR MEN' 
      WHEN (LOCATE('MICHIRU',UPPER(platform_name)) > 0) THEN 'MICHIRU' 
      WHEN (LOCATE('BABY DOVE',UPPER(platform_name)) > 0) THEN 'BABY DOVE' 
      WHEN (LOCATE('LUX',UPPER(platform_name)) > 0) THEN 'LUX'
    END, 
    Brand) AS Brand,
  CASE WHEN LOCATE('GIFT', UPPER(platform_name)) > 0 OR LOCATE('GIIFT', UPPER(platform_name))> 0 OR LOCATE('HÀNG TẶNG', UPPER(platform_name)) > 0 THEN 'Gift'
    ELSE Big_C
  END AS Big_C, 
  CASE WHEN LOCATE('GIFT', UPPER(platform_name)) > 0 OR LOCATE('GIIFT', UPPER(platform_name))> 0 OR LOCATE('HÀNG TẶNG', UPPER(platform_name)) > 0 THEN 'Gift'
    ELSE Small_C
  END AS Small_C, 
  
  s.`Product Visitors` as gross_unique_visitors,
  s.`Product Views` as gross_product_views, 
  p.gross_unique_buyers,
  p.gross_orders,
  p.gross_units_sold,
  s.`Gross Item Conversion Rate` as gross_item_cr,
  p.current_stock,
  p.gmv,
  p.net_unique_buyers,
  p.net_orders,
  p.net_units_sold,
  p.nmv
FROM df_shopee_product_date_seperate_grouped p
JOIN df_shopee_sales_performance_date_separate s
ON p.Date = s.Date
AND p.product_id = s.`Product ID`
""")
df_shopee_sales_traffic_ytd.createOrReplaceTempView("df_shopee_sales_traffic_ytd")
display(df_shopee_sales_traffic_ytd)

# Write YTD data sales- traffic 
df_shopee_sales_traffic_ytd.toPandas().to_csv("/dbfs/mnt/adls/staging/ecom/YTD Data/shopee/df_shopee_sales_traffic_ytd.csv", index=False, encoding='utf-8-sig')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write YTD product performance 

# COMMAND ----------

# Product performance data
df_shopee_product_performmance_ytd = spark.sql("""
SELECT 
  Date, Name, URL, t1.product_id, parent_sku, Variation, SKU, Region, shopid, shop_name, 
  IF (t2.Brand IS NULL, 
    CASE WHEN LOCATE('GIFT', UPPER(Name)) > 0 OR LOCATE('GIIFT', UPPER(Name))> 0 OR LOCATE('HÀNG TẶNG', UPPER(Name)) > 0 THEN 'Gift'
      WHEN (LOCATE('CIF',UPPER(Name)) > 0) THEN 'CIF' 
      WHEN (LOCATE('KNORR',UPPER(Name)) > 0) THEN 'KNORR' 
      WHEN (LOCATE('VIM',UPPER(Name)) > 0) THEN 'VIM' 
      WHEN (LOCATE('LOVE BEAUTY',UPPER(Name)) > 0) THEN 'LOVE BEAUTY AND PLANET' 
      WHEN (LOCATE('LIPTON',UPPER(Name)) > 0) THEN 'LIPTON' 
      WHEN (LOCATE('CLEAR',UPPER(Name)) > 0) THEN 'CLEAR' 
      WHEN (LOCATE('P/S',UPPER(Name)) > 0) THEN 'P/S' 
      WHEN (LOCATE("PONDS",UPPER(Name)) > 0) OR (LOCATE("POND'S",UPPER(Name)) > 0) OR (LOCATE("POND’S",UPPER(Name)) > 0)  THEN "POND'S" 
      WHEN (LOCATE('HAZELINE',UPPER(Name)) > 0) THEN 'HAZELINE' 
      WHEN (LOCATE('VASELINE',UPPER(Name)) > 0) THEN 'VASELINE' 
      WHEN (LOCATE('DOVE',UPPER(Name)) > 0) THEN 'DOVE' 
      WHEN (LOCATE('TRESEMME',UPPER(Name)) > 0) OR (LOCATE('TRESEMMÉ',UPPER(Name)) > 0) THEN 'TRESEMME' 
      WHEN (LOCATE('CLOSEUP',UPPER(Name)) > 0) OR (LOCATE('CLOSE UP',UPPER(Name)) > 0) THEN 'CLOSEUP' 
      WHEN (LOCATE('Gift - Not match',UPPER(Name)) > 0) THEN 'Gift - Not match' 
      WHEN (LOCATE('SUNLIGHT',UPPER(Name)) > 0) AND (LOCATE('RỬA CHÉN',UPPER(Name)) > 0) THEN 'SUNLIGHT DW' 
      WHEN (LOCATE('SUNLIGHT',UPPER(Name)) > 0) AND (LOCATE('LAU SÀN',UPPER(Name)) > 0) THEN 'SUNLIGHT FLC' 
      WHEN (LOCATE('OMO',UPPER(Name)) > 0) THEN 'OMO' 
      WHEN (LOCATE('COMFORT',UPPER(Name)) > 0) THEN 'COMFORT' 
      WHEN (LOCATE('SURF',UPPER(Name)) > 0) THEN 'SURF' 
      WHEN (LOCATE('SEVENTH GENERATION',UPPER(Name)) > 0) THEN 'SEVENTH GENERATION' 
      WHEN (LOCATE('DOVE DERMASERIES',UPPER(Name)) > 0) THEN 'DOVE DERMASERIES' 
      WHEN (LOCATE('REXONA',UPPER(Name)) > 0) THEN 'REXONA' 
      WHEN (LOCATE('AXE',UPPER(Name)) > 0) THEN 'AXE' 
      WHEN (LOCATE('LIFEBUOY',UPPER(Name)) > 0) THEN 'LIFEBUOY' 
      WHEN (LOCATE('SUNSILK',UPPER(Name)) > 0) THEN 'SUNSILK' 
      WHEN (LOCATE('CLEAR MEN',UPPER(Name)) > 0) THEN 'CLEAR MEN' 
      WHEN (LOCATE('MICHIRU',UPPER(Name)) > 0) THEN 'MICHIRU' 
      WHEN (LOCATE('BABY DOVE',UPPER(Name)) > 0) THEN 'BABY DOVE' 
      WHEN (LOCATE('LUX',UPPER(Name)) > 0) THEN 'LUX'
    END, 
    t2.Brand) AS Brand,
  CASE WHEN LOCATE('GIFT', UPPER(Name)) > 0 OR LOCATE('GIIFT', UPPER(Name))> 0 OR LOCATE('HÀNG TẶNG', UPPER(Name)) > 0 THEN 'Gift'
    ELSE t2.Big_C
  END AS Big_C, 
  CASE WHEN LOCATE('GIFT', UPPER(Name)) > 0 OR LOCATE('GIIFT', UPPER(Name))> 0 OR LOCATE('HÀNG TẶNG', UPPER(Name)) > 0 THEN 'Gift'
    ELSE t2.Small_C
  END AS Small_C,  
  product_rating, net_units_sold, net_orders, nmv, net_unique_buyers, gross_units_sold, gross_orders, gmv, gross_unique_buyers, current_stock
FROM 
  df_shopee_product_performmance_date_separate t1
LEFT JOIN 
  df_shopee_master_tree t2
ON t1.product_id = t2.product_id
AND t1.Date >= '2020-01-01'
WHERE t1.Date like '202%'  -- FILTER out period date
  """)
df_shopee_product_performmance_ytd.createOrReplaceTempView("df_shopee_product_performmance_ytd")
display(df_shopee_product_performmance_ytd)

# WRITE DATA
df_shopee_product_performmance_ytd.toPandas().to_csv("/dbfs/mnt/adls/staging/ecom/YTD Data/shopee/shopee_product_performance_ytd.csv", index=False, encoding='utf-8-sig')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Product Inventory

# COMMAND ----------

import pandas as pd
import numpy as np
import datetime
import glob
import os
from os import path
import sys
import time
from collections import OrderedDict
from datetime import timedelta
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Window

spark.conf.set("spark.sql.execution.arrow.enabled","true")


# COMMAND ----------

# Load Master data active SKU
dtype_format= {'core_sku': str, 
               'power_sku': str, 
               'product_sku': str,
               'platform': str,
               'status': str,
               'unilever_code':str,
               'product_name': str}

df_sku_active = pd.read_excel("/dbfs/mnt/adls/staging/ecom/master_data/SKU inventory Active.xlsx", sheet_name='Shopee', skiprows = 0, dtype = dtype_format, keep_default_na= False)

df_sku_active = spark.createDataFrame(df_sku_active) 
df_sku_active.createOrReplaceTempView("df_sku_active")
display(df_sku_active)

# COMMAND ----------

# # Load Master data active SKU
# address = "'Shopee'!A1"
# df_sku_active = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "false").option("treatEmptyValuesAsNulls", "false").option("dataAddress",address).load("dbfs:/mnt/adls/staging/ecom/master_data/SKU inventory Active.xlsx")

# display(df_sku_active)

# df_sku_active.createOrReplaceTempView("df_sku_active")

# COMMAND ----------

# %sql
# SELECT count(distinct SUBSTRING_INDEX(product_sku, "_", 1))
# FROM df_sku_active

# COMMAND ----------

# Create list of dates
start_date = '2021-01-01'  # Input start/min date
end_date = df_shopee_product_performmance_ytd.select(max(col("Date"))).collect()[0][0]   # Input end/max date

list_date = list(OrderedDict(
  ((datetime.datetime.strptime(start_date, "%Y-%m-%d") + timedelta(_)).strftime("%Y-%m-%d"), None) for _ in range((datetime.datetime.strptime(end_date, "%Y-%m-%d") - datetime.datetime.strptime(start_date, "%Y-%m-%d")).days+1)).keys())
list_date

# COMMAND ----------

# Create master calendar table
num = np.array(list_date)
df_date_table = pd.DataFrame(num.reshape(len(num),1), columns = ['Date'])
spark.conf.set("spark.sql.execution.arrow.enabled","true")

df_date_table = spark.createDataFrame(df_date_table)
df_date_table.createOrReplaceTempView("df_date_table")
display(df_date_table)

# COMMAND ----------

query_sku_master = """
WITH sku_active_master AS (
  SELECT DISTINCT 
    product_id, parent_sku, Variation, SKU
  FROM df_shopee_product_performmance_ytd 
  WHERE product_id IN (
    SELECT SUBSTRING_INDEX(product_sku, "_", 1) AS product_id
    FROM df_sku_active
    WHERE product_sku IS NOT NULL
    )
  AND Date >= '2021-01-01'
  )
SELECT DISTINCT 
  Date, product_id, parent_sku, Variation, SKU
FROM df_date_table t1
FULL JOIN sku_active_master t2
ON 1=1
--- WHERE t2.product_id = 3832932828 ----- Test
ORDER BY Date desc, product_id
"""

df_sku_master = spark.sql(query_sku_master)
df_sku_master.createOrReplaceTempView("df_sku_master")
display(df_sku_master)

# COMMAND ----------

# %sql
# SELECT count(distinct product_id)
# FROM df_sku_master

# COMMAND ----------

query_stock_master = """
SELECT DISTINCT 
  m.Date, m.product_id, m.parent_sku, m.Variation, m.SKU, 
  current_stock, 
  Name,
  URL,
  Region,
  shopid,
  shop_name,
  IF (t2.Brand IS NULL, 
    CASE WHEN LOCATE('GIFT', UPPER(Name)) > 0 OR LOCATE('GIIFT', UPPER(Name))> 0 OR LOCATE('HÀNG TẶNG', UPPER(Name)) > 0 THEN 'Gift'
      WHEN (LOCATE('CIF',UPPER(Name)) > 0) THEN 'CIF' 
      WHEN (LOCATE('KNORR',UPPER(Name)) > 0) THEN 'KNORR' 
      WHEN (LOCATE('VIM',UPPER(Name)) > 0) THEN 'VIM' 
      WHEN (LOCATE('LOVE BEAUTY',UPPER(Name)) > 0) THEN 'LOVE BEAUTY AND PLANET' 
      WHEN (LOCATE('LIPTON',UPPER(Name)) > 0) THEN 'LIPTON' 
      WHEN (LOCATE('P/S',UPPER(Name)) > 0) THEN 'P/S' 
      WHEN (LOCATE("PONDS",UPPER(Name)) > 0) OR (LOCATE("POND'S",UPPER(Name)) > 0) OR (LOCATE("POND’S",UPPER(Name)) > 0)  THEN "POND'S" 
      WHEN (LOCATE('HAZELINE',UPPER(Name)) > 0) THEN 'HAZELINE' 
      WHEN (LOCATE('VASELINE',UPPER(Name)) > 0) THEN 'VASELINE' 
      WHEN (LOCATE('DOVE',UPPER(Name)) > 0) THEN 'DOVE' 
      WHEN (LOCATE('TRESEMME',UPPER(Name)) > 0) OR (LOCATE('TRESEMMÉ',UPPER(Name)) > 0) THEN 'TRESEMME' 
      WHEN (LOCATE('CLOSEUP',UPPER(Name)) > 0) OR (LOCATE('CLOSE UP',UPPER(Name)) > 0) THEN 'CLOSEUP' 
      WHEN (LOCATE('Gift - Not match',UPPER(Name)) > 0) THEN 'Gift - Not match' 
      WHEN (LOCATE('SUNLIGHT',UPPER(Name)) > 0) AND (LOCATE('RỬA CHÉN',UPPER(Name)) > 0) THEN 'SUNLIGHT DW' 
      WHEN (LOCATE('SUNLIGHT',UPPER(Name)) > 0) AND (LOCATE('LAU SÀN',UPPER(Name)) > 0) THEN 'SUNLIGHT FLC' 
      WHEN (LOCATE('OMO',UPPER(Name)) > 0) THEN 'OMO' 
      WHEN (LOCATE('COMFORT',UPPER(Name)) > 0) THEN 'COMFORT' 
      WHEN (LOCATE('SURF',UPPER(Name)) > 0) THEN 'SURF' 
      WHEN (LOCATE('SEVENTH GENERATION',UPPER(Name)) > 0) THEN 'SEVENTH GENERATION' 
      WHEN (LOCATE('DOVE DERMASERIES',UPPER(Name)) > 0) THEN 'DOVE DERMASERIES' 
      WHEN (LOCATE('REXONA',UPPER(Name)) > 0) THEN 'REXONA' 
      WHEN (LOCATE('AXE',UPPER(Name)) > 0) THEN 'AXE' 
      WHEN (LOCATE('LIFEBUOY',UPPER(Name)) > 0) THEN 'LIFEBUOY' 
      WHEN (LOCATE('SUNSILK',UPPER(Name)) > 0) THEN 'SUNSILK' 
      WHEN (LOCATE('MICHIRU',UPPER(Name)) > 0) THEN 'MICHIRU' 
      WHEN (LOCATE('BABY DOVE',UPPER(Name)) > 0) THEN 'BABY DOVE' 
      WHEN (LOCATE('LUX',UPPER(Name)) > 0) THEN 'LUX'
      WHEN (LOCATE('SIMPLE',UPPER(Name)) > 0) THEN 'SIMPLE'
      WHEN (LOCATE('ST.IVES',UPPER(Name)) > 0) OR (LOCATE('ST. IVES',UPPER(Name)) > 0) THEN 'ST.IVES'
      WHEN (LOCATE('CLEAR MEN',UPPER(Name)) > 0) THEN 'CLEAR MEN' 
      WHEN (LOCATE('CLEAR',UPPER(Name)) > 0) THEN 'CLEAR' 
    END, 
    t2.Brand) AS Brand,
  CASE WHEN LOCATE('GIFT', UPPER(Name)) > 0 OR LOCATE('GIIFT', UPPER(Name))> 0 OR LOCATE('HÀNG TẶNG', UPPER(Name)) > 0 THEN 'Gift'
    ELSE t2.Big_C
  END AS Big_C, 
  CASE WHEN LOCATE('GIFT', UPPER(Name)) > 0 OR LOCATE('GIIFT', UPPER(Name))> 0 OR LOCATE('HÀNG TẶNG', UPPER(Name)) > 0 THEN 'Gift'
    ELSE t2.Small_C
  END AS Small_C 
FROM df_sku_master m
LEFT JOIN df_shopee_product_performmance_ytd t1
  ON m.Date = t1.Date
  AND m.product_id = t1.product_id
  AND m.parent_sku = t1.parent_sku
  AND m.Variation = t1.Variation
  AND m.SKU = t1.SKU
--- WHERE m.product_id = 3832932828 ------- Test
LEFT JOIN 
  df_shopee_master_tree t2
ON m.product_id = t2.product_id
ORDER BY product_id, Date DESC, Variation, SKU
"""
df_stock_master = spark.sql(query_stock_master)
df_stock_master.createOrReplaceTempView("df_stock_master")
display(df_stock_master)

# COMMAND ----------

# Define forward window ( max 60 days)
f_window = Window.partitionBy('product_id', 'parent_sku', 'Variation', 'SKU' )\
               .orderBy('Date')\
               .rowsBetween(-60, 0)

# define the forward-filled column
ffilled_stock = last(df_stock_master['current_stock'], ignorenulls=True).over(f_window)
ffilled_product_id = last(df_stock_master['product_id'], ignorenulls=True).over(f_window)
ffilled_Name = last(df_stock_master['Name'], ignorenulls=True).over(f_window)
ffilled_parent_sku = last(df_stock_master['parent_sku'], ignorenulls=True).over(f_window)
ffilled_Variation = last(df_stock_master['Variation'], ignorenulls=True).over(f_window)
ffilled_SKU = last(df_stock_master['SKU'], ignorenulls=True).over(f_window)
ffilled_Brand = last(df_stock_master['brand'], ignorenulls=True).over(f_window)
ffilled_Big_C = last(df_stock_master['Big_C'], ignorenulls=True).over(f_window)
ffilled_Small_C = last(df_stock_master['Small_C'], ignorenulls=True).over(f_window)

# do the fill
df_stock_master_imputed = df_stock_master.withColumn('current_stock_filled', ffilled_stock)\
                                          .withColumn('product_id_filled', ffilled_product_id)\
                                          .withColumn('Name_filled', ffilled_Name)\
                                          .withColumn('parent_sku_filled', ffilled_parent_sku)\
                                          .withColumn('Variation_filled', ffilled_Variation)\
                                          .withColumn('SKU_filled', ffilled_SKU)\
                                          .withColumn('Brand_filled', ffilled_Brand)\
                                          .withColumn('Big_C_filled', ffilled_Big_C)\
                                          .withColumn('Small_C_filled', ffilled_Small_C)\


# show off our glorious achievements
df_stock_master_imputed.createOrReplaceTempView("df_stock_master_imputed")

display(df_stock_master_imputed.orderBy( col('product_id').asc(), col('Date').desc() ) )

# COMMAND ----------

query_final_stock_report = """
WITH data AS (
  SELECT DISTINCT 
    Date, 
    CAST(product_id AS STRING) as product_id , 
    CAST(product_id_filled AS STRING) as product_id_filled, 
    Brand_filled AS Brand, 
    Big_C_filled AS Big_C, Small_C_filled AS Small_C, parent_sku, parent_sku_filled, Variation, Variation_filled, SKU, SKU_filled, Name, Name_filled, REPLACE(current_stock, "-", 0) AS current_stock,  REPLACE(current_stock_filled, "-", 0) AS current_stock_filled 
  FROM df_stock_master_imputed
--   WHERE product_id = 3832932828  --- TEST
  ORDER BY Date Desc, product_id_filled, Variation_filled, parent_sku_filled, SKU_filled
  )
SELECT DISTINCT *
FROM data
WHERE current_stock_filled IS NOT NULL
ORDER BY Date DESC
"""
df_final_stock_report = spark.sql(query_final_stock_report)
display(df_final_stock_report)
df_final_stock_report.createOrReplaceTempView("df_final_stock_report")


# COMMAND ----------

# %sql
# SELECT DISTINCT 
#   COUNT(DISTINCT product_id_filled)
# FROM df_final_stock_report
# -- WHERE Date NOT LIKE '202%'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write product Inventory report 

# COMMAND ----------

df_final_stock_report.toPandas().to_parquet("/dbfs/mnt/adls/staging/ecom/YTD Data/shopee/df_shopee_inventory_ytd.snappy", compression = "snappy")


# COMMAND ----------

### Write data stock latest 
spark.sql("SELECT * FROM df_final_stock_report WHERE Date = (SELECT MAX(Date) FROM df_final_stock_report)").toPandas().to_csv("/dbfs/mnt/adls/staging/ecom/YTD Data/shopee/df_shopee_inventory_latest.csv", line_terminator='\n', index=False, encoding="utf-8-sig")


# COMMAND ----------

# """ TESTING """
# df_test = spark.read.format("csv").option("header", "True").option("inferSchema", "True").option("multiLine", "True").load("dbfs:/mnt/adls/staging/ecom/YTD Data/shopee/df_shopee_inventory_ytd.csv")

# # df_test = pd.read_csv('/dbfs/mnt/adls/staging/ecom/YTD Data/shopee/df_shopee_inventory_ytd.csv', encoding = 'utf-8')

# df_test.createOrReplaceTempView("df_test")
# display(df_test)

# COMMAND ----------

# %sql
# SELECT DISTINCT Date
# FROM df_test
# WHERE Date NOT LIKE '202%'

# COMMAND ----------

# MAGIC %md 
# MAGIC #### ----

# COMMAND ----------

# Filter date needed for Daily report
query_shopee_product_date_seperate_filter = """
SELECT DISTINCT 
  Date, 
  Name as platform_name, 
  t1.product_id, t2.Brand, t2.Big_C, t2.Small_C,
  SUM(gross_unique_buyers) as gross_unique_buyers, 
  SUM(gross_orders) as gross_orders, 
  SUM(gross_units_sold) as gross_units_sold, 
  SUM(gmv) as gmv, 
  SUM(net_unique_buyers) as net_unique_buyers, 
  SUM(net_orders) as net_orders,
  SUM(net_units_sold) as net_units_sold, 
  SUM(nmv) as nmv
FROM df_shopee_product_performmance_date_separate t1
LEFT JOIN df_shopee_master_tree t2
ON t1.product_id = t2.product_id
WHERE Date >= (SELECT last30days_begin FROM df_date_period)
GROUP BY 1,2,3,4,5,6
"""

df_shopee_product_date_seperate_filter = spark.sql(query_shopee_product_date_seperate_filter)
df_shopee_product_date_seperate_filter.createOrReplaceTempView("df_shopee_product_date_seperate_filter")
display(df_shopee_product_date_seperate_filter)

# COMMAND ----------

# # Loading data traffic/sales performance of separate dates
# df_shopee_sales_performmance_date_separate = spark.read.format("csv").option("header","True").option("inferSchema", "True").load("dbfs:/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_SalesPerformance/")



# COMMAND ----------

query_shopee_daily_performance_separate = """
SELECT 
  p.Date,
  'shopee' as platform,
  p.platform_name, 
  p.product_id, Brand, Big_C, Small_C,
  s.`Product Visitors` as gross_unique_visitors, 
  s.`Product Views` as gross_product_views, 
  p.gross_unique_buyers,
  p.gross_orders,
  p.gross_units_sold,
  s.`Gross Item Conversion Rate` as gross_item_cr,
  p.gmv,
  p.net_unique_buyers, 
  p.net_orders, 
  p.net_units_sold, 
  p.nmv
FROM 
  df_shopee_product_date_seperate_filter p
JOIN 
  df_shopee_sales_performance_date_separate s
ON p.Date = s.Date
AND p.product_id = s.`Product ID`
"""

df_shopee_daily_performance_date_separate = spark.sql(query_shopee_daily_performance_separate)
 
display(df_shopee_daily_performance_date_separate)
df_shopee_daily_performance_date_separate.createOrReplaceTempView("df_shopee_daily_performance_date_separate")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Daily period grouped data

# COMMAND ----------

# Load Shopee Product period group Data
df_shopee_product_performance_daily = spark.read.format("parquet").load("dbfs:/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_DailyOfftake/product_performance/")

display(df_shopee_product_performance_daily)

df_shopee_product_performance_daily.createOrReplaceTempView("df_shopee_product_performance_daily")

# COMMAND ----------

# Load Shopee Traffic Period group Data 
df_shopee_sales_performance_daily = spark.read.format("csv").option("header","True").option("inferSchema", "True").load("dbfs:/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_DailyOfftake/sales_performance/")

display(df_shopee_sales_performance_daily)

df_shopee_sales_performance_daily.createOrReplaceTempView("df_shopee_sales_performance_daily")


# COMMAND ----------

# Mapping product - category
query_shopee_product_group = """
SELECT 
  Date, Name as  platform_name, t1.product_id, shop_name, t2.Brand, Big_C, Small_C,
  SUM(gross_unique_buyers) as gross_unique_buyers, 
  SUM(gross_orders) as gross_orders, 
  SUM(gross_units_sold) as gross_units_sold, 
  SUM(gmv) as gmv, 
  SUM(net_unique_buyers) as net_unique_buyers, 
  SUM(net_orders) as net_orders,
  SUM(net_units_sold) as net_units_sold, 
  SUM(nmv) as nmv
FROM
  df_shopee_product_performance_daily t1
LEFT JOIN 
  df_shopee_master_tree t2
ON t1.product_id = t2.product_id
GROUP BY 1,2,3,4,5,6,7
"""

df_shopee_product_grouped = spark.sql(query_shopee_product_group)

display(df_shopee_product_grouped)
df_shopee_product_grouped.createOrReplaceTempView("df_shopee_product_grouped")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Combine traffic & product performance

# COMMAND ----------

query_shopee_traffic_combined = """
SELECT 
  p.Date as Period,
  'shopee' as platform,
  p.platform_name, 
  p.product_id, 
  p.shop_name, 
  Brand, 
  Big_C, 
  Small_C,
  
  s.`Product Visitors` as gross_unique_visitors,
  s.`Product Views` as gross_product_views, 
  p.gross_unique_buyers,
  p.gross_orders,
  p.gross_units_sold,
  s.`Gross Item Conversion Rate` as gross_item_cr,
  p.gmv,
  p.net_unique_buyers,
  p.net_orders,
  p.net_units_sold, 
  p.nmv
FROM df_shopee_product_grouped p
JOIN df_shopee_sales_performance_daily s
ON p.Date = s.Date
AND p.product_id = s.`Product ID`
"""
 
df_shopee_traffic_combined = spark.sql(query_shopee_traffic_combined)
 
display(df_shopee_traffic_combined)
df_shopee_traffic_combined.createOrReplaceTempView("df_shopee_traffic_combined")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing data 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data daily seperate report

# COMMAND ----------

# Writing Shopee daily seperate  data (in CSV format)
df_shopee_daily_performance_date_separate.toPandas().to_csv("/dbfs/mnt/adls/staging/ecom/Daily Performance/df_shopee_daily_performance_date_separate.csv", index=False, encoding="utf-8-sig")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ecom_db.df_shopee_daily_performmance_date_separate;
# MAGIC
# MAGIC DROP TABLE IF EXISTS ecom_db.df_shopee_daily_performance_date_separate;
# MAGIC
# MAGIC CREATE TABLE ecom_db.df_shopee_daily_performance_date_separate
# MAGIC USING csv
# MAGIC OPTIONS (path "dbfs:/mnt/adls/staging/ecom/Daily Performance/df_shopee_daily_performance_date_separate.csv" , header "true", delimiter =",")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data traffic combined 

# COMMAND ----------

# Writing Shopee traffic combined data (in CSV format)
df_shopee_traffic_combined.toPandas().to_csv("/dbfs/mnt/adls/staging/ecom/Daily Performance/df_shopee_traffic_combined.csv", index=False, encoding="utf-8-sig")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ecom_db.df_shopee_traffic_combined;
# MAGIC
# MAGIC CREATE TABLE ecom_db.df_shopee_traffic_combined
# MAGIC USING csv
# MAGIC OPTIONS (path "dbfs:/mnt/adls/staging/ecom/Daily Performance/df_shopee_traffic_combined.csv" , header "true", delimiter =",")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA VALIDATION

# COMMAND ----------

# %sql
# WITH data_product AS (
#   SELECT Date, 
#     SUM(GMV) as GMV_product
#   FROM df_shopee_sales_traffic_ytd
#   WHERE Date LIKE '2021-11%'
#   GROUP BY 1
#   ORDER BY 1 
#   ), 
#   data_sales AS (
#   SELECT Date, 
#     SUM(`Gross Sales(₫)`) as GMV_sales
#   FROM df_shopee_sales_performance_date_separate
#   WHERE Date LIKE '2021-11%'
#   GROUP BY 1
#   ORDER BY 1 
# )
# SELECT t1.Date, 
#   GMV_product, 
#   GMV_sales
# FROM data_product t1
# LEFT JOIN data_sales t2
# ON t1.Date = t2.Date
# ORDER BY Date
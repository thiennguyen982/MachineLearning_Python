# Databricks notebook source
import re
import datetime as dt
import datetime
import pandas as pd
from pyspark.sql.functions import col


# COMMAND ----------

# MAGIC %md
# MAGIC ### Data checking update

# COMMAND ----------

# """ Lazada Check """
# # BPC
# FileInfo =  dbutils.fs.ls("dbfs:/mnt/adls/staging/ecom/BOT/Lazada/SellerCenter_ProductPerformance/bpc/")
# file_paths = [FileInfo[i][0] for i in range(0,len(FileInfo))]
# file_names = [FileInfo[i][1] for i in range(0,len(FileInfo)) if re.match(r'lazada_bpc_dailyofftake_20.*\.csv', FileInfo[i][1])]
# lastest_file = max(file_names)
# lzd_bpc_max_date = re.search("lazada_bpc_dailyofftake_(.*).csv", lastest_file).group(1).replace("_", "-")
# yesterday = dt.datetime.now() + dt.timedelta(hours=7) + dt.timedelta(days=-1)
# if dt.datetime.strftime(yesterday, "%Y-%m-%d") == lzd_bpc_max_date:
#   True
#   print("Lazada BPC data updated")
# else:
#   print("Lazada BPC data has NOT updated till yesterday!")
#   pass

# # HCF
# FileInfo =  dbutils.fs.ls("dbfs:/mnt/adls/staging/ecom/BOT/Lazada/SellerCenter_ProductPerformance/hcf/")
# file_paths = [FileInfo[i][0] for i in range(0,len(FileInfo))]
# file_names = [FileInfo[i][1] for i in range(0,len(FileInfo)) if re.match(r'lazada_hcf_dailyofftake_20.*\.csv', FileInfo[i][1])]
# lastest_file = max(file_names)
# lzd_hcf_max_date = re.search("lazada_hcf_dailyofftake_(.*).csv", lastest_file).group(1).replace("_", "-")
# yesterday = dt.datetime.now() + dt.timedelta(hours=7) + dt.timedelta(days=-1)
# if dt.datetime.strftime(yesterday, "%Y-%m-%d") == lzd_hcf_max_date:
#   True
#   print("Lazada HCF data updated")
# else:
#   print("Lazada HCF data has NOT updated till yesterday!")
#   pass

# # Michiru
# FileInfo =  dbutils.fs.ls("dbfs:/mnt/adls/staging/ecom/BOT/Lazada/SellerCenter_ProductPerformance/michiru/")
# file_paths = [FileInfo[i][0] for i in range(0,len(FileInfo))]
# file_names = [FileInfo[i][1] for i in range(0,len(FileInfo)) if re.match(r'lazada_michiru_dailyofftake_20.*\.csv', FileInfo[i][1])]
# lastest_file = max(file_names)
# lzd_michiru_max_date = re.search("lazada_michiru_dailyofftake_(.*).csv", lastest_file).group(1).replace("_", "-")
# yesterday = dt.datetime.now() + dt.timedelta(hours=7) + dt.timedelta(days=-1)
# if dt.datetime.strftime(yesterday, "%Y-%m-%d") == lzd_michiru_max_date:
#   True
#   print("Lazada Michiru data updated")
# else:
#   print("Lazada Michiru data has NOT updated till yesterday!")
#   pass


# if not (lzd_bpc_max_date == lzd_hcf_max_date == lzd_michiru_max_date ==  dt.datetime.strftime(yesterday, "%Y-%m-%d")):
#   raise Exception("\nJob exit because data is not updated!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Master mapping

# COMMAND ----------

master_latest_version = spark.sql("SELECT max(version) FROM (DESCRIBE HISTORY delta.`dbfs:/mnt/adls/staging/ecom/master_customer/df_master_customer_delta/`)").collect()[0][0]

df_master_customer = spark.read.format("delta").option("versionAsOf", master_latest_version).load("dbfs:/mnt/adls/staging/ecom/master_customer/df_master_customer_delta").filter(col('platform') == 'lazada')

display(df_master_customer)
df_master_customer.createOrReplaceTempView("df_master_customer")

# COMMAND ----------

query_master_mapping = """
WITH data_single  AS (
  SELECT DISTINCT 
    platform,
    CONCAT(itemid, "_", modelid) as product_id,  
    combo_gift_single,
  --  prop, 
  --  IF(DP_name LIKE '%Not match', 0, 1) as is_match,
    Brand,  Small_C, Big_C
  FROM df_master_customer
  WHERE 
  platform = 'lazada'
  AND combo_gift_single != "Combo"
  -- AND  IF(DP_name LIKE '%Not match', 0, 1) = 1 -- filter out not-matched single SKU 
  ),

  data_combo AS (
  SELECT DISTINCT 
    platform, 
    CONCAT(itemid, "_", modelid) as product_id,  
    combo_gift_single,
  --  prop, 
  --  IF(DP_name LIKE '%Not match', 0, 1) as is_match,
    Brand,  Small_C, Big_C
  FROM df_master_customer
  WHERE 
  platform = 'lazada'
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
# MAGIC ### Lazada Data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create lazada master tree mapping

# COMMAND ----------

query_lazada_master_tree = """
WITH data AS (
SELECT DISTINCT 
    CONCAT(itemid, "_", modelid) as product_id, 
    platform_name, 
    combo_gift_single, 
    Brand, 
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
    platform = "lazada"
  ),
  data_clean AS (
  SELECT 
    product_id, platform_name, combo_gift_single, Big_C, 
    IF(Brand LIKE '%Not match', brand_detect, COALESCE(Brand, brand_detect)) as Brand, 
    IF(Small_C LIKE '%Not match', brand_detect, COALESCE(Small_C, Small_C_detect)) as Small_C
  FROM data
  WHERE rank = 1
  ),
  data_master AS (
SELECT DISTINCT  
  product_id,  platform_name, Brand, Big_C, Small_C,
  CASE WHEN (Big_C LIKE '%Not macth' OR Big_C IS NULL) AND Small_C = 'HNH' THEN 'Homecare' 
    WHEN (Big_C LIKE '%Not macth' OR Big_C IS NULL) AND Small_C = 'Combo - Not match' THEN 'Combo - Not match' 
    WHEN (Big_C LIKE '%Not macth' OR Big_C IS NULL) AND Small_C = 'Food' THEN 'F&RFs' 
    WHEN (Big_C LIKE '%Not macth' OR Big_C IS NULL) AND Small_C = 'Hair' THEN 'Personal Care' 
    WHEN (Big_C LIKE '%Not macth' OR Big_C IS NULL) AND Small_C = 'SCL' THEN 'Personal Care' 
    WHEN (Big_C LIKE '%Not macth' OR Big_C IS NULL) AND Small_C = 'Skincare' THEN 'Personal Care' 
    WHEN (Big_C LIKE '%Not macth' OR Big_C IS NULL) AND Small_C = 'Single - Not match' THEN 'Single - Not match' 
    WHEN (Big_C LIKE '%Not macth' OR Big_C IS NULL) AND Small_C = 'Tea' THEN 'F&RFs' 
    WHEN (Big_C LIKE '%Not macth' OR Big_C IS NULL) AND Small_C = 'Oral' THEN 'Personal Care' 
    WHEN (Big_C LIKE '%Not macth' OR Big_C IS NULL) AND Small_C = 'Gift - Not match' THEN 'Gift - Not match' 
    WHEN (Big_C LIKE '%Not macth' OR Big_C IS NULL) AND Small_C = 'Fabclean' THEN 'Homecare' 
    WHEN (Big_C LIKE '%Not macth' OR Big_C IS NULL) AND Small_C = 'Fabenc' THEN 'Homecare' 
    WHEN (Big_C LIKE '%Not macth' OR Big_C IS NULL) AND Small_C = 'Deo' THEN 'Personal Care' 
    WHEN (Big_C LIKE '%Not macth' OR Big_C IS NULL) AND Small_C = 'Combo' THEN 'Combo'
    WHEN (Big_C LIKE '%Not macth' OR Big_C IS NULL) AND Small_C = 'Gift' THEN 'Gift'
  END AS Big_C_detect
FROM data_clean
)
SELECT DISTINCT 
  product_id, 
  Brand, 
  Small_C, 
  COALESCE(Big_C, Big_C_detect) AS Big_C
FROM data_master
ORDER BY 1
"""

df_lazada_master_tree = spark.sql(query_lazada_master_tree)
display(df_lazada_master_tree)
df_lazada_master_tree.createOrReplaceTempView("df_lazada_master_tree")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Daily separate date data (Combine BPC + HCF + Michiru)

# COMMAND ----------

# df_lazada_order_bpc = spark.read.options(header='True', inferSchema='True').csv("dbfs:/mnt/adls/staging/ecom/sellout/lazada/lazada_order_item/bpc/")
# df_lazada_order_hcf = spark.read.options(header='True', inferSchema='True').csv("dbfs:/mnt/adls/staging/ecom/sellout/lazada/lazada_order_item/hcf/")
# df_lazada_order_mic = spark.read.options(header='True', inferSchema='True').csv("dbfs:/mnt/adls/staging/ecom/sellout/lazada/lazada_order_item/mic/")


# df_lazada_order_bpc.createOrReplaceTempView("df_lazada_order_bpc")
# df_lazada_order_hcf.createOrReplaceTempView("df_lazada_order_hcf")
# df_lazada_order_mic.createOrReplaceTempView("df_lazada_order_mic")


# query_filter_lzd_order = """
# (SELECT *, 'Unilever - BPC' as shop_name
# FROM df_lazada_order_bpc
# WHERE date(created_at) >= (SELECT DATE(current_timestamp + interval '7' hours - interval '60' days)))

# UNION ALL
# (SELECT *, 'Unilever - HCF' as shop_name
# FROM df_lazada_order_hcf
# WHERE date(created_at) >= (SELECT DATE(current_timestamp + interval '7' hours - interval '60' days)))

# UNION ALL
# (SELECT *, 'Unilever - Michiru' as shop_name
# FROM df_lazada_order_mic
# WHERE date(created_at) >= (SELECT DATE(current_timestamp + interval '7' hours - interval '60' days)))
# """

# df_lazada_order_filter = spark.sql(query_filter_lzd_order)
# df_lazada_order_filter.createOrReplaceTempView("df_lazada_order_filter")
# display(df_lazada_order_filter)
from pyspark.sql.types import StructType
import pandas as pd
from pyspark.sql.functions import lit, to_date

df_lazada_order = None
shop_list = ['BPC', 'HCF', 'Michiru']
order_cols_list = ['createTime', 'lazadaSku', 'sellerSku', 'itemName', 'paidPrice', 'status', 'orderNumber', 'orderItemId', 'shop_name']
for shop in shop_list:
  shop_short_name = shop[:3].lower()
  print(shop_short_name)
  try:
    df = spark.read.options(header='True', inferSchema='True').option('delimiter', ';').csv(f'dbfs:/mnt/adls/staging/ecom/sellout/lazada/lazada_order_item_manual/{shop_short_name}/')
    df = df.withColumn("shop_name", lit(f'Unilever - {shop}')).withColumn('createTime', to_date(col('createTime'), 'dd MMM yyyy HH:mm'))
    if df_lazada_order == None:
      df_lazada_order = df.select(order_cols_list)
    else:
      df_lazada_order = df_lazada_order.union(df.select(order_cols_list))
  except Exception as e:
    print(f'Could not read order at shop {shop}')
    print(e)
df_lazada_order.createOrReplaceTempView("df_lazada_order")

# COMMAND ----------

# MAGIC %sql select date(createTime) as created_at, shop_name, lazadaSku as sku, sellerSku as shop_sku, itemName as product_name, paidPrice as paid_price, status, orderNumber as order_id, orderItemId as order_item_id
# MAGIC from df_lazada_order
# MAGIC where date(createTime) >= (SELECT DATE(current_timestamp + interval '7' hours - interval '60' days))

# COMMAND ----------

_sqldf.createOrReplaceTempView("df_lazada_order")

# COMMAND ----------

# MAGIC %sql select * from df_lazada_order
# MAGIC where date(created_at) >= (SELECT DATE(current_timestamp + interval '7' hours - interval '60' days))

# COMMAND ----------

_sqldf.createOrReplaceTempView("df_lazada_order_filter")

# COMMAND ----------

list_cols = ['product_name', 'seller_sku', 'sku_id', 'URL', 'sku_visitors', 'sku_views', 'visitor_value', 'a2c_visitors', 'a2c_units', 'a2c_rate', 'Wishlist_Visitors', 'Wishlists', 'buyers', 'orders', 'units_Sold', 'GMV', 'CR', 'GMV_per_buyer', 'Date']
df_lazada_daily_traffic_bpc_seperate = spark.read.options(header='True', inferSchema='True').csv("dbfs:/mnt/adls/staging/ecom/BOT/Lazada/SellerCenter_ProductPerformance/bpc/").select(list_cols)
df_lazada_daily_traffic_hcf_seperate = spark.read.options(header='True', inferSchema='True').csv("dbfs:/mnt/adls/staging/ecom/BOT/Lazada/SellerCenter_ProductPerformance/hcf/").select(list_cols)
df_lazada_daily_traffic_mic_seperate = spark.read.options(header='True', inferSchema='True').csv("dbfs:/mnt/adls/staging/ecom/BOT/Lazada/SellerCenter_ProductPerformance/michiru/").select(list_cols)

df_lazada_daily_traffic_bpc_seperate.createOrReplaceTempView("df_lazada_daily_traffic_bpc_seperate")
df_lazada_daily_traffic_hcf_seperate.createOrReplaceTempView("df_lazada_daily_traffic_hcf_seperate")
df_lazada_daily_traffic_mic_seperate.createOrReplaceTempView("df_lazada_daily_traffic_mic_seperate")

# COMMAND ----------

from pyspark.sql.functions import split, first, udf
from pyspark.sql.types import StringType

from pyspark.sql.functions import UserDefinedFunction
replace_null = udf(lambda x: None if x=='-' else x, StringType())

df_lazada_daily_traffic_seperate = spark.sql("""
SELECT * FROM 
  (SELECT *, 'Unilever - HCF' as shop_name
  FROM df_lazada_daily_traffic_hcf_seperate 
  
  UNION ALL 
  SELECT *, 'Unilever - BPC' as shop_name
  FROM df_lazada_daily_traffic_bpc_seperate
  
  UNION ALL 
  SELECT *, 'Unilever - Michiru' as shop_name
  FROM df_lazada_daily_traffic_mic_seperate
  ) 
WHERE Date >= (SELECT previous_last30days_begin FROM df_date_period) """)

def __group_by_url(df):
  split_col = split(df['URL'], '\?')
  df = df.withColumn("URL", split_col.getItem(0))
  df = df.groupBy(df['URL']).agg(
    first(df['product_name'], ignorenulls=True).alias('product_name'),
    first(df['seller_sku'], ignorenulls=True).alias('seller_sku'),
    first(df['sku_id'], ignorenulls=True).alias('sku_id'),
    first(df['sku_visitors'], ignorenulls=True).alias('sku_visitors'),
    first(df['sku_views'], ignorenulls=True).alias('sku_views'),
    first(df['visitor_value'], ignorenulls=True).alias('visitor_value'),
    first(df['a2c_visitors'], ignorenulls=True).alias('a2c_visitors'),
    first(df['a2c_units'], ignorenulls=True).alias('a2c_units'),
    first(df['a2c_rate'], ignorenulls=True).alias('a2c_rate'),
    first(df['Wishlist_Visitors'], ignorenulls=True).alias('Wishlist_Visitors'),
    first(df['Wishlists'], ignorenulls=True).alias('Wishlists'),
    first(df['buyers'], ignorenulls=True).alias('buyers'),
    first(df['orders'], ignorenulls=True).alias('orders'),
    first(df['units_Sold'], ignorenulls=True).alias('units_Sold'),
    first(df['GMV'], ignorenulls=True).alias('GMV'),
    first(df['CR'], ignorenulls=True).alias('CR'),
    first(df['GMV_per_buyer'], ignorenulls=True).alias('GMV_per_buyer'),
    first(df['Date'], ignorenulls=True).alias('Date'),
    first(df['shop_name'], ignorenulls=True).alias('shop_name')
  )
  return df

df_lazada_daily_traffic_seperate = df_lazada_daily_traffic_seperate.withColumn("seller_sku", replace_null("seller_sku")).withColumn("sku_id", replace_null("sku_id"))
df_lazada_daily_traffic_seperate = __group_by_url(df_lazada_daily_traffic_seperate)
df_lazada_daily_traffic_seperate.createOrReplaceTempView("df_lazada_daily_traffic_seperate")
# display(df_lazada_daily_traffic_seperate)

# COMMAND ----------

query_lazada_daily_performance_seperate = """
WITH data_order AS (
  SELECT DISTINCT 
      Date(created_at) as Date, shop_name,
      'lazada' as platform, 
      CONCAT(IFNULL(sku, 0), "_", IFNULL(shop_sku,0)) as product_id, 
      product_name as platform_name, 
      SUM(paid_price) as GMV,
      SUM(CASE WHEN status = 'delivered' THEN paid_price END) as NMV,
      COUNT(DISTINCT order_id) as gross_orders,
      COUNT(DISTINCT CASE WHEN status = 'delivered' THEN order_id END) as net_orders,
      COUNT(order_item_id) as gross_items,
      COUNT(CASE WHEN status = 'delivered' THEN order_item_id END) as net_items
  FROM df_lazada_order_filter
  GROUP BY 1,2,3,4,5
  ),
  data_traffic AS (
  SELECT DISTINCT 
    Date, 
    'lazada' as platform, shop_name,
    CONCAT(IFNULL(seller_sku, 0), "_", IFNULL(sku_id,0)) as product_id,
    product_name as platform_name, 

    SUM(sku_views) as gross_product_views, 
    SUM(sku_visitors) as gross_unique_visitors,

    SUM(buyers) as gross_unique_buyers,
    SUM(GMV) as gmv,
    SUM(orders) as gross_orders, 
    SUM(units_Sold) as gross_units_sold
  FROM df_lazada_daily_traffic_seperate
  GROUP BY 1,2,3,4,5
  )
SELECT 
  t1.*, t2.gross_product_views, t2.gross_unique_visitors, t2.gross_unique_buyers, 
  Brand, Big_C, Small_C
FROM 
  data_order  t1
LEFT JOIN 
  data_traffic t2
ON t1.Date = t2.Date
AND t1.product_id = t2.product_id
LEFT JOIN 
  df_lazada_master_tree t3
ON t1.product_id = t3.product_id
"""

df_lazada_daily_performance_date_separate = spark.sql(query_lazada_daily_performance_seperate)

display(df_lazada_daily_performance_date_separate)
df_lazada_daily_performance_date_separate.createOrReplaceTempView("df_lazada_daily_performance_date_separate")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write YTD Lazada product performance
# MAGIC

# COMMAND ----------

# Load data order
df_lazada_daily_order_ytd = spark.sql("""
SELECT *
FROM df_lazada_order
WHERE date(created_at) >= '2020-01-01'
""")
df_lazada_daily_order_ytd.createOrReplaceTempView("df_lazada_daily_order_ytd")

# Load data traffic

df_lazada_daily_traffic_ytd = spark.sql("""
SELECT * 
FROM 
  (SELECT * , 'Unilever - HCF' as shop_name
  FROM df_lazada_daily_traffic_hcf_seperate 
  UNION ALL 
  SELECT *, 'Unilever - BPC' as shop_name
  FROM df_lazada_daily_traffic_bpc_seperate
  UNION ALL 
  SELECT *, 'Unilever - Michiru' as shop_name
  FROM df_lazada_daily_traffic_mic_seperate
  ) 
WHERE Date >= '2020-01-01' """)

df_lazada_daily_traffic_ytd.createOrReplaceTempView("df_lazada_daily_traffic_ytd")


# COMMAND ----------


# Combine offtake and traffic
query_lazada_daily_performance_ytd = """
WITH data_order AS (
  SELECT DISTINCT 
      Date(created_at) as Date,
      'lazada' as platform, 
      shop_name,
      CONCAT(IFNULL(sku, 0), "_", IFNULL(shop_sku,0)) as product_id, 
      product_name as platform_name, 
      SUM(paid_price) as GMV,
      SUM(CASE WHEN status = 'delivered' THEN paid_price END) as NMV,
      COUNT(DISTINCT order_id) as gross_orders,
      COUNT(DISTINCT CASE WHEN status = 'delivered' THEN order_id END) as net_orders,
      COUNT(order_item_id) as gross_items,
      COUNT(CASE WHEN status = 'delivered' THEN order_item_id END) as net_items
  FROM df_lazada_daily_order_ytd
  GROUP BY 1,2,3,4,5
  ),
  data_traffic AS (
  SELECT DISTINCT 
    Date, 
    'lazada' as platform,
    shop_name,
    CONCAT(IFNULL(seller_sku, 0), "_", IFNULL(sku_id,0)) as product_id,
    product_name as platform_name, 

    SUM(sku_views) as gross_product_views, 
    SUM(sku_visitors) as gross_unique_visitors,

    SUM(buyers) as gross_unique_buyers,
    SUM(GMV) as gmv,
    SUM(orders) as gross_orders, 
    SUM(units_Sold) as gross_units_sold
  FROM df_lazada_daily_traffic_ytd
  GROUP BY 1,2,3,4,5
  )
SELECT 
    t1.*, 
    t2.gross_product_views, t2.gross_unique_visitors, t2.gross_unique_buyers,
    IF(Brand IS NULL, 
  CASE WHEN LOCATE('GIFT', UPPER(t1.platform_name)) > 0 OR LOCATE('GIIFT', UPPER(t1.platform_name))> 0 OR LOCATE('HÀNG TẶNG', UPPER(t1.platform_name)) > 0 THEN 'Gift'
      WHEN (LOCATE('CIF',UPPER(t1.platform_name)) > 0) THEN 'CIF' 
      WHEN (LOCATE('KNORR',UPPER(t1.platform_name)) > 0) THEN 'KNORR' 
      WHEN (LOCATE('VIM',UPPER(t1.platform_name)) > 0) THEN 'VIM' 
      WHEN (LOCATE('LOVE BEAUTY',UPPER(t1.platform_name)) > 0) THEN 'LOVE BEAUTY AND PLANET' 
      WHEN (LOCATE('LIPTON',UPPER(t1.platform_name)) > 0) THEN 'LIPTON' 
      WHEN (LOCATE('CLEAR',UPPER(t1.platform_name)) > 0) THEN 'CLEAR' 
      WHEN (LOCATE('P/S',UPPER(t1.platform_name)) > 0) THEN 'P/S' 
      WHEN (LOCATE("PONDS",UPPER(t1.platform_name)) > 0) OR (LOCATE("POND'S",UPPER(t1.platform_name)) > 0) OR (LOCATE("POND’S",UPPER(t1.platform_name)) > 0)  THEN "POND'S" 
      WHEN (LOCATE('HAZELINE',UPPER(t1.platform_name)) > 0) THEN 'HAZELINE' 
      WHEN (LOCATE('VASELINE',UPPER(t1.platform_name)) > 0) THEN 'VASELINE' 
      WHEN (LOCATE('DOVE',UPPER(t1.platform_name)) > 0) THEN 'DOVE' 
      WHEN (LOCATE('TRESEMME',UPPER(t1.platform_name)) > 0) OR (LOCATE('TRESEMMÉ',UPPER(t1.platform_name)) > 0) THEN 'TRESEMME' 
      WHEN (LOCATE('CLOSEUP',UPPER(t1.platform_name)) > 0) OR (LOCATE('CLOSE UP',UPPER(t1.platform_name)) > 0) THEN 'CLOSEUP' 
      WHEN (LOCATE('Gift - Not match',UPPER(t1.platform_name)) > 0) THEN 'Gift - Not match' 
      WHEN (LOCATE('SUNLIGHT',UPPER(t1.platform_name)) > 0) AND (LOCATE('RỬA CHÉN',UPPER(t1.platform_name)) > 0) THEN 'SUNLIGHT DW' 
      WHEN (LOCATE('SUNLIGHT',UPPER(t1.platform_name)) > 0) AND (LOCATE('LAU SÀN',UPPER(t1.platform_name)) > 0) THEN 'SUNLIGHT FLC' 
      WHEN (LOCATE('OMO',UPPER(t1.platform_name)) > 0) THEN 'OMO' 
      WHEN (LOCATE('COMFORT',UPPER(t1.platform_name)) > 0) THEN 'COMFORT' 
      WHEN (LOCATE('SURF',UPPER(t1.platform_name)) > 0) THEN 'SURF' 
      WHEN (LOCATE('SEVENTH GENERATION',UPPER(t1.platform_name)) > 0) THEN 'SEVENTH GENERATION' 
      WHEN (LOCATE('DOVE DERMASERIES',UPPER(t1.platform_name)) > 0) THEN 'DOVE DERMASERIES' 
      WHEN (LOCATE('REXONA',UPPER(t1.platform_name)) > 0) THEN 'REXONA' 
      WHEN (LOCATE('AXE',UPPER(t1.platform_name)) > 0) THEN 'AXE' 
      WHEN (LOCATE('LIFEBUOY',UPPER(t1.platform_name)) > 0) THEN 'LIFEBUOY' 
      WHEN (LOCATE('SUNSILK',UPPER(t1.platform_name)) > 0) THEN 'SUNSILK' 
      WHEN (LOCATE('CLEAR MEN',UPPER(t1.platform_name)) > 0) THEN 'CLEAR MEN' 
      WHEN (LOCATE('MICHIRU',UPPER(t1.platform_name)) > 0) THEN 'MICHIRU' 
      WHEN (LOCATE('BABY DOVE',UPPER(t1.platform_name)) > 0) THEN 'BABY DOVE' 
      WHEN (LOCATE('LUX',UPPER(t1.platform_name)) > 0) THEN 'LUX'
    END, 
    Brand) AS Brand,
  CASE WHEN LOCATE('GIFT', UPPER(t1.platform_name)) > 0 OR LOCATE('GIIFT', UPPER(t1.platform_name))> 0 OR LOCATE('HÀNG TẶNG', UPPER(t1.platform_name)) > 0 THEN 'Gift'
       WHEN LOCATE('MICHIRU', UPPER(t1.platform_name)) > 0 THEN 'Personal Care'
    ELSE Big_C
  END AS Big_C, 
  CASE WHEN LOCATE('GIFT', UPPER(t1.platform_name)) > 0 OR LOCATE('GIIFT', UPPER(t1.platform_name))> 0 OR LOCATE('HÀNG TẶNG', UPPER(t1.platform_name)) > 0 THEN 'Gift'
       WHEN LOCATE('MICHIRU', UPPER(t1.platform_name)) > 0 THEN 'Hair'
    ELSE Small_C
  END AS Small_C
FROM 
  data_order  t1
LEFT JOIN 
  data_traffic t2
ON t1.Date = t2.Date
AND t1.product_id = t2.product_id
LEFT JOIN 
  df_lazada_master_tree t3
ON t1.product_id = t3.product_id
"""
df_lazada_daily_performmance_ytd = spark.sql(query_lazada_daily_performance_ytd)


df_lazada_daily_performmance_ytd.createOrReplaceTempView("df_lazada_daily_performmance_ytd")
display(df_lazada_daily_performmance_ytd)


# COMMAND ----------

# Write data daily performance (YTD) to csv
df_lazada_daily_performmance_ytd.toPandas().to_csv("/dbfs/mnt/adls/staging/ecom/YTD Data/lazada/lazada_product_performance_ytd.csv", index=False, encoding='utf-8-sig')

# COMMAND ----------

### Load data product profile
df_lazada_bpc_product_profile= spark.read.options(header='True', inferSchema='True', delimiter = ",").csv("dbfs:/mnt/adls/staging/ecom/sellout/lazada/product_profile/bpc/")

df_lazada_hcf_product_profile = spark.read.options(header='True', inferSchema='True', delimiter = ",").csv("dbfs:/mnt/adls/staging/ecom/sellout/lazada/product_profile/hcf/")

df_lazada_mic_product_profile = spark.read.options(header='True', inferSchema='True', delimiter = ",").csv("dbfs:/mnt/adls/staging/ecom/sellout/lazada/product_profile/mic/")
                                 
df_lazada_bpc_product_profile.createOrReplaceTempView("df_lazada_bpc_product_profile")
df_lazada_hcf_product_profile.createOrReplaceTempView("df_lazada_hcf_product_profile")
df_lazada_mic_product_profile.createOrReplaceTempView("df_lazada_mic_product_profile")

# COMMAND ----------

### Checking if column's names are different or NOT
[value for value in df_lazada_hcf_product_profile.columns if value not in df_lazada_mic_product_profile.columns]


# COMMAND ----------

df_lazada_product_profile = spark.sql("""
WITH product_map AS (
  SELECT product_id, platform_name,  Brand, Big_C, Small_C 
  FROM (
    SELECT DISTINCT 
      product_id, platform_name,  Brand, Big_C, Small_C, 
      ROW_NUMBER() OVER( PARTITION BY product_id ORDER BY Date DESC) as rank
    FROM df_lazada_daily_performmance_ytd
    )
  WHERE rank = 1
  )
  
SELECT 
  t1.*, platform_name,  Brand, Big_C, Small_C 
FROM
  (SELECT 
      date , _compatible_variation_ ,  sellableStock ,  SellerSku ,  ShopSku , occupiedStock ,  dropshippingStock ,  Url ,  package_height ,  price , package_length ,  special_from_date ,  preorderStock , special_to_date ,  Status ,  quantity ,  Images , special_time_format ,  fulfilmentStock ,  volume , multiWarehouseInventories ,  package_width ,  special_to_time , special_from_time ,  special_price ,  package_weight ,  SkuId , withholdingStock ,  package_content ,  fragrance_family , 
    'Unilever - BPC' as shop_name
  FROM df_lazada_bpc_product_profile 
  WHERE date IS NOT NULL
  
  UNION ALL 
  SELECT 
    date , _compatible_variation_ ,  sellableStock ,  SellerSku ,  ShopSku , occupiedStock ,  dropshippingStock ,  Url ,  package_height ,  price , package_length ,  special_from_date ,  preorderStock , special_to_date ,  Status ,  quantity ,  Images , special_time_format ,  fulfilmentStock ,  volume , multiWarehouseInventories ,  package_width ,  special_to_time , special_from_time ,  special_price ,  package_weight ,  SkuId , withholdingStock ,  package_content ,  fragrance_family , 
    'Unilever - HCF' as shop_name
  FROM df_lazada_hcf_product_profile
  WHERE date IS NOT NULL
  
  UNION ALL 
  SELECT 
    date , _compatible_variation_ ,  sellableStock ,  SellerSku ,  ShopSku , occupiedStock ,  dropshippingStock ,  Url ,  package_height ,  price , package_length ,  special_from_date ,  preorderStock , special_to_date ,  Status ,  quantity ,  Images , special_time_format ,  fulfilmentStock ,  volume , multiWarehouseInventories ,  package_width ,  special_to_time , special_from_time ,  special_price ,  package_weight ,  SkuId , withholdingStock ,  package_content ,  fragrance_family , 
    'Unilever - Michiru' as shop_name
  FROM df_lazada_mic_product_profile
  WHERE date IS NOT NULL
  ) as t1
LEFT JOIN 
  product_map t2
ON CONCAT(t1.SellerSku , '_', t1.ShopSku) = t2.product_id
ORDER BY date DESC
""")

df_lazada_product_profile.createOrReplaceTempView("df_lazada_product_profile")
display(df_lazada_product_profile)

# COMMAND ----------

# %sql
# SELECT DISTINCT Brand, Big_C, Small_C 
# FROM df_lazada_product_profile
# Where Brand = 'SUNLIGHT DW'


# COMMAND ----------

# Write data product profile
df_lazada_product_profile.toPandas().to_csv("/dbfs/mnt/adls/staging/ecom/Product Data/lazada_product_profile.csv", index=False, sep = ",", encoding='utf-8-sig')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Daily period grouped data

# COMMAND ----------

# LAzada OFFTAKE DAILY PERIOD
query_sellout_daily_period = """
SELECT DISTINCT 
    'yesterday' as period,
    'lazada' as platform, 
    CONCAT(IFNULL(sku, 0), "_", IFNULL(shop_sku,0)) as product_id, 
    product_name as platform_name, 
    SUM(paid_price) as GMV,
    SUM(CASE WHEN status = 'delivered' THEN paid_price END) as NMV,
    COUNT(DISTINCT order_id) as gross_orders,
    COUNT(DISTINCT CASE WHEN status = 'delivered' THEN order_id END) as net_orders,
    COUNT(order_item_id) as gross_items,
    COUNT(CASE WHEN status = 'delivered' THEN order_item_id END) as net_items
FROM df_lazada_order_filter
WHERE date(created_at) = (SELECT yesterday FROM df_date_period)
GROUP BY 1,2,3,4

UNION ALL 
SELECT DISTINCT 
    'last7days' as period,
    'lazada' as platform, 
    CONCAT(IFNULL(sku, 0), "_", IFNULL(shop_sku,0)) as product_id, 
    product_name as platform_name, 
    SUM(paid_price) as GMV,
    SUM(CASE WHEN status = 'delivered' THEN paid_price END) as NMV,
    COUNT(DISTINCT order_id) as gross_orders,
    COUNT(DISTINCT CASE WHEN status = 'delivered' THEN order_id END) as net_orders,
    COUNT(order_item_id) as gross_items,
    COUNT(CASE WHEN status = 'delivered' THEN order_item_id END) as net_items
FROM df_lazada_order_filter
WHERE date(created_at) BETWEEN (SELECT last7days_begin FROM df_date_period) AND (SELECT yesterday FROM df_date_period)
GROUP BY 1,2,3,4

UNION ALL
SELECT DISTINCT 
    'last30days' as period,
    'lazada' as platform, 
    CONCAT(IFNULL(sku, 0), "_", IFNULL(shop_sku,0)) as product_id, 
    product_name as platform_name, 
    SUM(paid_price) as GMV,
    SUM(CASE WHEN status = 'delivered'THEN paid_price END) as NMV,
    COUNT(DISTINCT order_id) as gross_orders,
    COUNT(DISTINCT CASE WHEN status = 'delivered' THEN order_id END) as net_orders,
    COUNT(order_item_id) as gross_items,
    COUNT(CASE WHEN status = 'delivered' THEN order_item_id END) as net_items
FROM df_lazada_order_filter
WHERE date(created_at) BETWEEN (SELECT last30days_begin FROM df_date_period) AND (SELECT yesterday FROM df_date_period)
GROUP BY 1,2,3,4

UNION ALL
SELECT DISTINCT 
    'monthtodate' as period,
    'lazada' as platform, 
    CONCAT(IFNULL(sku, 0), "_", IFNULL(shop_sku,0)) as product_id, 
    product_name as platform_name, 
    SUM(paid_price) as GMV,
    SUM(CASE WHEN status = 'delivered'THEN paid_price END) as NMV,
    COUNT(DISTINCT order_id) as gross_orders,
    COUNT(DISTINCT CASE WHEN status = 'delivered' THEN order_id END) as net_orders,
    COUNT(order_item_id) as gross_items,
    COUNT(CASE WHEN status = 'delivered' THEN order_item_id END) as net_items
FROM df_lazada_order_filter
WHERE date(created_at) BETWEEN (SELECT MTD_begin FROM df_date_period) AND (SELECT yesterday FROM df_date_period)
GROUP BY 1,2,3,4
"""

df_lazada_sellout_daily_period = spark.sql(query_sellout_daily_period)
df_lazada_sellout_daily_period.createOrReplaceTempView("df_lazada_sellout_daily_period")

display(df_lazada_sellout_daily_period)

# COMMAND ----------

# Lazada OFFTAKE PREVIOUS PERIOD
query_sellout_previous_period = """
SELECT DISTINCT 
    'PreviousYesterday' as period,
    'lazada' as platform, 
    CONCAT(IFNULL(sku, 0), "_", IFNULL(shop_sku,0)) as product_id, 
    product_name as platform_name, 
    SUM(paid_price) as GMV,
    SUM(CASE WHEN status = 'delivered' THEN paid_price END) as NMV,
    COUNT(DISTINCT order_id) as gross_orders,
    COUNT(DISTINCT CASE WHEN status = 'delivered' THEN order_id END) as net_orders,
    COUNT(order_item_id) as gross_items,
    COUNT(CASE WHEN status = 'delivered' THEN order_item_id END) as net_items
FROM df_lazada_order_filter
WHERE date(created_at) = (SELECT previous_yesterday FROM df_date_period)
GROUP BY 1,2,3,4

UNION ALL
SELECT DISTINCT 
    'PreviousLast7days' as period,
    'lazada' as platform, 
    CONCAT(IFNULL(sku, 0), "_", IFNULL(shop_sku,0)) as product_id, 
    product_name as platform_name, 
    SUM(paid_price) as GMV,
    SUM(CASE WHEN status = 'delivered' THEN paid_price END) as NMV,
    COUNT(DISTINCT order_id) as gross_orders,
    COUNT(DISTINCT CASE WHEN status = 'delivered' THEN order_id END) as net_orders,
    COUNT(order_item_id) as gross_items,
    COUNT(CASE WHEN status = 'delivered' THEN order_item_id END) as net_items
FROM df_lazada_order_filter
WHERE date(created_at) BETWEEN (SELECT previous_last7days_begin FROM df_date_period) AND (SELECT previous_last7days_end FROM df_date_period)
GROUP BY 1,2,3,4

UNION ALL 
SELECT DISTINCT 
    'PreviousLast30days' as period,
    'lazada' as platform, 
    CONCAT(IFNULL(sku, 0), "_", IFNULL(shop_sku,0)) as product_id, 
    product_name as platform_name, 
    SUM(paid_price) as GMV,
    SUM(CASE WHEN status = 'delivered' THEN paid_price END) as NMV,
    COUNT(DISTINCT order_id) as gross_orders,
    COUNT(DISTINCT CASE WHEN status = 'delivered' THEN order_id END) as net_orders,
    COUNT(order_item_id) as gross_items,
    COUNT(CASE WHEN status = 'delivered' THEN order_item_id END) as net_items
FROM df_lazada_order_filter
WHERE date(created_at) BETWEEN (SELECT previous_last30days_begin FROM df_date_period) AND (SELECT previous_last30days_end FROM df_date_period)
GROUP BY 1,2,3,4

UNION ALL 
SELECT DISTINCT 
    'PreviousMonthtodate' as period,
    'lazada' as platform, 
    CONCAT(IFNULL(sku, 0), "_", IFNULL(shop_sku,0)) as product_id, 
    product_name as platform_name, 
    SUM(paid_price) as GMV,
    SUM(CASE WHEN status = 'delivered' THEN paid_price END) as NMV,
    COUNT(DISTINCT order_id) as gross_orders,
    COUNT(DISTINCT CASE WHEN status = 'delivered' THEN order_id END) as net_orders,
    COUNT(order_item_id) as gross_items,
    COUNT(CASE WHEN status = 'delivered' THEN order_item_id END) as net_items
FROM df_lazada_order_filter
WHERE date(created_at) BETWEEN (SELECT previous_MTD_begin FROM df_date_period) AND (SELECT previous_MTD_end FROM df_date_period)
GROUP BY 1,2,3,4
"""

df_lazada_sellout_previous_period = spark.sql(query_sellout_previous_period)
df_lazada_sellout_previous_period.createOrReplaceTempView("df_lazada_sellout_previous_period")

display(df_lazada_sellout_previous_period)

# COMMAND ----------

query_lazada_sellout_agg = """
SELECT t1.*, Brand, Big_C, Small_C
FROM 
  (SELECT * FROM df_lazada_sellout_previous_period
  UNION ALL 
  SELECT * FROM df_lazada_sellout_daily_period) t1
LEFT JOIN 
  df_lazada_master_tree t3
ON t1.product_id = t3.product_id
"""

df_lazada_sellout_agg = spark.sql(query_lazada_sellout_agg)
df_lazada_sellout_agg.createOrReplaceTempView("df_lazada_sellout_agg")

display(df_lazada_sellout_agg)

# COMMAND ----------

# Load Lazada Traffic Period Data
df_lazada_daily_traffic_period = spark.read.format("csv").option("header","True").option("inferSchema", "True").load("dbfs:/mnt/adls/staging/ecom/BOT/Lazada/SellerCenter_DailyOfftake/")

display(df_lazada_daily_traffic_period)

df_lazada_daily_traffic_period.createOrReplaceTempView("df_lazada_daily_traffic_period")

# COMMAND ----------

query_lazada_traffic_clean = """
SELECT DISTINCT 
  Date as Period, 
  'lazada' as platform,
  CONCAT(IFNULL(seller_sku, 0), "_", IFNULL(sku_id,0)) as product_id,
  product_name as platform_name, 
  
  SUM(sku_views) as gross_product_views, 
  SUM(sku_visitors) as gross_unique_visitors,
  
  SUM(buyers) as gross_unique_buyers,
  SUM(GMV) as gmv,
  SUM(orders) as gross_orders, 
  SUM(units_Sold) as gross_units_sold
FROM df_lazada_daily_traffic_period
GROUP BY 1,2,3,4
"""
df_lazada_traffic_clean = spark.sql(query_lazada_traffic_clean)

display(df_lazada_traffic_clean)
df_lazada_traffic_clean.createOrReplaceTempView("df_lazada_traffic_clean")

# COMMAND ----------

query_lazada_traffic_combined = """
SELECT 
  t1.*, t2.gross_product_views, t2.gross_unique_visitors, t2.gross_unique_buyers
FROM 
  df_lazada_sellout_agg  t1
LEFT JOIN 
  df_lazada_traffic_clean t2
ON t1.period = t2.Period
AND t1.product_id = t2.product_id
"""

df_lazada_traffic_combined = spark.sql(query_lazada_traffic_combined)

display(df_lazada_traffic_combined)
df_lazada_traffic_combined.createOrReplaceTempView("df_lazada_traffic_combined")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Daily seperate

# COMMAND ----------

# Writing Lazada daily seperate data (in CSV format)
df_lazada_daily_performance_date_separate.toPandas().to_csv("/dbfs/mnt/adls/staging/ecom/Daily Performance/df_lazada_daily_performance_date_separate.csv", index=False, encoding="utf-8-sig")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ecom_db.df_lazada_daily_performance_date_separate;
# MAGIC
# MAGIC CREATE TABLE ecom_db.df_lazada_daily_performance_date_separate
# MAGIC USING csv
# MAGIC OPTIONS (path "dbfs:/mnt/adls/staging/ecom/Daily Performance/df_lazada_daily_performance_date_separate.csv" , header "true", delimiter =",")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data traffic Combined

# COMMAND ----------

# Writing Lazada traffic data (in CSV format)
df_lazada_traffic_combined.toPandas().to_csv("/dbfs/mnt/adls/staging/ecom/Daily Performance/df_lazada_traffic_combined.csv", index=False, encoding="utf-8-sig")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ecom_db.df_lazada_traffic_combined;
# MAGIC
# MAGIC CREATE TABLE ecom_db.df_lazada_traffic_combined
# MAGIC USING csv
# MAGIC OPTIONS (path "dbfs:/mnt/adls/staging/ecom/Daily Performance/df_lazada_traffic_combined.csv" , header "true", delimiter =",")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TESTING
# MAGIC

# COMMAND ----------

# df_lazada_report = spark.read.options(header='True', inferSchema='True', delimiter = ",").csv("dbfs:/mnt/adls/staging/ecom/Daily Performance/df_lazada_traffic_combined.csv")


# df_lazada_report.createOrReplaceTempView("df_lazada_report")
# display(df_lazada_report)


# COMMAND ----------

# %sql
# SELECT DISTINCT period
# FROM df_lazada_report
# -- ORDER BY 1 ,2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MIN(Date), MAX(Date)
# MAGIC FROM enrm_db.shopee_market_daily
# MAGIC WHERE category IN ('Deo&Fragrance')
# MAGIC LIMIT 10
# MAGIC
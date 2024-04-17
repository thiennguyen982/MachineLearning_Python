# Databricks notebook source
from datetime import datetime
from_date = datetime.strptime('01-10-2022', '%d-%m-%Y')
to_date = datetime.strptime('14-03-2023', '%d-%m-%Y')
from_date, to_date

# COMMAND ----------

from datetime import timedelta
from pyspark.sql.functions import lit

curr_date = from_date
data_address = "'Sản phẩm'!A6"
STORE_LIST = ['bpc', 'hcf', 'michiru']

while curr_date < to_date:
  for STORE_NAME in STORE_LIST:
    file_path = f'dbfs:/mnt/adls/staging/lazada/lazada_daily/Offtake/{STORE_NAME}/lazada_{STORE_NAME}_DailyOfftake_{curr_date.strftime("%Y_%m_%d")}.xls'
    # load raw data
    lazada_dailyofftake_df = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "false").option("treatEmptyValuesAsNulls", "false").option("dataAddress",f"{data_address}").load(file_path)
    lazada_dailyofftake_df = lazada_dailyofftake_df.withColumn("Date", lit(curr_date.strftime("%Y-%m-%d")))
    lazada_dailyofftake_df = lazada_dailyofftake_df.toPandas()

    lazada_dailyofftake_df = lazada_dailyofftake_df.rename(columns={
      "Tên sản phẩm": "product_name", 
      "Seller SKU": "seller_sku", 
      "SKU ID": "sku_id", 
      "Khách truy cập": "sku_visitors",
      "Khách truy cập (Định nghĩa mới)": "sku_visitors_new",
      "Khách truy cập sản phẩm (Định nghĩa cũ)": "sku_visitors_old",
      "Xem trang sản phẩm (Định nghĩa cũ)": "sku_views_old",
      "Xem trang sản phẩm (Định nghĩa mới)": "sku_views_new",
      "Giá trị khách truy cập (Định nghĩa mới)": "visitor_value_new",
      "Giá trị lượt truy cập (định nghĩa cũ)": "visitor_value_old",
      "Thêm vào khách truy cập xe đẩy": "a2c_visitors",
      "Thêm vào các đơn vị giỏ hàng": "a2c_units",
      "Tỷ lệ khách thêm vào giỏ hàng (định nghĩa mới)": "a2c_rate_new",
      "Tỷ lệ khách thêm vào giỏ hàng (định nghĩa cũ)": "a2c_rate_old",
      "Người dùng danh sách mong muốn": "Wishlist_Visitors",
      "Danh sách mong muốn": "Wishlists",
      "Lượt mua": "buyers",
      "Đơn hàng": "orders",
      "Sản phẩm bán được": "units_Sold",
      "Doanh thu": "GMV",
      "Tỷ lệ chuyển đổi (định nghĩa mới)": "CR_new",
      "Tỷ lệ chuyển đổi (định nghĩa cũ):": "CR_old",
      "Doanh thu cho mỗi người mua": "GMV_per_buyer"
    })
    lazada_dailyofftake_df.to_csv(f'/dbfs/mnt/adls/staging/ecom/BOT/Lazada/SellerCenter_ProductPerformance_New/{STORE_NAME}/lazada_{STORE_NAME}_dailyofftake_{curr_date.strftime("%Y_%m_%d")}.csv', index=False, encoding="utf-8-sig")
  curr_date = curr_date + timedelta(days=1)

# COMMAND ----------

df_lazada_daily_traffic_bpc_seperate = spark.read.options(header='True', inferSchema='True').csv("dbfs:/mnt/adls/staging/ecom/BOT/Lazada/SellerCenter_ProductPerformance_New/bpc/")
display(df_lazada_daily_traffic_bpc_seperate)

# COMMAND ----------


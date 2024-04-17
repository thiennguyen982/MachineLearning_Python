# Databricks notebook source
pip install pandas --upgrade

# COMMAND ----------

import os
import pandas as pd
import re
import datetime
from datetime import timedelta
import calendar
import time
import glob
from pyspark.sql.functions import lit

# COMMAND ----------

""" Get date info to check """
#store = "michiru"   # INPUT here
#store = "bpc"   # INPUT here
store = "hcf"   # INPUT here

download_folder = "/dbfs/mnt/adls/staging/lazada/lazada_daily/Offtake/{}/".format(store)
pattern = "Business Advisor - Product - Performance*.xls"

downloaded_files = glob.glob(os.path.join(download_folder, pattern ))
# print(downloaded_files)
regex = ".*/(.*).xls"
file_name = []
# destination_files = []

for file in downloaded_files:
    match = re.search(regex, file).group(1)
    file_name.append(match)
#     destination_files.append(target_file)    
#     destination_files.append(target_file)
#     dbutils.fs.mv("file:" + file, target_file)

names = []
dates = []

#flags required for reading the excel
isHeaderOn = "true"
isInferSchemaOn = "false"
#sheet address in excel
address1 = "'Product'!A1"
exception = ['']

# Read file to get date info
isHeaderOn = "true"
isInferSchemaOn = "false"
#sheet address in excel
# date_type= 'last30days'
all_item_name = [file_name[i] + '.xls' for i in range(0, len(file_name))]
print(all_item_name)


# COMMAND ----------

# display(dbutils.fs.ls("dbfs:/mnt/adls/staging/lazada/lazada_daily/Offtake/{}".format(storea)))

# COMMAND ----------

for item in all_item_name:
    print(item)
    exec( 'df = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "false").option("treatEmptyValuesAsNulls", "false").option("dataAddress","{}").load("dbfs:/mnt/adls/staging/lazada/lazada_daily/Offtake/{}/{}")'.format(address1, store, item) )
    info = df.columns[0]
#       print(info)
    regex = "\xa0(.*)_(.*)"
    from_date = re.search(regex, info).group(1)
    to_date = re.search(regex, info).group(2)

    print("From date: " + from_date)
    print("To date: " + to_date)
    suffix_name = from_date.replace("-", "_")
    print("Suffix name = " + str(suffix_name) + "\n")
    address = "'Product!A6"


# COMMAND ----------

# Create var
yesterday = datetime.datetime.strftime(datetime.datetime.today() - datetime.timedelta(days=1), '%Y-%m-%d')
yesterday_name = datetime.datetime.strftime(datetime.datetime.today() - datetime.timedelta(days=1), '%Y_%m_%d')
last_7day = datetime.datetime.strftime(datetime.datetime.today() - datetime.timedelta(days=7), '%Y-%m-%d')
last_30day = datetime.datetime.strftime(datetime.datetime.today() - datetime.timedelta(days=30), '%Y-%m-%d')

print("yesterday = " + yesterday)

def days_diff(d1, d2):
    d1 = datetime.datetime.strptime(d1, "%Y-%m-%d")
    d2 = datetime.datetime.strptime(d2, "%Y-%m-%d")
    return abs((d2 - d1).days) + 1

# COMMAND ----------

# Write data for yesterday
for item in all_item_name:  
      exec( 'df = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "false").option("treatEmptyValuesAsNulls", "false").option("dataAddress","{}").load("dbfs:/mnt/adls/staging/lazada/lazada_daily/Offtake/{}/{}")'.format(address1, store, item) )
      info = df.columns[0]
#       print(info)
      regex = " :(.*)_(.*)"
      from_date = re.search(regex, info).group(1)
      to_date = re.search(regex, info).group(2)
      
      print("From date: " + from_date)
      print("To date: " + to_date)
      suffix_name = from_date.replace("-", "_")
      
      print("Writing data: " + suffix_name + "\n")
      address = "'Product'!A6"
      if from_date == to_date == yesterday:
          suffix_name = 'yesterday'
          # Copy another copy for yesterday
          dbutils.fs.cp("/mnt/adls/staging/lazada/lazada_daily/Offtake/{}/{}".format(store,item), "/mnt/adls/staging/lazada/lazada_daily/Offtake/{}/".format(store) + "lazada_{}_DailyOfftake_{}.xls".format(store, suffix_name))

          exec( 'lazada_dailyofftake_{} = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "false").option("treatEmptyValuesAsNulls", "false").option("dataAddress","{}").load("dbfs:/mnt/adls/staging/lazada/lazada_daily/Offtake/{}/{}")'.format(suffix_name, address, store, item) )

          exec( 'lazada_dailyofftake_{} = lazada_dailyofftake_{}.withColumn("Date", lit("{}"))'.format(suffix_name, suffix_name, suffix_name.replace('_', '-')))
          exec( 'lazada_dailyofftake_{} = lazada_dailyofftake_{}.toPandas()'.format(suffix_name, suffix_name))
          col_rename = {"Product Name": "product_name", 
          "Seller SKU": "seller_sku", 
          "SKU ID": "sku_id", 
          "Visitors ": "sku_visitors",
          "Product Pageviews": "sku_views",
          "Visitor Value": "visitor_value",
          "Add to Cart Visitors": "a2c_visitors",
          "Add to Cart Units": "a2c_units",
          "Add to Cart Conversion Rate": "a2c_rate",
          "Wishlist Users": "Wishlist_Visitors",
          "Wishlists": "Wishlists",
          "Buyers": "buyers",
          "Orders": "orders",
          "Units Sold": "units_Sold",
          "Revenue": "GMV",
          "Conversion Rate": "CR",
          "Revenue per Buyer": "GMV_per_buyer"
                       }
          exec( 'lazada_dailyofftake_{}.rename(columns={}, inplace=True)'.format(suffix_name, col_rename) )
          exec( 'lazada_dailyofftake_{}.to_csv("/dbfs/mnt/adls/staging/ecom/BOT/Lazada/SellerCenter_DailyOfftake/lazada_{}_dailyofftake_{}.csv", index=False, encoding="utf-8-sig")'.format(suffix_name,  store, suffix_name))
#           # Write another copy for yesterday to csv
#           exec( 'lazada_dailyofftake_{}.to_csv("/dbfs/mnt/adls/staging/ecom/BOT/Lazada/SellerCenter_ProductPerformance/{}/lazada_{}_dailyofftake_{}.csv", index=False, encoding="utf-8-sig")'.format(suffix_name, store, store, 'yesterday'))

# COMMAND ----------

for item in all_item_name:
#       regex = "Phân tích bán hàng nâng cao - Sản phẩm - Hiệu quả bán hàng(.*).xls"
#       suffix_name = re.search(regex, item).group(1).replace('.', '_')
      
      exec( 'df = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "false").option("treatEmptyValuesAsNulls", "false").option("dataAddress","{}").load("dbfs:/mnt/adls/staging/lazada/lazada_daily/Offtake/{}/{}")'.format(address1, store, item) )
      info = df.columns[0]
#       print(info)
      regex = " :(.*)_(.*)"
      from_date = re.search(regex, info).group(1)
      to_date = re.search(regex, info).group(2)
      
      print("From date: " + from_date)
      print("To date: " + to_date)
      suffix_name = from_date.replace("-", "_")
      
      print("Writing data: " + suffix_name + "\n")
      address = "'Product'!A6"

      if from_date == to_date:
          exec( 'lazada_dailyofftake_{} = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "false").option("treatEmptyValuesAsNulls", "false").option("dataAddress","{}").load("dbfs:/mnt/adls/staging/lazada/lazada_daily/Offtake/{}/{}")'.format(suffix_name, address, store, item) )

          exec( 'lazada_dailyofftake_{} = lazada_dailyofftake_{}.withColumn("Date", lit("{}"))'.format(suffix_name, suffix_name, suffix_name.replace('_', '-')))
          exec( 'lazada_dailyofftake_{} = lazada_dailyofftake_{}.toPandas()'.format(suffix_name, suffix_name))
          col_rename = {"Product Name": "product_name", 
          "Seller SKU": "seller_sku", 
          "SKU ID": "sku_id", 
          "Visitors ": "sku_visitors",
          "Product Pageviews": "sku_views",
          "Visitor Value": "visitor_value",
          "Add to Cart Visitors": "a2c_visitors",
          "Add to Cart Units": "a2c_units",
          "Add to Cart Conversion Rate": "a2c_rate",
          "Wishlist Users": "Wishlist_Visitors",
          "Wishlists": "Wishlists",
          "Buyers": "buyers",
          "Orders": "orders",
          "Units Sold": "units_Sold",
          "Revenue": "GMV",
          "Conversion Rate": "CR",
          "Revenue per Buyer": "GMV_per_buyer"
                       }
          exec( 'lazada_dailyofftake_{}.rename(columns={}, inplace=True)'.format(suffix_name, col_rename) )
          exec( 'lazada_dailyofftake_{}.to_csv("/dbfs/mnt/adls/staging/ecom/BOT/Lazada/SellerCenter_ProductPerformance/{}/lazada_{}_dailyofftake_{}.csv", index=False, encoding="utf-8-sig")'.format(suffix_name, store, store, suffix_name))
          # Rename files
          dbutils.fs.mv("/mnt/adls/staging/lazada/lazada_daily/Offtake/{}/{}".format(store,item), "/mnt/adls/staging/lazada/lazada_daily/Offtake/{}/".format(store) + "lazada_{}_DailyOfftake_{}.xls".format(store, suffix_name))

      elif from_date != to_date :
          days_num = days_diff(from_date, to_date)
          suffix_name = "last" + str(days_num) +"days"

          exec( 'lazada_dailyofftake_{} = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "false").option("treatEmptyValuesAsNulls", "false").option("dataAddress","{}").load("dbfs:/mnt/adls/staging/lazada/lazada_daily/Offtake/{}/{}")'.format(suffix_name, address, store, item) )
          exec( 'lazada_dailyofftake_{} = lazada_dailyofftake_{}.withColumn("Date", lit("{}"))'.format(suffix_name, suffix_name, suffix_name.replace('_', '-')))
          exec( 'lazada_dailyofftake_{} = lazada_dailyofftake_{}.toPandas()'.format(suffix_name, suffix_name))
          col_rename = {"Product Name": "product_name", 
          "Seller SKU": "seller_sku", 
          "SKU ID": "sku_id", 
          "Visitors ": "sku_visitors",
          "Product Pageviews": "sku_views",
          "Visitor Value": "visitor_value",
          "Add to Cart Visitors": "a2c_visitors",
          "Add to Cart Units": "a2c_units",
          "Add to Cart Conversion Rate": "a2c_rate",
          "Wishlist Users": "Wishlist_Visitors",
          "Wishlists": "Wishlists",
          "Buyers": "buyers",
          "Orders": "orders",
          "Units Sold": "units_Sold",
          "Revenue": "GMV",
          "Conversion Rate": "CR",
          "Revenue per Buyer": "GMV_per_buyer"
                       }
          exec( 'lazada_dailyofftake_{}.rename(columns={}, inplace=True)'.format(suffix_name, col_rename) )
          exec( 'lazada_dailyofftake_{}.to_csv("/dbfs/mnt/adls/staging/ecom/BOT/Lazada/SellerCenter_DailyOfftake/lazada_{}_dailyofftake_{}.csv", index=False, encoding="utf-8-sig")'.format(suffix_name,  store, suffix_name))
        
          # Rename files
          dbutils.fs.mv("/mnt/adls/staging/lazada/lazada_daily/Offtake/{}/{}".format(store,item), "/mnt/adls/staging/lazada/lazada_daily/Offtake/{}/".format(store) + "lazada_{}_DailyOfftake_{}.xls".format(store, suffix_name))
      else: 
          print("Don't match file")

# COMMAND ----------

### TEST
yesterday_suffix = yesterday.replace("-", "_")

df_check = spark.read.csv("/mnt/adls/staging/ecom/BOT/Lazada/SellerCenter_ProductPerformance/{}/lazada_{}_dailyofftake_{}.csv".format(store, store, yesterday_suffix), header=True)
# display(df_check)

# COMMAND ----------


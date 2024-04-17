# Databricks notebook source
# MAGIC %md
# MAGIC ## SHOPEE SALES PERFORMANCES

# COMMAND ----------

# !pip install pandas --upgrade

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/sales_performance/

# COMMAND ----------

import calendar
import pandas as pd
import datetime 
from datetime import timedelta
import regex as re
from collections import OrderedDict
from pyspark.sql.functions import lit

# COMMAND ----------

# Define funtions
def file_exists(path):
  try:
    dbutils.fs.ls(path)
    print("*** File detected: " + path + '\n')
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
#       print("NOT existed: " + path)
      return False
    else:
#       print("NOT existed: " + path)
      raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### KEY METRICS MONTHLY REPORT

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sale - Traffic

# COMMAND ----------

### Create list of first and end date of month
year = 2020
list_first_date = []
list_last_date = []
for month in range(1,13):
  last_day = calendar.monthrange(year, month)[1]
  first_date = datetime.date(year, month, 1).strftime("%Y-%m-%d")
  list_first_date.append(first_date)
  
  last_date = datetime.date(year, month, last_day).strftime("%Y-%m-%d")
  list_last_date.append(last_date)
  
  print("Month = " + str(month) + " :")
  print("First date = " + str(first_date)) 
  print("Last date = " + str(last_date) + "\n") 
  
month_range = zip(list_first_date, list_last_date)
      

# COMMAND ----------

# for from_date, to_date in month_range:
#     suffix_name = from_date[:7].replace("-", "M")
#     df_name = "shopee_monthly_executive_{}".format(suffix_name)

#     file_name = 'Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx'.format(from_date.replace("-", '.'), to_date.replace("-", '.'))
#     file_path = "dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/sales_performance/{}".format(file_name)
    
#     file_exists(file_path)

# COMMAND ----------

address= "'Product Ranking'!B1"
### Writing data report
for from_date, to_date in month_range:
    suffix_name = from_date[:7].replace("-", "M")
    df_name = "shopee_monthly_sales_executive_{}".format(suffix_name)

    file_name = 'Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx'.format(from_date.replace("-", '.'), to_date.replace("-", '.'))
    file_path = "dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/sales_performance/{}".format(file_name)
    
    if file_exists(file_path):
        exec( '{} = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "false").option("treatEmptyValuesAsNulls", "false").option("dataAddress","{}").load("dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/sales_performance/{}")'.format(df_name, address, file_name) )
        exec( '{} = {}.withColumn("Month", lit("{}"))'.format(df_name, df_name, suffix_name))
        exec( '{} = {}.toPandas()'.format(df_name, df_name))
        ### Rename columns
        col_rename = {"Product Name": "product_name", 
                      "Product ID": "product_id", 
                      "Product Visitors": "product_visitors",
                      "Product Views": "product_view",
                      "Lượt mua": "buyers",
                      "Gross Orders": "orders",
                      "Gross Units Sold": "units_Sold",
                      "Gross Sales(₫)": "GMV",
                      "Gross Item Conversion Rate": "CR",
                      "Gross Average Basket Size(₫)": "ABS", 
                      "Gross Average Selling Price(₫)": "ASP"
                       }
        exec( '{}.rename(columns={}, inplace=True)'.format(df_name, col_rename) )
        ### Write data to folder 
        exec( '{}.to_csv("/dbfs/mnt/adls/staging/ecom/Mandala/Performance Compass/Executive Summary/Shopee/Monthly/sales_performance/{}.csv", index=False)'.format(df_name, df_name))
        print('Complete writing shopee monthly executive file: ' + df_name + '\n')
    else:
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC #### Product - Buyer

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/product_performance/

# COMMAND ----------

### Create list of first and end date of month
year = 2022
list_first_date = []
list_last_date = []
for month in range(1,13):
  last_day = calendar.monthrange(year, month)[1]
  first_date = datetime.date(year, month, 1).strftime("%Y-%m-%d")
  list_first_date.append(first_date)
  
  last_date = datetime.date(year, month, last_day).strftime("%Y-%m-%d")
  list_last_date.append(last_date)
  
  print("Month = " + str(month) + " :")
  print("First date = " + str(first_date)) 
  print("Last date = " + str(last_date) + "\n") 
  
month_range = zip(list_first_date, list_last_date)
      

# COMMAND ----------

# for from_date, to_date in month_range:
#     suffix_name = from_date[:7].replace("-", "M")
#     df_name = "shopee_monthly_product_executive_{}".format(suffix_name)

#     file_name = 'Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-{}_{}.xlsx'.format(from_date.replace("-", '.'), to_date.replace("-", '.'))
#     file_path = "dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/product_performance/{}".format(file_name)
    
#     file_exists(file_path)

# COMMAND ----------

from pyspark.sql.functions import lit

address = "'Product Performance SKU Level'!A1"
### Writing data report
for from_date, to_date in month_range:
    suffix_name = from_date[:7].replace("-", "M")
    df_name = "shopee_monthly_product_executive_{}".format(suffix_name)
    file_name = 'Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-{}_{}.xlsx'.format(from_date.replace("-", '.'), to_date.replace("-", '.'))
    
    file_path = "dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/product_performance/{}".format(file_name)
    
    if file_exists(file_path):
        exec( '{} = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "false").option("treatEmptyValuesAsNulls", "false").option("dataAddress","{}").load("dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/product_performance/{}")'.format(df_name, address, file_name) )
        # ADD Month COLUMN FOR DATA
        exec( '{} = {}.withColumn("Month", lit("{}"))'.format(df_name, df_name, suffix_name))
        exec( '{} = {}.toPandas()'.format(df_name, df_name))
        ### Rename columns
        col_rename = {"Product ID": "product_id", 
                  "Parent SKU": "parent_sku", 
                  "Shop ID": "shopid", 
                  "Shop name": "shop_name",
                  "Product Rating": "product_rating",
                  "Net Units Sold": "net_units_sold",
                  "Net Orders": "net_orders",
                  "Net Sales(₫)": "nmv",
                  "Net # of Unique Buyers": "net_unique_buyers",
                  "Gross Units Sold": "gross_units_sold",
                  "Gross Orders": "gross_orders",
                  "Gross Sales(₫)": "gmv",
                  "Gross # of Unique Buyers": "gross_unique_buyers",
                  "ATC Units": "atc_units",
                  "Current Stock": "current_stock"
                  }
        exec( '{}.rename(columns={}, inplace=True)'.format(df_name, col_rename) )
        ### Write data to folder 
        exec( '{}.to_csv("/dbfs/mnt/adls/staging/ecom/Mandala/Performance Compass/Executive Summary/Shopee/Monthly/product_performance/{}.csv", index=False)'.format(df_name, df_name))
        print('Complete writing shopee monthly product executive file: ' + df_name + '\n')
    else:
        pass

# COMMAND ----------


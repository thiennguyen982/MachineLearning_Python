# Databricks notebook source
# MAGIC %md
# MAGIC ### SHOPEE SALES PERFORMANCES

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/sales_performance/

# COMMAND ----------

import calendar
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
    print("File detected")
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise

# COMMAND ----------

# MAGIC %md
# MAGIC #### KEY METRICS MONTHLY REPORT

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

### Writing data report
for from_date, to_date in month_range:
    name = "shopee_sales_performance_key_metrics_{}_to_{}".format(from_date.replace("-", ""), to_date.replace("-", ""))

    file_name = 'Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx'.format(from_date.replace("-", '.'), to_date.replace("-", '.'))
    file_path = "dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/sales_performance/{}".format(file_name)
    if file_exists(file_path):
      address= "'Key Metrics'!A1:N3"
      exec( '{} = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "false").option("treatEmptyValuesAsNulls", "false").option("dataAddress","{}").load("dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/sales_performance/{}")'.format(name, address, file_name) )

      ### Filter only data of Region = Vietnam
      exec('{} = {}.filter("Region == \'VN\'")'.format(name, name))
      ### Write data to folder 
      exec( '{}.toPandas().to_csv("/dbfs/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_KeyMetrics/Monthly/{}.csv", index=False)'.format(name, name))
      print('Complete writing key metrics report file: ' + name)
    else:
      pass

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_KeyMetrics/Monthly/

# COMMAND ----------

# MAGIC %md
# MAGIC #### KEY METRICS DAILY REPORT

# COMMAND ----------

# import regex as re
# from collections import OrderedDict

# ### Create list of dates
# start_date = datetime.datetime.strptime('2021-01-01', "%Y-%m-%d")  # Input start/min date
# end_date = datetime.datetime.strptime('2021-10-31', "%Y-%m-%d")   # Input end/max date

# list_date = list(OrderedDict(
#   ((start_date + timedelta(_)).strftime("%Y-%m-%d"), None) for _ in range((end_date - start_date).days+1)).keys())

# list_date[::-1]


# COMMAND ----------

### Create list of first and end date of month
year = 2021
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

### Writing data report
for from_date, to_date in month_range:
    name = "shopee_sales_performance_key_metrics_daily_{}_to_{}".format(from_date.replace("-", ""), to_date.replace("-", ""))

    file_name = 'Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx'.format(from_date.replace("-", '.'), to_date.replace("-", '.'))
    file_path = "dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/sales_performance/{}".format(file_name)
    if file_exists(file_path):
      address= "'Key Metrics'!A5"
      exec( '{} = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "false").option("treatEmptyValuesAsNulls", "false").option("dataAddress","{}").load("dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/sales_performance/{}")'.format(name, address, file_name) )

      ### Write data to folder 
      exec( '{}.toPandas().to_csv("/dbfs/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_KeyMetrics/Daily/{}.csv", index=False)'.format(name, name))
      print('Complete writing key metrics report file: ' + name)
    else:
      pass

# COMMAND ----------

# MAGIC %md
# MAGIC ### SHOPEE PRODUCT PERFORMANCES

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/product_performance/

# COMMAND ----------

# # ADD DATE COLUMN FOR DATA (ALL FOLDER)
# from pyspark.sql.functions import lit

# isHeaderOn = "true"
# isInferSchemaOn = "false"
# #sheet address in excel
# address = "'Product Performance SKU Level'!A1"
# exception = ['shopee_product_performance_2021_04_13']

# date = "2021-11-05"   ### INPUT DATE
# name = "shopee_product_performance_{}".format(date.replace("-", "_"))


# file_name = 'Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-{}_{}.xlsx'.format(date.replace("-", '.'), date.replace("-", '.'))
# exec( '{} = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "false").option("treatEmptyValuesAsNulls", "false").option("dataAddress","{}").load("dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/product_performance/{}")'.format(name, address, file_name) )
# exec( '{} = {}.withColumn("Date", lit("{}")).drop("No.").toPandas()'.format(name, name, date))
# col_rename = {"Product ID": "product_id", 
#                   "Parent SKU": "parent_sku", 
#                   "Shop ID": "shopid", 
#                   "Shop name": "shop_name",
#                   "Product Rating": "product_rating",
#                   "Net Units Sold": "net_units_sold",
#                   "Net Orders": "net_orders",
#                   "Net Sales(₫)": "nmv",
#                   "Net # of Unique Buyers": "net_unique_buyers",
#                   "Gross Units Sold": "gross_units_sold",
#                   "Gross Orders": "gross_orders",
#                   "Gross Sales(₫)": "gmv",
#                   "Gross # of Unique Buyers": "gross_unique_buyers",
#                   "ATC Units": "atc_units",
#                   "Current Stock": "current_stock"
#                   }
# exec( '{}.rename(columns={}, inplace=True)'.format(name, col_rename))
# print('Writing data : {}'.format(name))
# exec( '{}.to_parquet("/dbfs/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_ProductPerformance/{}.snappy")'.format(name, name))

# COMMAND ----------


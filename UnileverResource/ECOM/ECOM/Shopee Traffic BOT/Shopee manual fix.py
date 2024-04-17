# Databricks notebook source
# MAGIC %md
# MAGIC ### SHOPEE SALES PERFORMANCES

# COMMAND ----------

# !pip install pandas --upgrade

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/sales_performance/

# COMMAND ----------

import pandas as pd
pd.__version__

# COMMAND ----------

###### FOR MANUAL FIX SALES TRAFFIC ONLY #####
from pyspark.sql.functions import lit
date = "2022-09-01"   ### INPUT Date
name = "shopee_sales_performance_gross_{}".format(date.replace("-", "_"))

file_name = 'Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx'.format(date.replace("-", '.'), date.replace("-", '.'))

address= "'Product Ranking'!A1"
exec( '{} = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "false").option("treatEmptyValuesAsNulls", "false").option("dataAddress","{}").load("dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/sales_performance/{}")'.format(name, address, file_name) )
exec( '{} = {}.withColumn("Date", lit("{}")).drop("No.").toPandas()'.format(name, name, date))
exec( '{}.to_csv("/dbfs/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_SalesPerformance/{}.csv", index=False)'.format(name, name))
print('Complete clean & write file: ' + name)


# COMMAND ----------

# MAGIC %md
# MAGIC ### SHOPEE PRODUCT PERFORMANCES

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/product_performance/

# COMMAND ----------

# ADD DATE COLUMN FOR DATA (ALL FOLDER)
from pyspark.sql.functions import lit

isHeaderOn = "true"
isInferSchemaOn = "false"
#sheet address in excel
address = "'Product Performance SKU Level'!A1"
exception = ['shopee_product_performance_2021_04_13']

date = "2022-09-01"   ### INPUT Date
name = "shopee_product_performance_{}".format(date.replace("-", "_"))


file_name = 'Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-{}_{}.xlsx'.format(date.replace("-", '.'), date.replace("-", '.'))
exec( '{} = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "false").option("treatEmptyValuesAsNulls", "false").option("dataAddress","{}").load("dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/product_performance/{}")'.format(name, address, file_name) )
exec( '{} = {}.withColumn("Date", lit("{}")).drop("No.").toPandas()'.format(name, name, date))
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
exec( '{}.rename(columns={}, inplace=True)'.format(name, col_rename))
print('Writing data : {}'.format(name))
exec( '{}.to_parquet("/dbfs/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_ProductPerformance/{}.snappy")'.format(name, name))

# COMMAND ----------


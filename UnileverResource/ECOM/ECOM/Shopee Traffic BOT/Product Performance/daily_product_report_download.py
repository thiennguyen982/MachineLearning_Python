# Databricks notebook source
# MAGIC %run ./_init_product_performance_setup

# COMMAND ----------

# MAGIC %md
# MAGIC #### Download daily reports

# COMMAND ----------

### Create vars for daily report
yesterday = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=1), '%Y.%m.%d')
last_7day = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=7), '%Y.%m.%d')
last_30day = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=30), '%Y.%m.%d')

last_7day_begin = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=7), '%Y-%m-%d')
last_30day_begin = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=30), '%Y-%m-%d')
month_to_date = mtd_begin.replace("-", ".")

pattern_yesterday = "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-{}_{}.xlsx".format(yesterday, yesterday)
pattern_last_7days= "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-{}_{}.xlsx".format(last_7day, yesterday)
pattern_last_30days = "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-{}_{}.xlsx".format(last_30day, yesterday)
pattern_monthtodate = "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-{}_{}.xlsx".format(month_to_date, yesterday)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Yesterday

# COMMAND ----------

### Daily download Dashboard
### Download Yesterday
download_num = 1
while (not file_exists('file:/tmp/Shopee_BOT_ProductPerformance/' + pattern_yesterday)) and download_num <5:
  try:
    refresh_page()
    download_yesterday()
    time.sleep(5)
    print("Download processing")
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      download_num +=1
      print("Download failed. Try to download = " + str(download_num))
    else:
      raise


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Last 7 days

# COMMAND ----------

### Download Last 7 day
download_num = 1
while (not file_exists('file:/tmp/Shopee_BOT_ProductPerformance/' + pattern_last_7days)) and download_num <5:
  try:
    refresh_page()
    download_last7days()
#     download_period_file(from_date=last_7day_begin, to_date=yesterday).execute()  ## Alternative
    time.sleep(5)
    print("Download processing")
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      download_num +=1
      print("Download failed. Try to download = " + str(download_num))
    else:
      raise


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Last 30 days

# COMMAND ----------

### Download Last 30 day
download_num = 1
while (not file_exists('file:/tmp/Shopee_BOT_ProductPerformance/' + pattern_last_30days)) and download_num <5:
  try:
    refresh_page()
    download_last30days()
#     download_period_file(from_date=last_30day_begin, to_date=yesterday).execute() ##Alternative
    time.sleep(5)
    print("Download processing")
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      download_num +=1
      print("Download failed. Try to download = " + str(download_num))
    else:
      raise


# COMMAND ----------

# MAGIC %md
# MAGIC ##### MTD

# COMMAND ----------

### Download MTD period reports
pattern_mtd_period = "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-{}_{}.xlsx".format(mtd_begin.replace("-", "."), yesterday.replace("-", "."))
yesterday_fix = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=1), '%Y-%m-%d')

download_num = 1
while (not file_exists('file:/tmp/Shopee_BOT_ProductPerformance/' + pattern_mtd_period)) and download_num <5:
  try:
    refresh_page()
    if mtd_begin > yesterday:
      mtd_begin_fix = datetime.datetime.strftime((current_datetime - datetime.timedelta(1)).replace(day=1), '%Y-%m-%d')
      
      download_period_file(from_date=mtd_begin_fix, to_date=yesterday_fix).execute()
    else:
      download_period_file(from_date=mtd_begin, to_date=yesterday_fix).execute()
    time.sleep(1)
    download_num +=1
    print("Download processing")
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      download_num +=1
      print("Download failed. Try to download = " + str(download_num))
    else:
      raise

# COMMAND ----------

# MAGIC %fs ls file:/tmp/Shopee_BOT_ProductPerformance/

# COMMAND ----------

# MAGIC %md
# MAGIC #### Moving & rename Daily reports to destination

# COMMAND ----------

### Create vars for daily report
yesterday = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=1), '%Y.%m.%d')
last_7day = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=7), '%Y.%m.%d')
last_30day = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=30), '%Y.%m.%d')
month_to_date = mtd_begin.replace("-", ".")

pattern_yesterday = "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-{}_{}.xlsx".format(yesterday, yesterday)
pattern_last_7days= "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-{}_{}.xlsx".format(last_7day, yesterday)
pattern_last_30days = "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-{}_{}.xlsx".format(last_30day, yesterday)
pattern_monthtodate = "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-{}_{}.xlsx".format(month_to_date, yesterday)

destination_path = "dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/product_performance/"
   
download_folder = prefs["download.default_directory"]
yesterday_path = glob.glob(os.path.join(download_folder, pattern_yesterday))[0]
last7days_path = glob.glob(os.path.join(download_folder, pattern_last_7days))[0]
last30days_path = glob.glob(os.path.join(download_folder, pattern_last_30days))[0]
monthtodate_path = glob.glob(os.path.join(download_folder, pattern_monthtodate))[0]


yesterday_destination = download_folder + "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-DailyOfftake_yesterday.xlsx"
last7days_destination = download_folder + "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-DailyOfftake_last7days.xlsx"
last30days_destination = download_folder + "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-DailyOfftake_last30days.xlsx"
monthtodate_destination = download_folder + "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-DailyOfftake_monthtodate.xlsx"

# Copy yesterday file with new name and move original file to destination
dbutils.fs.cp("file:" + yesterday_path, "file:" + yesterday_destination)
print("Complete copy file to: "+ yesterday_destination)

# Move/Rename last 7 days and last 30 days file to destination
dbutils.fs.cp("file:" + last7days_path, "file:" + last7days_destination)

dbutils.fs.cp("file:" + last30days_path, "file:" + last30days_destination)

if monthtodate_path == yesterday_path:
  dbutils.fs.cp("file:" + yesterday_destination, "file:" + monthtodate_destination)
elif monthtodate_path == last7days_path:
  dbutils.fs.cp("file:" + last7days_destination, "file:" + monthtodate_destination)
elif monthtodate_path == last30days_path:
  dbutils.fs.cp("file:" + last30days_destination, "file:" + monthtodate_destination)
else:
  dbutils.fs.cp("file:" + monthtodate_path, "file:" + monthtodate_destination)

# COMMAND ----------

# MAGIC %fs ls file:/tmp/Shopee_BOT_ProductPerformance/

# COMMAND ----------

# MAGIC %md
# MAGIC #### Moving downloaded files to destination (ADLS)

# COMMAND ----------


### Move downloaded files to destination
pattern = "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance*.xlsx"
destination = "dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/product_performance/"

destination_files, file_name = move_to(pattern, destination)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning data report, add column date to file

# COMMAND ----------

# MAGIC %scala
# MAGIC import shadeio.poi.openxml4j.util.ZipSecureFile
# MAGIC ZipSecureFile.setMinInflateRatio(0)

# COMMAND ----------

# all_item_name = [file_name[i] + '.xlsx' for i in range(0, len(file_name))]
# regex = "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-(.*)_(.*).xlsx"
# exception = ['shopee_product_performance_2021_04_13']

# for item in all_item_name:
#     group1 = re.search(regex, item).group(1).replace('.', '_')
#     group2 = re.search(regex, item).group(2).replace('.', '_')
#     name = "shopee_product_performance_" + re.search(regex, item).group(2).replace('.', '_')
#     if (name not in exception and group1 == group2) or (group1 == 'DailyOfftake'):
#         print(item)

# COMMAND ----------

# ADD DATE COLUMN FOR DATA (ALL FOLDER)
from pyspark.sql.functions import lit

all_item_path = destination_files
all_item_name = [file_name[i] + '.xlsx' for i in range(0, len(file_name))]


names = []
dates = []

#flags required for reading the excel
isHeaderOn = "true"
isInferSchemaOn = "false"
#sheet address in excel
address = "'Product Performance SKU Level'!A1"
exception = ['shopee_product_performance_2021_04_13']
regex = "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-(.*)_(.*).xlsx"

for item in all_item_name:
    group1 = re.search(regex, item).group(1).replace('.', '_')
    group2 = re.search(regex, item).group(2).replace('.', '_')
    name = "shopee_product_performance_" + re.search(regex, item).group(2).replace('.', '_')
    if (name not in exception and group1 == group2) or (group1 == 'DailyOfftake'):
      try:
          date = re.search(regex, item).group(2).replace('.', '-')
          names.append(name)
          dates.append(date)
#           print("EXECUTING CODE: " + '{} = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "false").option("treatEmptyValuesAsNulls", "false").option("dataAddress","{}").load("dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/product_performance/{}")'.format(name, address, item))
          
          exec( '{} = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "false").option("treatEmptyValuesAsNulls", "false").option("dataAddress","{}").load("dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/product_performance/{}")'.format(name, address, item) )
#           print("sucess read")

#           print("EXECUTING CODE: " + '{} = {}.withColumn("Date", lit("{}")).drop("No.").toPandas()'.format(name, name, date))
          exec( '{} = {}.withColumn("Date", lit("{}")).drop("No.").toPandas()'.format(name, name, date))
#           print("sucess convert to pandas")
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
#           print("sucess rename")
          ### Writing parquet data format: 
          print('Writing data : {}.snappy'.format(name) + "\n")
          
#           print("EXECUTING CODE: " + '{}.to_parquet("/dbfs/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_ProductPerformance/{}.snappy")'.format(name, name))
          exec( '{}.to_parquet("/dbfs/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_ProductPerformance/{}.snappy")'.format(name, name))     
#         print('Writing data : {}'.format(name))
#         exec( '{}.to_parquet("/dbfs/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_ProductPerformance/{}.snappy")'.format(name, name))
      except:
#           raise
          print("===> ERROR at: " + str(name) + "\n")
    else:
      pass

# COMMAND ----------

# # Load Shopee Product period group Data
# df_shopee_product_performance_daily = spark.read.format("parquet").load("dbfs:/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_DailyOfftake/product_performance/")
# display(df_shopee_product_performance_daily)
# df_shopee_product_performance_daily.createOrReplaceTempView("df_shopee_product_performance_daily")

# COMMAND ----------

### MOVE daily offtake to folder
check_path = "mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_ProductPerformance/"
daily_offtake_path = "mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_DailyOfftake/product_performance/"

if file_exists(check_path + "shopee_product_performance_yesterday.snappy"):
  dbutils.fs.mv("/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_ProductPerformance/shopee_product_performance_yesterday.snappy", daily_offtake_path + "shopee_dailyofftake_yesterday.snappy")
  print("Move yesterday file to Daily Offtake folder")
else:
  print("File shopee_product_performance_yesterday does not exist!")

if file_exists(check_path + "shopee_product_performance_last7days.snappy"):
  dbutils.fs.mv("/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_ProductPerformance/shopee_product_performance_last7days.snappy", daily_offtake_path + "shopee_dailyofftake_last7days.snappy")
  print("Move last 7 days file to Daily Offtake folder")
else:
  print("File shopee_product_performance_last7days does not exist!")

if file_exists(check_path + "shopee_product_performance_last30days.snappy"):
  dbutils.fs.mv("/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_ProductPerformance/shopee_product_performance_last30days.snappy", daily_offtake_path + "shopee_dailyofftake_last30days.snappy")
  print("Move last 30 days file to Daily Offtake folder")
else:
  print("File shopee_product_performance_last30days does not exist!")

if file_exists(check_path + "shopee_product_performance_monthtodate.snappy"):
  dbutils.fs.mv("/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_ProductPerformance/shopee_product_performance_monthtodate.snappy", daily_offtake_path + "shopee_dailyofftake_monthtodate.snappy")
  print("Move MTD file to Daily Offtake folder")
else:
  print("File shopee_product_performance_monthtodate does not exist!")

# COMMAND ----------

# MAGIC %fs ls mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_DailyOfftake/product_performance
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove unnecessary files

# COMMAND ----------


### Remove daily report
dbutils.fs.rm("file:" + last7days_path)
print("\n Complete rename last 7 days file to: "+ last7days_destination)

dbutils.fs.rm("file:" + last30days_path)
print("\n Complete rename last 30 days file to: " + last30days_destination)

dbutils.fs.rm("file:" + monthtodate_path)
print("\n Complete rename MTD file to: " + monthtodate_destination)

# COMMAND ----------

# # Checking data yesterday download
# yesterday_name = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=1), '%Y_%m_%d')

# df_check = spark.read.parquet("/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_ProductPerformance/shopee_product_performance_{}.snappy".format(yesterday_name))
# df_check.createOrReplaceTempView("df_check")
# display(df_check)

# COMMAND ----------

# #FIX DATA
# all_item_path =['dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/product_performance/Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-2020.07.10_2020.07.10.xlsx']
# all_item_name = ['Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-2020.07.10_2020.07.10.xlsx']

# names = []
# dates = []

# #flags required for reading the excel
# isHeaderOn = "true"
# isInferSchemaOn = "false"
# #sheet address in excel
# address = "'Product Performance SKU Level'!A1"

# for item in all_item_name:
#     name = "shopee_product_performance_" + re.search(regex, item).group(2).replace('.', '_')
#     date = re.search(regex, item).group(2).replace('.', '-')
#     names.append(name)
#     dates.append(date)
#     exec( '{} = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "false").option("treatEmptyValuesAsNulls", "false").option("dataAddress","{}").load("dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/product_performance/{}")'.format(name, address, item) )
#     exec( '{} = {}.withColumn("Date", lit("{}")).drop("No.").toPandas()'.format(name, name, date))
#     col_rename = {"Product ID": "product_id", 
#                 "Parent SKU": "parent_sku", 
#                 "Shop ID": "shopid", 
#                 "Shop name": "shop_name",
#                 "Product Rating": "product_rating",
#                 "Net Units Sold": "net_units_sold",
#                 "Net Orders": "net_orders",
#                 "Net Sales(₫)": "nmv",
#                 "Net # of Unique Buyers": "net_unique_buyers",
#                 "Gross Units Sold": "gross_units_sold",
#                 "Gross Orders": "gross_orders",
#                 "Gross Sales(₫)": "gmv",
#                 "Gross # of Unique Buyers": "gross_unique_buyers",
#                 "Current Stock": "current_stock"}
#     exec( '{}.rename(columns={}, inplace=True)'.format(name, col_rename))
#     exec( '{}.to_parquet("/dbfs/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_ProductPerformance/{}.snappy")'.format(name, name))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customised report

# COMMAND ----------

# ## DOWNLOAD FILE (INPUT customzied date range to download)
# refresh_page()
# download_period_file(from_date='2022-09-01', to_date='2022-09-01').execute()
# # download_files(from_date='2022-06-12', to_date='2022-06-13').execute()

# #### Move download files to destination
# pattern = "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-*"
# download_folder = prefs["download.default_directory"]
# destination_path = "dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/product_performance/"

# downloaded_files = glob.glob(os.path.join(download_folder, pattern ))
# regex = ".*/(.*.xlsx)"
# file_name = []
# destination_files = []

# for file in downloaded_files:
  
#     match = re.search(regex, file).group(1)
#     file_name.append(match)
    
#     destination = destination_path + match
#     print("\n Writing to: " + destination)
#     dbutils.fs.cp("file:" + file, destination)


# COMMAND ----------


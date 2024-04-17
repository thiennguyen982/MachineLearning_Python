# Databricks notebook source
# MAGIC %run ./_init_product_performance_setup

# COMMAND ----------

# ## DOWNLOAD FILE (INPUT customzied date range to download)
# refresh_page()
# download_period_file(from_date='2022-08-08', to_date='2022-08-08').execute()
# # download_files(from_date='2022-06-12', to_date='2022-06-13').execute()

# #### Move download files to dtestination
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

# MAGIC %md
# MAGIC #### Download previous period report & MTD report
# MAGIC  

# COMMAND ----------

# Download previous period report
# Create vars
current_datetime = datetime.datetime.now() + datetime.timedelta(hours=7)
current_date = datetime.datetime.strftime(current_datetime, '%Y-%m-%d')
yesterday = datetime.datetime.strftime(current_datetime - datetime.timedelta(days=1), '%Y-%m-%d')
previous_yesterday = datetime.datetime.strftime(current_datetime - datetime.timedelta(days=2), '%Y-%m-%d')

previous_last7days_begin = datetime.datetime.strftime(current_datetime - datetime.timedelta(days=14), '%Y-%m-%d')
previous_last7days_end = datetime.datetime.strftime(current_datetime - datetime.timedelta(days=8), '%Y-%m-%d')

previous_last30days_begin = datetime.datetime.strftime(current_datetime - datetime.timedelta(days=60), '%Y-%m-%d')
previous_last30days_end = datetime.datetime.strftime(current_datetime - datetime.timedelta(days=31), '%Y-%m-%d')


# COMMAND ----------

### Define mtd and previous mtd period
mtd_begin = datetime.datetime.strftime((current_datetime - datetime.timedelta(days=1)).replace(day=1), '%Y-%m-%d')

last_day_of_prev_month = current_datetime.replace(day=1) - datetime.timedelta(days=1)

previous_mtd_begin = datetime.datetime.strftime(last_day_of_prev_month.replace(day=1), '%Y-%m-%d')

if (last_day_of_prev_month.year == current_datetime.year) and (last_day_of_prev_month.day > (current_datetime - datetime.timedelta(days=1)).day): # When last month and current month in the same year
    previous_mtd_end = datetime.datetime.strftime((current_datetime - datetime.timedelta(days=1)).replace(month=current_datetime.month -1), '%Y-%m-%d')
# elif (last_day_of_prev_month.year < current_datetime.year) and (last_day_of_prev_month.day > (current_datetime - datetime.timedelta(days=1)).day): ## When last month is in last year
#     previous_mtd_end = datetime.datetime.strftime((current_datetime - datetime.timedelta(days=1)).replace(month=12), '%Y-%m-%d')
else:
    previous_mtd_end = datetime.datetime.strftime(last_day_of_prev_month, '%Y-%m-%d')


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Previous MTD

# COMMAND ----------

### Download Previous MTD period reports
pattern_previous_mtd_period = "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-{}_{}.xlsx".format(previous_mtd_begin.replace("-", "."), previous_mtd_end.replace("-", "."))

### Check previous MTD file history
if file_exists('dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/product_performance/' + pattern_previous_mtd_period):
  dbutils.fs.cp('dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/product_performance/' + pattern_previous_mtd_period, "file:/tmp/Shopee_BOT_ProductPerformance/" + pattern_previous_mtd_period)
else:
  print("Previous MTD file does not exist")

### Dowload file if not in history
download_num = 1
while (not file_exists('file:/tmp/Shopee_BOT_ProductPerformance/' + pattern_previous_mtd_period)) and download_num <5:
  try:
    refresh_page()
    download_period_file(from_date=previous_mtd_begin, to_date=previous_mtd_end).execute()
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

# MAGIC %md
# MAGIC ##### MTD

# COMMAND ----------

# ### Download MTD period reports
# pattern_mtd_period = "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-{}_{}.xlsx".format(mtd_begin.replace("-", "."), yesterday.replace("-", "."))

# download_num = 1
# while (not file_exists('file:/tmp/Shopee_BOT_ProductPerformance/' + pattern_mtd_period)) and download_num <5:
#   try:
#     refresh_page()
#     if mtd_begin > yesterday:
#       mtd_begin_fix = datetime.datetime.strftime((current_datetime - datetime.timedelta(1)).replace(day=1), '%Y-%m-%d')
#       download_period_file(from_date=mtd_begin_fix, to_date=yesterday).execute()
#     else:
#       download_period_file(from_date=mtd_begin, to_date=yesterday).execute()
#     time.sleep(1)
#     download_num +=1
#     print("Download processing")
#   except Exception as e:
#     if 'java.io.FileNotFoundException' in str(e):
#       download_num +=1
#       print("Download failed. Try to download = " + str(download_num))
#     else:
#       raise

# COMMAND ----------

##### Create vars for previous period report
previous_yesterday_name = previous_yesterday.replace('-','.')

previous_last7days_begin_name = previous_last7days_begin.replace('-','.')
previous_last7days_end_name = previous_last7days_end.replace('-','.')

previous_last30days_begin_name = previous_last30days_begin.replace('-','.')
previous_last30days_end_name = previous_last30days_end.replace('-','.')

previous_mtd_begin_name = previous_mtd_begin.replace('-','.')
previous_mtd_end_name = previous_mtd_end.replace('-','.')

### Pattern for previous file report
pattern_previous_yesterday = "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-{}_{}.xlsx".format(previous_yesterday_name, previous_yesterday_name)

pattern_previous_last_7days= "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-{}_{}.xlsx".format(previous_last7days_begin_name, previous_last7days_end_name)

pattern_previous_last_30days = "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-{}_{}.xlsx".format(previous_last30days_begin_name, previous_last30days_end_name)

pattern_previous_mtd = "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-{}_{}.xlsx".format(previous_mtd_begin_name, previous_mtd_end_name)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Previous Yesterday

# COMMAND ----------

### Download previous period reports
### Download Previous Yesterday
if file_exists('dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/product_performance/' + pattern_previous_yesterday) == False:
  download_num = 1
  while (not file_exists('file:/tmp/Shopee_BOT_ProductPerformance/' + pattern_previous_yesterday)) and download_num <5:
    try:
      refresh_page()
      download_files(from_date=previous_yesterday, to_date=previous_yesterday).execute()
      time.sleep(1)
      download_num +=1
      print("Download processing")
    except Exception as e:
      if 'java.io.FileNotFoundException' in str(e):
        download_num +=1
        print("Download failed. Try to download = " + str(download_num))
      else:
        raise
else:
  print("Previous yesterday file is existed. Passed!")
  pass

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Previous Last 7 days

# COMMAND ----------

### Download Previous last 7 days
download_num = 1
while (not file_exists('file:/tmp/Shopee_BOT_ProductPerformance/' + pattern_previous_last_7days)) and download_num <5:
  try:
    refresh_page()
    download_period_file(from_date=previous_last7days_begin, to_date=previous_last7days_end).execute()
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

# MAGIC %md
# MAGIC ##### Previous Last 30 days

# COMMAND ----------

### Download Previous last 30 days
download_num = 1
while (not file_exists('file:/tmp/Shopee_BOT_ProductPerformance/' + pattern_previous_last_30days)) and download_num <5:
  try:
    refresh_page()
    download_period_file(from_date=previous_last30days_begin, to_date=previous_last30days_end).execute()
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

# MAGIC %fs ls 'file:/tmp/Shopee_BOT_ProductPerformance/'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Moving & rename Previous Daily report

# COMMAND ----------

# path_from = "dbfs:/mnt/adls/staging/ecom/Brand_Portal-Business_Insights---Brand_Marketing---Flash_Sales_Performance-Gross_Data-2021.12.01_2021.12.31.xlsx"

# path_dest = "file:/tmp/Shopee_BOT_ProductPerformance/Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-2021.12.01_2021.12.31.xlsx"

# dbutils.fs.cp(path_from,path_dest)

# COMMAND ----------


destination_path = "dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/product_performance/"

### Path for previous files
download_folder = prefs["download.default_directory"]

if file_exists('dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/product_performance/' + pattern_previous_yesterday) == False:
  previous_yesterday_path = glob.glob(os.path.join(download_folder, pattern_previous_yesterday))[0]
else:
  pass

previous_last7days_path = glob.glob(os.path.join(download_folder, pattern_previous_last_7days))[0]
previous_last30days_path = glob.glob(os.path.join(download_folder, pattern_previous_last_30days))[0]
previous_mtd_path = glob.glob(os.path.join(download_folder, pattern_previous_mtd))[0]

### Destination for previous files 
previous_yesterday_destination = download_folder + "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-DailyOfftake_PreviousYesterday.xlsx"
previous_last7days_destination = download_folder + "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-DailyOfftake_PreviousLast7days.xlsx"
previous_last30days_destination = download_folder + "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-DailyOfftake_PreviousLast30days.xlsx"
previous_mtd_destination = download_folder + "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-DailyOfftake_PreviousMonthtodate.xlsx"

##### Copy yesterday file with new name and move original file to destination
if file_exists('dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/product_performance/' + pattern_previous_yesterday) == False:
  dbutils.fs.cp("file:" + previous_yesterday_path, "file:" + previous_yesterday_destination)
  print("Complete copy previous file to: "+ previous_yesterday_destination)
else:
  dbutils.fs.cp(destination_path + pattern_previous_yesterday, "file:" + previous_yesterday_destination)
##### Copy last 7 days and last 30 days file to destination
dbutils.fs.cp("file:" + previous_last7days_path, "file:" + previous_last7days_destination)

dbutils.fs.cp("file:" + previous_last30days_path, "file:" + previous_last30days_destination)

dbutils.fs.cp("file:" + previous_mtd_path, "file:" + previous_mtd_destination)

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

### MOVE daily Previous period offtake to folder
check_path = "mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_ProductPerformance/"
daily_offtake_path = "mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_DailyOfftake/product_performance/"

if file_exists(check_path + "shopee_product_performance_PreviousYesterday.snappy"):
  dbutils.fs.mv("/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_ProductPerformance/shopee_product_performance_PreviousYesterday.snappy", daily_offtake_path + "shopee_dailyofftake_PreviousYesterday.snappy")
  print("Move Previous yesterday file to Daily Offtake folder")
else:
  print("File shopee_product_performance_PreviousYesterday does not exist!")

if file_exists(check_path + "shopee_product_performance_PreviousLast7days.snappy"):
  dbutils.fs.mv("/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_ProductPerformance/shopee_product_performance_PreviousLast7days.snappy", daily_offtake_path + "shopee_dailyofftake_PreviousLast7days.snappy")
  print("Move Previous last 7 days file to Daily Offtake folder")
else:
  print("File shopee_product_performance_PreviousLast7days does not exist!")

if file_exists(check_path + "shopee_product_performance_PreviousLast30days.snappy"):
  dbutils.fs.mv("/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_ProductPerformance/shopee_product_performance_PreviousLast30days.snappy", daily_offtake_path + "shopee_dailyofftake_PreviousLast30days.snappy")
  print("Move Previous last 30 days file to Daily Offtake folder")
else:
  print("File shopee_product_performance_PreviousLast30days does not exist!")

if file_exists(check_path + "shopee_product_performance_PreviousMonthtodate.snappy"):
  dbutils.fs.mv("/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_ProductPerformance/shopee_product_performance_PreviousMonthtodate.snappy", daily_offtake_path + "shopee_dailyofftake_PreviousMonthtodate.snappy")
  print("Move Previous MTD file to Daily Offtake folder")
else:
  print("File shopee_product_performance_PreviousMonthtodate does not exist!")

# COMMAND ----------

# MAGIC %fs ls mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_DailyOfftake/product_performance
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove unnecessary files

# COMMAND ----------

### Remove previous daily report
dbutils.fs.rm("file:" + previous_last7days_path)
print("\n Complete rename previous last 7 days file to: "+ previous_last7days_destination)

dbutils.fs.rm("file:" + previous_last30days_path)
print("\n Complete rename previous last 30 days file to: " + previous_last30days_destination)

dbutils.fs.rm("file:" + previous_mtd_path)
print("\n Complete rename previous MTD file to: " + previous_mtd_destination)



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


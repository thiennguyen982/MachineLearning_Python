# Databricks notebook source
# MAGIC %run ./_init_sale_performance_setup

# COMMAND ----------

# MAGIC %md
# MAGIC #### Download daily report 

# COMMAND ----------

# Create vars for daily report
yesterday_name = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=1), '%Y.%m.%d')
last_7day_name = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=7), '%Y.%m.%d')
last_30day_name = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=30), '%Y.%m.%d')

last_7day_begin = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=7), '%Y-%m-%d')
last_30day_begin = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=30), '%Y-%m-%d')

mtd_begin = datetime.datetime.strftime((current_datetime - datetime.timedelta(days=1)+ datetime.timedelta(hours=7)).replace(day=1), '%Y-%m-%d')
mtd_begin_name = mtd_begin.replace("-", ".")

pattern_last_7days= "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx".format(last_7day_name, yesterday_name)
pattern_last_30days = "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx".format(last_30day_name, yesterday_name)
pattern_month_to_date = "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx".format(mtd_begin_name, yesterday_name)


# COMMAND ----------

### Download Last 7 day report
download_num = 1
while (not file_exists('file:/tmp/Shopee_BOT_SalesPerformance/' + pattern_last_7days)) and download_num <5:
  try:
    refresh_page()
    print("Num of download: " + str(download_num) +"\n")
    download_last7days()
#     download_files(from_date=last_7day_begin, to_date=yesterday).execute()  ### Alternative
    time.sleep(1)
    print("Download processing")
    download_num +=1
    print("Download failed. Try to download = " + str(download_num))
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      download_num +=1
      print("Download failed. Try to download = " + str(download_num))
    else:
      raise

# COMMAND ----------

### Download Last 30 day report
download_num = 1
while (not file_exists('file:/tmp/Shopee_BOT_SalesPerformance/' + pattern_last_30days)) and download_num <5:
  try:
    refresh_page()
    download_last30days()
#     download_files(from_date=last_30day_begin, to_date=yesterday).execute() ###Alternative
    time.sleep(1)
    print("Download processing")
    download_num +=1
    print("\n Download failed. Try to download = " + str(download_num))
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      download_num +=1
      print("\n Download failed. Try to download = " + str(download_num))
    else:
      raise

# COMMAND ----------

### Download MTD report
download_num = 1
while (not file_exists('file:/tmp/Shopee_BOT_SalesPerformance/' + pattern_month_to_date)) and download_num <5:
  try:
    refresh_page()
    if mtd_begin > yesterday:
      mtd_begin_fix = datetime.datetime.strftime((current_datetime - datetime.timedelta(1)).replace(day=1), '%Y-%m-%d')
      download_period_file(from_date=mtd_begin_fix, to_date=yesterday).execute()
    else:
      download_period_file(from_date=mtd_begin, to_date=yesterday).execute()

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

# MAGIC %fs ls "file:/tmp/Shopee_BOT_SalesPerformance"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Moving & rename Daily reports to destination 

# COMMAND ----------

#### Create path to save file
destination_path = "dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/sales_performance/"

download_folder = prefs["download.default_directory"]
last7days_path = glob.glob(os.path.join(download_folder, pattern_last_7days))[0]
last30days_path = glob.glob(os.path.join(download_folder, pattern_last_30days))[0]
month_to_date_path = glob.glob(os.path.join(download_folder, pattern_month_to_date))[0]


last7days_destination = download_folder + "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-DailyOfftake_last7days.xlsx"
last30days_destination = download_folder + "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-DailyOfftake_last30days.xlsx"
month_to_date_destination = download_folder + "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-DailyOfftake_monthtodate.xlsx"

# COMMAND ----------

### Create vars for daily report
yesterday_name = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=1), '%Y.%m.%d')
last_7day_name = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=7), '%Y.%m.%d')
last_30day_name = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=30), '%Y.%m.%d')

pattern_yesterday = "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx".format(yesterday_name, yesterday_name)
pattern_last_7days= "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx".format(last_7day_name, yesterday_name)
pattern_last_30days = "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx".format(last_30day_name, yesterday_name)

destination_path = "dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/sales_performance/"

download_folder = prefs["download.default_directory"]

yesterday_path = glob.glob(os.path.join(download_folder, pattern_yesterday))[0]
last7days_path = glob.glob(os.path.join(download_folder, pattern_last_7days))[0]
last30days_path = glob.glob(os.path.join(download_folder, pattern_last_30days))[0]
month_to_date_path = glob.glob(os.path.join(download_folder, pattern_month_to_date))[0]

last7days_destination = download_folder + "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-DailyOfftake_last7days.xlsx"
last30days_destination = download_folder + "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-DailyOfftake_last30days.xlsx"

# COMMAND ----------

### Create vars for daily report
yesterday_name = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=1), '%Y.%m.%d')

pattern_yesterday = "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx".format(yesterday_name, yesterday_name)

destination_path = "dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/sales_performance/"

download_folder = prefs["download.default_directory"]
yesterday_path = glob.glob(os.path.join(download_folder, pattern_yesterday))[0]

# COMMAND ----------

# Move/Rename last 7 days, last 30 days & MTD file to destination
dbutils.fs.cp("file:" + last7days_path, "file:" + last7days_destination)
print("\n Complete rename last 7 days file to: "+ last7days_destination)

dbutils.fs.cp("file:" + last30days_path, "file:" + last30days_destination)
print("\n Complete rename last 30 days file to: " + last30days_destination)

if month_to_date_path == yesterday_path:
  dbutils.fs.cp("file:" + yesterday_destination, "file:" + month_to_date_destination)
elif month_to_date_path == last7days_path:
  dbutils.fs.cp("file:" + last7days_destination, "file:" + month_to_date_destination)
elif month_to_date_path == last30days_path:
  dbutils.fs.cp("file:" + last30days_destination, "file:" + month_to_date_destination)
else:
  dbutils.fs.cp("file:" + month_to_date_path, "file:" + month_to_date_destination)
print("\n Complete rename Month to date file to: " + month_to_date_destination)

# COMMAND ----------

# MAGIC %fs ls file:/tmp/Shopee_BOT_SalesPerformance/

# COMMAND ----------

# MAGIC %md
# MAGIC #### Move downloaded files to Destination (ADLS)

# COMMAND ----------

### Move to destination
pattern = "Brand_Portal-Business_Insights---Sales_Performance*.xlsx"
destination = "dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/sales_performance/"

destination_files, file_name = move_to(pattern, destination)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cleaning data & write to destination folder

# COMMAND ----------

####### FOR MANUAL FIX ONLY #####
# name = "shopee_sales_performance_gross_2021_05_13" 
# date = "2021-05-13"
# exec( '{} = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "false").option("treatEmptyValuesAsNulls", "false").option("dataAddress","{}").load("dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/sales_performance/Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-2021.05.13_2021.05.13.xlsx")'.format(name, address) )qq
# exec( '{} = {}.withColumn("Date", lit("{}")).drop("No.").toPandas()'.format(name, name, date))
# exec( '{}.to_csv("/dbfs/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_SalesPerformance/{}.csv", index=False)'.format(name, name))
# print('Complete clean & write file: ' + name) 

# ["Product Name",	"Product ID", "Gross Sales(đ)", "Gross Orders",	"Gross Units Sold", "Gross Average Basket Size(đ)",	"Gross Average Selling Price(đ)", "Gross Product Views", "Gross Unique Visitors",	"Gross Item",  "Conversion Rate", "Date"]

# COMMAND ----------

# MAGIC %scala
# MAGIC // import shadeio.poi.openxml4j.util.ZipSecureFile
# MAGIC // ZipSecureFile.setMinInflateRatio(0)

# COMMAND ----------

# ADD DATE COLUMN FOR DATA
from pyspark.sql.functions import lit

all_item_path = destination_files
all_item_name = [file_name[i] + '.xlsx' for i in range(0, len(file_name))]
regex = "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-(.*)_(.*).xlsx"


names = []
dates = []

#flags required for reading the excel
isHeaderOn = "true"
isInferSchemaOn = "false"
#sheet address in excel
address = "'Product Ranking'!A1"

for item in all_item_name:
  group1 = re.search(regex, item).group(1).replace('.', '_')
  group2 = re.search(regex, item).group(2).replace('.', '_')
  name = "shopee_sales_performance_gross_" + re.search(regex, item).group(2).replace('.', '_')
  if (group1 == group2) or (group1 == 'DailyOfftake'):
    try:
        date = re.search(regex, item).group(2).replace('.', '-')
        names.append(name)
        dates.append(date)
        exec( '{} = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "false").option("treatEmptyValuesAsNulls", "false").option("dataAddress","{}").load("dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/sales_performance/{}")'.format(name, address, item) )
        exec( '{} = {}.withColumn("Date", lit("{}")).drop("No.").toPandas()'.format(name, name, date))
        ### Rename column for consistency (NOTE: column "Gross Unique Visitors" is changed to "Gross Product Visitors")
        exec( '{} = {}.rename(columns = {{"Gross Product Visitors": "Gross Unique Visitors"}})'.format(name, name) )
        ### Write data to csv
        if not eval(name).empty:
            exec( '{}.to_csv("/dbfs/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_SalesPerformance/{}.csv", index=False)'.format(name, name))
            print('Complete clean & write file: ' + name)
        else:
            print('\n File ' + name + ' is empty ==> SKIP SAVING \n')
    except:
#         raise
        print("Error at: " + name)

# COMMAND ----------

# MOVE daily sales performances to folder
check_path = "mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_SalesPerformance/"
daily_offtake_path = "mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_DailyOfftake/sales_performance/"

if file_exists(check_path + "shopee_sales_performance_gross_last7days.csv"):
  dbutils.fs.mv("/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_SalesPerformance/shopee_sales_performance_gross_last7days.csv", daily_offtake_path + "shopee_sales_performance_gross_last7days.csv")
  print("Move last 7 days file to Daily Offtake folder")
else:
  print("No exist file")

if file_exists(check_path + "shopee_sales_performance_gross_last30days.csv"):
  dbutils.fs.mv("/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_SalesPerformance/shopee_sales_performance_gross_last30days.csv", daily_offtake_path + "shopee_sales_performance_gross_last30days.csv")
  print("Move last 30 days file to Daily Offtake folder")
else:
  print("No exist file")

if file_exists(check_path + "shopee_sales_performance_gross_monthtodate.csv"):
  dbutils.fs.mv("/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_SalesPerformance/shopee_sales_performance_gross_monthtodate.csv", daily_offtake_path + "shopee_sales_performance_gross_monthtodate.csv")
  print("Move MTD file to Daily Offtake folder")
else:
  print("No exist file")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Remove uncessary files

# COMMAND ----------

### Remove daily report
dbutils.fs.rm("file:" + last7days_path)
dbutils.fs.rm("file:" + last30days_path)
dbutils.fs.rm("file:" + month_to_date_path)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Download customized data (MANUAL)

# COMMAND ----------

# ### DOWNLOAD CUSTOMISED DATA
# refresh_page()
# # print("Time of download: " + str(download_num) +"\n")

# from_date = '2022-09-01'
# to_date = '2022-09-01'

# pattern_custom_date = "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx".format(from_date.replace("-", "."), to_date.replace("-", "."))

# download_num = 1
# while (not file_exists('file:/tmp/Shopee_BOT_SalesPerformance/' + pattern_custom_date)) and download_num <5:
#   try:
#     download_period_file(from_date=from_date, to_date=to_date).execute() ### Download period
# #     download_files(from_date = from_date, to_date = to_date).execute() ### Download seperate files
#     time.sleep(1)
#     download_num +=1
#     print("Download processing ...")
#   except Exception as e:
#     if 'java.io.FileNotFoundException' in str(e):
#       download_num +=1
#       print("Download failed. Try to download = " + str(download_num))
#     else:
#       raise
      
# # Move to destination
# destination = "dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/sales_performance/"

# destination_files, file_name = move_to(pattern_custom_date, destination)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Screenshot to debug

# COMMAND ----------

# ### Screenshot
# from Screenshot import Screenshot

# ob = Screenshot.Screenshot()
# img_url = ob.full_Screenshot(browser, save_path=r"/tmp/Shopee_BOT_SalesPerformance/",image_name="screenshot.jpg")

# dbutils.fs.cp("file:/tmp/Shopee_BOT_SalesPerformance/screenshot.jpg", "dbfs:/FileStore/screenshot.jpg")

# COMMAND ----------

# %md
# ![screenshot_img](files/screenshot.jpg)

# COMMAND ----------


# Databricks notebook source
# MAGIC %run ./_init_sale_performance_setup

# COMMAND ----------

# MAGIC %md
# MAGIC #### Download previous daily report

# COMMAND ----------

# Create date period vars
current_datetime = datetime.datetime.today() + datetime.timedelta(hours=7)
current_date = datetime.datetime.strftime(current_datetime, '%Y-%m-%d')
yesterday = datetime.datetime.strftime(current_datetime - datetime.timedelta(days=1), '%Y-%m-%d')
yesterday_name = yesterday.replace("-", ".")
previous_yesterday = datetime.datetime.strftime(current_datetime - datetime.timedelta(days=2), '%Y-%m-%d')

previous_last7days_begin = datetime.datetime.strftime(current_datetime - datetime.timedelta(days=14), '%Y-%m-%d')
previous_last7days_end = datetime.datetime.strftime(current_datetime - datetime.timedelta(days=8), '%Y-%m-%d')

previous_last30days_begin = datetime.datetime.strftime(current_datetime - datetime.timedelta(days=60), '%Y-%m-%d')
previous_last30days_end = datetime.datetime.strftime(current_datetime - datetime.timedelta(days=31), '%Y-%m-%d')

# COMMAND ----------

# Define previous mtd period
last_day_of_prev_month = current_datetime.replace(day=1) - datetime.timedelta(days=1)

previous_mtd_begin = datetime.datetime.strftime(last_day_of_prev_month.replace(day=1), '%Y-%m-%d')

if (last_day_of_prev_month.year == current_datetime.year) and (last_day_of_prev_month.day > (current_datetime - datetime.timedelta(days=1)).day): # When last month and current month in the same year
    previous_mtd_end = datetime.datetime.strftime((current_datetime - datetime.timedelta(days=1)).replace(month=current_datetime.month -1), '%Y-%m-%d')
# elif (last_day_of_prev_month.year < current_datetime.year) and (last_day_of_prev_month.day > (current_datetime - datetime.timedelta(days=1)).day): ## When last month is in last year
#     previous_mtd_end = datetime.datetime.strftime((current_datetime - datetime.timedelta(days=1)).replace(month=12), '%Y-%m-%d')
else:
    previous_mtd_end = datetime.datetime.strftime(last_day_of_prev_month, '%Y-%m-%d')

# COMMAND ----------

# Create vars for previous period report
previous_yesterday_name = previous_yesterday.replace('-','.')

previous_last7days_name_begin = previous_last7days_begin.replace('-','.')
previous_last7days_name_end = previous_last7days_end.replace('-','.')

previous_last30days_name_begin = previous_last30days_begin.replace('-','.')
previous_last30days_name_end = previous_last30days_end.replace('-','.')

previous_monthtodate_name_begin = previous_mtd_begin.replace('-','.')
previous_monthtodate_name_end = previous_mtd_end.replace('-','.')

pattern_previous_yesterday = "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx".format(previous_yesterday_name, previous_yesterday_name)

pattern_previous_last_7days= "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx".format(previous_last7days_name_begin, previous_last7days_name_end)

pattern_previous_last_30days = "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx".format(previous_last30days_name_begin, previous_last30days_name_end)

pattern_previous_monthtodate = "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx".format(previous_monthtodate_name_begin, previous_monthtodate_name_end)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Previous Yesterday

# COMMAND ----------

"""Download previous reports"""

### Download Previous Yesterday
if file_exists('dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/sales_performance/' + pattern_previous_yesterday) == False:
  download_num = 1
  while (not file_exists('file:/tmp/Shopee_BOT_SalesPerformance/' + pattern_previous_yesterday)) and download_num <5:
    try:
      download_period_file(from_date=previous_yesterday, to_date=previous_yesterday).execute()
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

### Download Previous Last 7 day
download_num = 1
while (not file_exists('file:/tmp/Shopee_BOT_SalesPerformance/' + pattern_previous_last_7days)) and download_num <5:
  try:
    refresh_page()
    print("Time of download: " + str(download_num) +"\n")
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

### Download Previous Last 30 day
download_num = 1
while (not file_exists('file:/tmp/Shopee_BOT_SalesPerformance/' + pattern_previous_last_30days)) and download_num <5:
  try:
    refresh_page()
    print("Time of download: " + str(download_num) +"\n")
    download_period_file(from_date=previous_last30days_begin, to_date=previous_last30days_end).execute()
    time.sleep(1)
    download_num +=1
    print("Download processing")
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      download_num += 1
      print("Download failed. Try to download = " + str(download_num))
    else:
      raise

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Previous MTD

# COMMAND ----------

### Check previous MTD file history
if file_exists('dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/sales_performance/' + pattern_previous_monthtodate):
  dbutils.fs.cp('dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/sales_performance/' + pattern_previous_monthtodate, "file:/tmp/Shopee_BOT_SalesPerformance/" + pattern_previous_monthtodate)
else:
  print("Previous MTD file does not exist")
  
### Download MTD previous period reports

download_num = 1
while (not file_exists('file:/tmp/Shopee_BOT_SalesPerformance/' + pattern_previous_monthtodate)) and download_num <5:
  try:
    refresh_page()
    print("Time of download: " + str(download_num) +"\n")
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

# MAGIC
# MAGIC %fs ls "file:/tmp/Shopee_BOT_SalesPerformance"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Moving & rename Previous Daily report

# COMMAND ----------

#### Create path to save file
destination_path = "dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/sales_performance/"

download_folder = prefs["download.default_directory"]

if file_exists('dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/sales_performance/' + pattern_previous_yesterday) == False:
  previous_yesterday_path = glob.glob(os.path.join(download_folder, pattern_previous_yesterday))[0]
else: 
  pass

previous_last7days_path = glob.glob(os.path.join(download_folder, pattern_previous_last_7days))[0]
previous_last30days_path = glob.glob(os.path.join(download_folder, pattern_previous_last_30days))[0]
previous_monthtodate_path = glob.glob(os.path.join(download_folder, pattern_previous_monthtodate))[0]

previous_yesterday_destination = download_folder + "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-DailyOfftake_PreviousYesterday.xlsx"
previous_last7days_destination = download_folder + "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-DailyOfftake_PreviousLast7days.xlsx"
previous_last30days_destination = download_folder + "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-DailyOfftake_PreviousLast30days.xlsx"
previous_monthtodate_destination = download_folder + "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-DailyOfftake_PreviousMonthtodate.xlsx"

# COMMAND ----------

# Rename previous yesterday to destination
if file_exists('dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/sales_performance/' + pattern_previous_yesterday) == False:
  dbutils.fs.cp("file:" + previous_yesterday_path, "file:" + previous_yesterday_destination)
  print("Complete rename previous yesterday file to: "+ previous_yesterday_destination)
else: 
  dbutils.fs.cp(destination_path + pattern_previous_yesterday, "file:" + previous_yesterday_destination)
  print("\n Complete COPY previous last 7 days file to: "+ previous_last7days_destination)

### Move/Rename previous last 7 days, previous last 30 days & previous MTD file to destination
dbutils.fs.cp("file:" + previous_last7days_path, "file:" + previous_last7days_destination)
print("\n Complete rename previous last 7 days file to: "+ previous_last7days_destination)

dbutils.fs.cp("file:" + previous_last30days_path, "file:" + previous_last30days_destination)
print("\n Complete rename previous last 30 days file to: " + previous_last30days_destination)

dbutils.fs.cp("file:" + previous_monthtodate_path, "file:" + previous_monthtodate_destination)
print("\n Complete rename Previous Month to date file to: " + previous_monthtodate_destination)

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

check_path = "mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_SalesPerformance/"
daily_offtake_path = "mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_DailyOfftake/sales_performance/"

# MOVE previous daily sales performances to folder
if file_exists(check_path + "shopee_sales_performance_gross_PreviousYesterday.csv"):
  dbutils.fs.mv("/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_SalesPerformance/shopee_sales_performance_gross_PreviousYesterday.csv", daily_offtake_path + "shopee_sales_performance_gross_PreviousYesterday.csv")
  print("Move previous yesterday file to Daily Offtake folder")
else:
  print("No exist file")

if file_exists(check_path + "shopee_sales_performance_gross_PreviousLast7days.csv"):
  dbutils.fs.mv("/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_SalesPerformance/shopee_sales_performance_gross_PreviousLast7days.csv", daily_offtake_path + "shopee_sales_performance_gross_PreviousLast7days.csv")
  print("Move previous last 7 days file to Daily Offtake folder")
else:
  print("No exist file")

if file_exists(check_path + "shopee_sales_performance_gross_PreviousLast30days.csv"):
  dbutils.fs.mv("/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_SalesPerformance/shopee_sales_performance_gross_PreviousLast30days.csv", daily_offtake_path + "shopee_sales_performance_gross_PreviousLast30days.csv")
  print("Move previous last 30 days file to Daily Offtake folder")
else:
  print("No exist file")

if file_exists(check_path + "shopee_sales_performance_gross_PreviousMonthtodate.csv"):
  dbutils.fs.mv("/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_SalesPerformance/shopee_sales_performance_gross_PreviousMonthtodate.csv", daily_offtake_path + "shopee_sales_performance_gross_PreviousMonthtodate.csv")
  print("Move Previous MTD file to Daily Offtake folder")
else:
  print("No exist file")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Remove uncessary files

# COMMAND ----------

### Remove previous daily report
dbutils.fs.rm("file:" + previous_last7days_path)
dbutils.fs.rm("file:" + previous_last30days_path)
dbutils.fs.rm("file:" + previous_monthtodate_path)


# COMMAND ----------

# ### DOWNLOAD CUSTOMISED DATA
# refresh_page()
# # print("Time of download: " + str(download_num) +"\n")

# from_date = '2022-09-01'
# to_date = '2022-09-02'

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

# ### Screenshot
# from Screenshot import Screenshot

# ob = Screenshot.Screenshot()
# dbutils.fs.rm("dbfs:/FileStore/screenshot.jpg")

# img_url = ob.full_Screenshot(browser, save_path=r"/tmp/Shopee_BOT_SalesPerformance/",image_name="screenshot.jpg")
# dbutils.fs.cp("file:/tmp/Shopee_BOT_SalesPerformance/screenshot.jpg", "dbfs:/FileStore/screenshot1.jpg")

# COMMAND ----------

# %md
# ![screenshot_img](files/screenshot1.jpg)

# COMMAND ----------


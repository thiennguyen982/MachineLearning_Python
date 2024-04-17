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

pattern_yesterday = "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx".format(yesterday_name, yesterday_name)


# COMMAND ----------

"""Download Daily download Dashboard report"""
## Download Yesterday report
download_num = 1
while (not file_exists('file:/tmp/Shopee_BOT_SalesPerformance/' + pattern_yesterday)) and download_num <5:
  try:
    refresh_page()
    print("Time of download: " + str(download_num) +"\n")
    download_yesterday()
#     download_files(from_date=yesterday, to_date=yesterday).execute() ### Alternative
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

# MAGIC %fs ls "file:/tmp/Shopee_BOT_SalesPerformance"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Moving & rename Yesterday reports to destination 

# COMMAND ----------

#### Create path to save file
destination_path = "dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/sales_performance/"

download_folder = prefs["download.default_directory"]
yesterday_path = glob.glob(os.path.join(download_folder, pattern_yesterday))[0]

yesterday_destination = download_folder + "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-DailyOfftake_yesterday.xlsx"

# COMMAND ----------

### Create vars for daily report
yesterday_name = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=1), '%Y.%m.%d')

pattern_yesterday = "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx".format(yesterday_name, yesterday_name)

destination_path = "dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/sales_performance/"

download_folder = prefs["download.default_directory"]
yesterday_path = glob.glob(os.path.join(download_folder, pattern_yesterday))[0]


yesterday_destination = download_folder + "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-DailyOfftake_yesterday.xlsx"

# COMMAND ----------

yesterday_path

# COMMAND ----------

# Copy yesterday file with new name and move original file to destination
dbutils.fs.cp("file:" + yesterday_path, "file:" + yesterday_destination)
print("Complete copy file to: "+ yesterday_destination)


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

# MOVE yesterday sales performances to folder
check_path = "mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_SalesPerformance/"
daily_offtake_path = "mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_DailyOfftake/sales_performance/"

if file_exists(check_path + "shopee_sales_performance_gross_yesterday.csv"):
  dbutils.fs.mv("/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_SalesPerformance/shopee_sales_performance_gross_yesterday.csv", daily_offtake_path + "shopee_sales_performance_gross_yesterday.csv")
  print("Move yesterday file to Daily Offtake folder")
else:
  print("No exist file")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Download customized data (MANUAL)

# COMMAND ----------

# ### DOWNLOAD CUSTOMISED DATA
# refresh_page()
# # print("Time of download: " + str(download_num) +"\n")

# from_date = '2022-08-25'
# to_date = '2022-08-25'

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


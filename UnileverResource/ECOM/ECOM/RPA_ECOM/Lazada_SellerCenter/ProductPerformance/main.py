# Databricks notebook source
dbutils.widgets.text(
    name = 'credential_email',
    defaultValue = 'view.unileverpremium2@gmail.com',
    label = 'Credential Email'
)
dbutils.widgets.text(
    name = 'credential_password',
    defaultValue = '',
    label = 'Credential Password'
)
dbutils.widgets.text(
    name = 'date',
    defaultValue = '',
    label = 'Date'
)
dbutils.widgets.text(
    name = 'store_name',
    defaultValue = '',
    label = 'Store Name'
)
dbutils.widgets.text(
    name = 'to_date',
    defaultValue = '',
    label = 'To Date'
)

CREDENTIAL_EMAIL = dbutils.widgets.get('credential_email')
CREDENTIAL_PASSWORD = dbutils.widgets.get('credential_password')
CURRENT_DATE = dbutils.widgets.get('date')
STORE_NAME = dbutils.widgets.get('store_name')
TO_DATE = dbutils.widgets.get('to_date')

# COMMAND ----------

# MAGIC %run ../../utilities/file_ultilities

# COMMAND ----------

chrome_driver_path = dbutils.notebook.run('../../utilities/CHROME_DRIVER_SETUP', 300)

# COMMAND ----------

# MAGIC %run ../common_functions

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# from selenium import webdriver
import undetected_chromedriver as webdriver

# Create folder for download
if not path_exists(f'file:{PRODUCT_PERFORMANCE_TMP_DIR}'):
    print("Create new folder: PRODUCT_PERFORMANCE_TMP_DIR")
    dbutils.fs.mkdirs(f"file:{PRODUCT_PERFORMANCE_TMP_DIR}")
else:
    print("Download folder ready: PRODUCT_PERFORMANCE_TMP_DIR")
    print('Delete files inside')
    for i in dbutils.fs.ls(f'file:{PRODUCT_PERFORMANCE_TMP_DIR}'):
      dbutils.fs.rm(i[0])

# Init chrome browser
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--headless')
chrome_options.add_argument("window-size=1920,1080")
chrome_options.add_argument('--auto-open-devtools-for-tabs')

prefs = {
    "download.prompt_for_download": False,
    "download.default_directory": PRODUCT_PERFORMANCE_TMP_DIR,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": False,
    "safebrowsing.disable_download_protection": True
}
chrome_options.add_experimental_option("prefs", prefs)

print('Starting Browser')
browser = webdriver.Chrome(executable_path=chrome_driver_path, options=chrome_options, use_subprocess=True, version_main=106)

params = {
    "behavior": "allow",
    "downloadPath": PRODUCT_PERFORMANCE_TMP_DIR
}
browser.execute_cdp_cmd("Page.setDownloadBehavior", params)
# browser.maximize_window()

# login
credentials = {
    "email": CREDENTIAL_EMAIL,
    "pass": CREDENTIAL_PASSWORD
}
url = 'https://sellercenter.lazada.vn/apps/seller/login'
print('Get URL')
browser.get(url)
login_ISR(browser, chrome_options, 30, credentials)
# wait_for_element_located(browser, DA_PAGE)
check = 'Sucessfully login!'
print(check)

# COMMAND ----------

from datetime import datetime, timedelta
import time

# Check current date
if CURRENT_DATE == '':
  CURRENT_DATE = datetime.now() + timedelta(hours=7)

  print(f'Running extract data at date {CURRENT_DATE}')

  jobs = {
      'yesterday': {
          'dateType': 'recent1',
          'from_date': CURRENT_DATE - timedelta(days=1),
          'to_date': CURRENT_DATE - timedelta(days=1)
      },
      'last7days': {
          'dateType': 'recent7',
          'from_date': CURRENT_DATE - timedelta(days=7),
          'to_date': CURRENT_DATE - timedelta(days=1)
      },
      'last30days': {
          'dateType': 'recent30',
          'from_date': CURRENT_DATE - timedelta(days=30),
          'to_date': CURRENT_DATE - timedelta(days=1)
      }
  }
else:
  curr_date = datetime.strptime(CURRENT_DATE, '%d-%m-%Y')
  end_date = datetime.strptime(TO_DATE, '%d-%m-%Y')
  jobs = {}
  while curr_date <= end_date:
      jobs[f"specificed_date_{curr_date.strftime('%Y_%m_%d')}"] = {
          'dateType': 'day',
          'from_date': curr_date,
          'to_date': curr_date
      }
      curr_date = curr_date + timedelta(days=1)

# COMMAND ----------

import re
from pyspark.sql.functions import lit

def run_job(browser, pre_url, job_name, from_date, to_date, date_type):
    print(f'Running job {job_name}')
    downloaded_file_path = download_data(browser, pre_url, from_date, to_date, date_type, PRODUCT_PERFORMANCE_TMP_DIR, PRODUCT_PERFORMANCE_TMP_DIR)
    if downloaded_file_path == None:
        raise Exception(f'Download failed with job {job_name}: {from_date} - {to_date}')
    # read file
    try:
        if 'specificed_date' not in job_name and job_name != 'yesterday':
          save_path = f"dbfs:/mnt/adls/staging/lazada/lazada_daily/Offtake/{STORE_NAME}/lazada_{STORE_NAME}_DailyOfftake_{job_name}.xls"
        else:
          save_path = f"dbfs:/mnt/adls/staging/lazada/lazada_daily/Offtake/{STORE_NAME}/lazada_{STORE_NAME}_DailyOfftake_{from_date.strftime('%Y_%m_%d')}.xls"
        # Copy raw file
        print(f'Moving file from downloaded path {downloaded_file_path} to {save_path}')
        dbutils.fs.mv(f'{downloaded_file_path}', save_path)

        address1 = "'Sản phẩm'!A1"
        df = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "false").option("treatEmptyValuesAsNulls", "false").option("dataAddress",f"{address1}").load(save_path)
        info = df.columns[0]
#         regex = ".*:.*(.*)_(.*)"
        regex = "\xa0(.*)_(.*)"
        from_date = re.search(regex, info).group(1).strip()
        to_date = re.search(regex, info).group(2).strip()
      
        print("From date: " + from_date)
        print("To date: " + to_date)
        
        address = "'Sản phẩm'!A6"
        lazada_dailyofftake_df = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "false").option("treatEmptyValuesAsNulls", "false").option("dataAddress",f"{address}").load(save_path)
        lazada_dailyofftake_df = lazada_dailyofftake_df.withColumn("Date", lit(job_name))
        lazada_dailyofftake_df = lazada_dailyofftake_df.toPandas()

        lazada_dailyofftake_df = lazada_dailyofftake_df.rename(columns={
          "Tên sản phẩm": "product_name", 
          "Seller SKU": "seller_sku", 
          "SKU ID": "sku_id", 
          "Khách truy cập (Định nghĩa mới)": "sku_visitors",
          "Xem trang sản phẩm (Định nghĩa mới)": "sku_views",
          "Giá trị khách truy cập (Định nghĩa mới)": "visitor_value",
          "Thêm vào khách truy cập xe đẩy": "a2c_visitors",
          "Thêm vào các đơn vị giỏ hàng": "a2c_units",
          "Tỷ lệ khách thêm vào giỏ hàng (định nghĩa mới)": "a2c_rate",
          "Người dùng danh sách mong muốn": "Wishlist_Visitors",
          "Danh sách mong muốn": "Wishlists",
          "Lượt mua": "buyers",
          "Đơn hàng": "orders",
          "Sản phẩm bán được": "units_Sold",
          "Doanh thu": "GMV",
          "Tỷ lệ chuyển đổi (định nghĩa mới)": "CR",
          "Doanh thu cho mỗi người mua": "GMV_per_buyer"
        })
        if 'specificed_date' not in job_name:
          lazada_dailyofftake_df.to_csv(f"/dbfs/mnt/adls/staging/ecom/BOT/Lazada/SellerCenter_DailyOfftake/lazada_{STORE_NAME}_dailyofftake_{job_name}.csv", index=False, encoding="utf-8-sig")

        if from_date.strip() == to_date.strip():
          print('Add-in process for yesterday data')
          # store the file by daily
#           daily_path = f"/dbfs/mnt/adls/staging/lazada/lazada_daily/Offtake/{STORE_NAME}/lazada_{STORE_NAME}_DailyOfftake_{from_date.replace('-', '_')}.xls"
#           print(f'Moving file from {save_path} to {daily_path}')
#           dbutils.fs.cp(save_path, daily_path)
          date_str = from_date.replace('-', '_')
          lazada_dailyofftake_df['Date'] = from_date
          lazada_dailyofftake_df.to_csv(f"/dbfs/mnt/adls/staging/ecom/BOT/Lazada/SellerCenter_ProductPerformance/{STORE_NAME}/lazada_{STORE_NAME}_dailyofftake_{date_str}.csv", index=False, encoding="utf-8-sig")
        
        print(f'Completed run job {job_name}')
        print(f'Cleaning file at path file:{PRODUCT_PERFORMANCE_TMP_DIR}')
        for i in dbutils.fs.ls(f'file:{PRODUCT_PERFORMANCE_TMP_DIR}'):
          dbutils.fs.rm(i[0])
    except Exception as e:
        print(f'Failed to run job at {job_name}')
        raise e

# COMMAND ----------

import time
from Screenshot import Screenshot

try:
  print('Start job...')
  for job in jobs:
      retry_count = 1
      job_detail = jobs[job]
      while retry_count <= 3:
          try:
              run_job(browser, 'https://sellercenter.lazada.vn/ba/product/performance', job, job_detail['from_date'], job_detail['to_date'], job_detail['dateType'])
              retry_count = 999
          except Exception as e:
              print(e)
              print(f'Failed with retry {retry_count}')
              retry_count += 1
              if retry_count > 3:
                  raise e
              print(f'Try again!')
              time.sleep(60)

      time.sleep(10) # wait for 10 sec
except Exception as e:
  ob = Screenshot.Screenshot()
  img_url = ob.full_Screenshot(browser, save_path="/tmp/",image_name="screenshot.jpg")
  dbutils.fs.mv(f"file:/tmp/screenshot.jpg", f"dbfs:/FileStore/Lazada_screenshot{datetime.now().strftime('%Y%m%d-%H%M%S')}.jpg")
  raise e


# COMMAND ----------


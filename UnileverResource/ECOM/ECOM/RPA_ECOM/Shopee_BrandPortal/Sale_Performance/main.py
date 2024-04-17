# Databricks notebook source
# MAGIC %md Notebook parameters
# MAGIC

# COMMAND ----------

dbutils.widgets.text(
    name = 'credential_email',
    defaultValue = 'nguyen-ngoc.hanh@unilever.com',
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
    name = 'to_date',
    defaultValue = '',
    label = 'To Date'
)

CREDENTIAL_EMAIL = dbutils.widgets.get('credential_email')
CREDENTIAL_PASSWORD = dbutils.widgets.get('credential_password')
CURRENT_DATE = dbutils.widgets.get('date')
TO_DATE = dbutils.widgets.get('to_date')

# COMMAND ----------

chrome_driver_path = dbutils.notebook.run('../../utilities/CHROME_DRIVER_SETUP', 300)

# COMMAND ----------

# MAGIC %run ../../utilities/file_ultilities

# COMMAND ----------

# MAGIC %run ../common_functions

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# import selenium
from selenium import webdriver

# COMMAND ----------

# Create folder for download
if not path_exists(f'file:{SALE_PERFORMANCE_TMP_DIR}'):
    print("Create new folder: Shopee_BOT_SalesPerformance")
    dbutils.fs.mkdirs(f"file:{SALE_PERFORMANCE_TMP_DIR}")
else:
    print("Download folder ready: Shopee_BOT_SalesPerformance")

# Init chrome browser
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--headless')
chrome_options.add_argument("window-size=1920,1080")
chrome_options.add_argument('--auto-open-devtools-for-tabs')

prefs = {
    "download.prompt_for_download": False,
    "download.default_directory": SALE_PERFORMANCE_TMP_DIR,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": False,
    "safebrowsing.disable_download_protection": True
}
chrome_options.add_experimental_option("prefs", prefs)

browser = webdriver.Chrome(executable_path=chrome_driver_path, options=chrome_options)

params = {
    "behavior": "allow",
    "downloadPath": SALE_PERFORMANCE_TMP_DIR
}
browser.execute_cdp_cmd("Page.setDownloadBehavior", params)

browser.maximize_window()

# COMMAND ----------

# login
credentials = {
    "email": CREDENTIAL_EMAIL,
    "pass": CREDENTIAL_PASSWORD
}
url = 'https://brandportal.shopee.com/seller/login'
browser.get(url)
login_ISR(browser, chrome_options, 30, credentials)

# COMMAND ----------

from pyspark.sql.functions import lit

def run_job(job_name, from_date, to_date):
    print(f'Running job {job_name}')
    downloaded_file_path = download_data(from_date, to_date, SALE_PERFORMANCE_TMP_DIR, SALE_PERFORMANCE_FILE_PATTERN)
    if downloaded_file_path == None:
        raise Exception(f'Download failed with job {job_name}: {from_date} - {to_date}')
    # read file
    try:
        address = "'Product Ranking'!A1"
        data_df = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "false").option("treatEmptyValuesAsNulls", "false").option("dataAddress",f"{address}").load(downloaded_file_path)
#         data_df = data_df.withColumn("Date", lit(f"{to_date.strftime('%Y-%m-%d')}")).drop("No.").toPandas()
        data_df = data_df.withColumn("Date", lit(job_name)).drop("No.").toPandas()
        data_df = data_df.rename(columns = {
            "Gross Product Visitors": "Gross Unique Visitors"
        })
        if job_name == 'yesterday' or 'specificed_date' in job_name:
            yesterday_df = data_df.copy()
            yesterday_df['Date'] = to_date.strftime('%Y-%m-%d')
            yesterday_df.to_csv(
                f"/dbfs/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_SalesPerformance/shopee_sales_performance_gross_{to_date.strftime('%Y_%m_%d')}.csv"
                ,index=False)
        
        output_path = f"/dbfs/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_DailyOfftake/sales_performance/shopee_sales_performance_gross_{job_name}.csv"
        if 'specificed_date' not in job_name:
            print(f'Writing file at path {output_path}')
            data_df.to_csv(output_path, index=False)
            
#         output_path = f"/dbfs/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_SalesPerformance/shopee_sales_performance_gross_{job_name}.csv"
#         print(f'Writing file at path {output_path}')
#         data_df.to_csv(output_path, index=False)
        print(f'Completed run job {job_name}')
    except Exception as e:
        print(f'Failed to run job at {job_name}')
        raise e

# COMMAND ----------

from datetime import datetime, timedelta
import time
from Screenshot import Screenshot

# Check current date
if CURRENT_DATE == '':
    CURRENT_DATE = datetime.now() + timedelta(hours=7)

    print(f'Running extract data at date {CURRENT_DATE}')

    # Define previous mtd period
    last_day_of_prev_month = CURRENT_DATE.replace(day=1) - timedelta(days=1)

    previous_mtd_begin = last_day_of_prev_month.replace(day=1)

    if (last_day_of_prev_month.year == CURRENT_DATE.year) and (last_day_of_prev_month.day > (CURRENT_DATE - timedelta(days=1)).day): # When last month and current month in the same year
        previous_mtd_end = (CURRENT_DATE - timedelta(days=1)).replace(month=CURRENT_DATE.month -1)
    # elif (last_day_of_prev_month.year < current_datetime.year) and (last_day_of_prev_month.day > (current_datetime - datetime.timedelta(days=1)).day): ## When last month is in last year
    #     previous_mtd_end = datetime.datetime.strftime((current_datetime - datetime.timedelta(days=1)).replace(month=12), '%Y-%m-%d')
    else:
        previous_mtd_end = last_day_of_prev_month

    jobs = {
        'yesterday': {
            'from_date': CURRENT_DATE - timedelta(days=1),
            'to_date': CURRENT_DATE - timedelta(days=1)
        },
        'last7days': {
            'from_date': CURRENT_DATE - timedelta(days=7),
            'to_date': CURRENT_DATE - timedelta(days=1)
        },
        'last30days': {
            'from_date': CURRENT_DATE - timedelta(days=30),
            'to_date': CURRENT_DATE - timedelta(days=1)
        },
        'monthtodate': {
            'from_date': (CURRENT_DATE - timedelta(days=1)).replace(day=1),
            'to_date': CURRENT_DATE - timedelta(days=1)
        },
        'PreviousYesterday': {
            'from_date': CURRENT_DATE - timedelta(days=2),
            'to_date': CURRENT_DATE - timedelta(days=2)
        },
        'PreviousLast7days': {
            'from_date': CURRENT_DATE - timedelta(days=14),
            'to_date': CURRENT_DATE - timedelta(days=8)
        },
        'PreviousLast30days': {
            'from_date': CURRENT_DATE - timedelta(days=60),
            'to_date': CURRENT_DATE - timedelta(days=31)
        },
        'PreviousMonthtodate': {
            'from_date': previous_mtd_begin,
            'to_date': previous_mtd_end
        }
    }
else:
    curr_date = datetime.strptime(CURRENT_DATE, '%d-%m-%Y')
    end_date = datetime.strptime(TO_DATE, '%d-%m-%Y')
    jobs = {}
    while curr_date <= end_date:
        jobs[f"specificed_date_{curr_date.strftime('%Y_%m_%d')}"] = {
            'from_date': curr_date,
            'to_date': curr_date
        }
        curr_date = curr_date + timedelta(days=1)

browser.get('https://brandportal.shopee.com/seller/insights/sales/shop')
for job in jobs:
    retry_count = 1
    job_detail = jobs[job]
    while retry_count <= 3:
        try:
            run_job(job, job_detail['from_date'], job_detail['to_date'])
            retry_count = 999
        except Exception as e:
            print(f'Failed with retry {retry_count}')
            browser.get('https://brandportal.shopee.com/seller/insights/sales/shop')
            retry_count += 1
            if retry_count > 3:
                ob = Screenshot.Screenshot()
                img_url = ob.full_Screenshot(browser, save_path="/tmp/",image_name="screenshot.jpg")
                dbutils.fs.mv(f"file:/tmp/screenshot.jpg", f"dbfs:/FileStore/Shopee_SalePerformance_screenshot{datetime.now().strftime('%Y%m%d-%H%M%S')}.jpg")
                raise e
            print(f'Try again!')
            time.sleep(60)
            
    time.sleep(90) # wait for 1 min 30 sec

# COMMAND ----------


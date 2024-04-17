# Databricks notebook source
# MAGIC %run ./Chrome_setup_utility

# COMMAND ----------

# MAGIC %run ./libraries_utility

# COMMAND ----------

# MAGIC %run ./Login_function

# COMMAND ----------

# MAGIC %run ./From_To_Date_selection_method

# COMMAND ----------

# MAGIC %run ./file_checks_utility

# COMMAND ----------

# MAGIC %run ./marketing_solutions_utility

# COMMAND ----------

# MAGIC %run ./sales_performance_utility

# COMMAND ----------

# MAGIC %run ./move_file_utility

# COMMAND ----------

# MAGIC %run ./delete_file

# COMMAND ----------

#dbutils.widgets.text("Dataset Name", "")
dbutils.widgets.dropdown("Dataset Name", "market solutions product", ['market solutions product', 'market solutions campaign', 'sales shop', 'sales product', 'flash sale'])

dbutils.widgets.text("From Date (YYYY/MM/DD)", "")
dbutils.widgets.text("To Date (YYYY/MM/DD)", "")

# COMMAND ----------

input_from_date = dbutils.widgets.get("From Date (YYYY/MM/DD)")
input_to_date = dbutils.widgets.get("To Date (YYYY/MM/DD)")
dataset_name = dbutils.widgets.get("Dataset Name")

from_date = get_date_format(input_from_date)
to_date = get_date_format(input_to_date)


base_folder_path = 'file:/tmp/Shopee/'


campaign_file_pattern = 'Off-platform_Traffic_Report---Campaign_Performance---Unilever-'
product_file_pattern = 'Off-platform_Traffic_Report---Product_Performance---Unilever-'
sales_shop_file_format = 'Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-'
sales_product_file_format = 'Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-'
flash_sales_file_format = 'Brand_Portal-Business_Insights---Brand_Marketing---Flash_Sales_Performance-Gross_Data-'


from_date_dot_format = str(from_date.year) + '.' + month_check(from_date) + '.' + day_check(from_date)
to_date_dot_format = str(to_date.year) + '.' + month_check(to_date) + '.' + day_check(to_date)


product_file_full_path = base_folder_path + product_file_pattern + str(from_date) + '_' + str(to_date) + '_' + 'App' + '.xlsx'
campaign_file_full_path = base_folder_path + campaign_file_pattern + str(from_date) + '_' + str(to_date) + '_' + 'App' + '.xlsx'
sales_shop_file_full_path = base_folder_path + sales_shop_file_format + from_date_dot_format + '_' + to_date_dot_format + '.xlsx'
sales_product_file_full_path = base_folder_path + sales_product_file_format + from_date_dot_format + '_' + to_date_dot_format + '.xlsx'
flash_sales_file_full_path = base_folder_path + flash_sales_file_format + from_date_dot_format + '_' + to_date_dot_format + '.xlsx'

# COMMAND ----------

#     browser.close()
#     browser.quit()

# COMMAND ----------

# Create credential
credentials = {
    "email": "nguyen-ngoc.hanh@unilever.com",
    "pass": "Pizza4ps700@"
}
element_load_timeout = 10 * 2
poll_frequency = 2
    
if not os.path.exists("/temp/Shopee/"):
    print("Create new folder: Shopee")
    dbutils.fs.mkdirs("file:/tmp/Shopee/")
else:
    print("Download folder ready: Shopee/")

# LOG IN
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--disable-blink-features=AutomationControlled')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--headless')
chrome_options.add_argument('--auto-open-devtools-for-tabs') 

prefs = {
    "download.prompt_for_download": False,
    "download.default_directory": "/tmp/Shopee/",
    "download.directory_upgrade": True,
    "safebrowsing.enabled": False,
    "safebrowsing.disable_download_protection": True
}
chrome_options.add_experimental_option("prefs", prefs)
chrome_driver = "/tmp/chromedriver/chromedriver"

#browser.close()

input_from_date = dbutils.widgets.get("From Date (YYYY/MM/DD)")
input_to_date = dbutils.widgets.get("To Date (YYYY/MM/DD)")

# input_from_date = '2022/05/01'
# input_to_date = '2022/05/28'

excution_count = 1

while excution_count <= 5:
  
  try:
    from_date = get_date_format(input_from_date)
    to_date = get_date_format(input_to_date)
    
    print(from_date, to_date)
    
    browser = webdriver.Chrome(executable_path=ChromeDriverManager(version = latest_chrome_version).install(), options=chrome_options)
    actions = ActionChains(browser) 
    
    if from_date <= to_date:
      
      timeout = 10
      url = r'https://brandportal.shopee.com/seller/login'
      browser.get(url)
      
      time.sleep(5)
      login_ISR(browser, chrome_options, timeout, credentials)
    
      dataset_name_witout_space = dbutils.widgets.get("Dataset Name").replace(" ", "")
      print(dataset_name_witout_space)
      
      if (dataset_name_witout_space.lower()) == 'marketsolutionsproduct':
        if not file_exists(dataset_name_witout_space, product_file_full_path):
          get_product_performance_data(browser,from_date, to_date)
          excution_count = 6
          print(excution_count)
        else:
          break
      elif (dataset_name_witout_space.lower()) == 'marketsolutionscampaign':
        if not file_exists(dataset_name_witout_space, campaign_file_full_path):
          get_campaign_performance_data(browser,from_date, to_date)
          excution_count = 6
          print(excution_count)
        else:
          break
      elif (dataset_name_witout_space.lower()) == 'salesshop':
        if not file_exists(dataset_name_witout_space, sales_shop_file_full_path):
          get_sales_shop_data(browser,from_date, to_date)
          excution_count = 6
          print(excution_count)
        else:
          break
      elif (dataset_name_witout_space.lower()) == 'salesproduct':
        if not file_exists(dataset_name_witout_space, sales_product_file_full_path):
          get_sales_product_performance_data(browser,from_date, to_date)
          excution_count = 6
          print(excution_count)
          if ((to_date-from_date).days + 1) > 31 :
            print('The export report will be sent to your email, please check it later.')
        else:
          break
      elif (dataset_name_witout_space.lower()) == 'flashsalesproduct':
        if not file_exists(dataset_name_witout_space, flash_sales_file_full_path):
          get_flash_sale_performance_data(browser,from_date, to_date)
          excution_count = 6
          print(excution_count)
        else:
          break
      else:
        print('please enter one of the following data set name: market solutions product,market solutions campaign, sales shop, sales product, flash sale')
        excution_count = 6
        browser.close()
        browser.quit()
        
    else:
      print("from_date should be less than to_date")
  except:
    excution_count = excution_count + 1
    browser.close()
    browser.quit()

# COMMAND ----------

try:
  browser.close()
  browser.quit()
  print('browser os closed')
except:
  print('No browser is opened')

# COMMAND ----------

# MAGIC %fs ls file:/tmp/Shopee/

# COMMAND ----------

# DBTITLE 1,Move Files to ADLS and Delete those
try:
  move_files_to_adls(dataset_name, from_date, to_date)
  print('moved file to ADLS')
  
  delete_file(dataset_name, from_date, to_date)
  print('Deleted the files')
  
except:
  print('either File not moved nor delete thr file')

# COMMAND ----------

# DBTITLE 1,Delete Files
#delete_file(dataset_name, from_date, to_date)

# COMMAND ----------

# import os, glob

# #Loop Through the folder projects all files and deleting them one by one
# for file in glob.glob("/tmp/Shopee/*"):
#     os.remove(file)
#     print("Deleted " + str(file))

# COMMAND ----------

#         today = date.today()
#         print('today:',today)
#         last_7_days = timedelta(-7)
#         last_7_days_date = date.today() + last_7_days
#         print(last_7_days_date)
#         print(from_date)

# COMMAND ----------


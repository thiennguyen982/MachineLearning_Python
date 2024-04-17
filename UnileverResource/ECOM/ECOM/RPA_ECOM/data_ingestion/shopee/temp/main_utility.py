# Databricks notebook source
# MAGIC %run ./Chrome_setup_utility

# COMMAND ----------

# MAGIC %run ./libraries_utility

# COMMAND ----------

# MAGIC %run ./Login_function

# COMMAND ----------

# MAGIC %run ./From_To_Date_selection_method

# COMMAND ----------

# MAGIC %run ./product_performance_utility

# COMMAND ----------

# MAGIC %run ./campaign_performance_utility

# COMMAND ----------

# MAGIC %run ./sales_performance_utilities

# COMMAND ----------

#%fs ls file:/tmp/Shopee/

# COMMAND ----------

dbutils.widgets.dropdown('Dataset Name', 'product', ['product', 'campaign', 'sales shop', 'sales product', 'flash sale'])

dbutils.widgets.text("From Date (YYYY/MM/DD)", "")
dbutils.widgets.text("To Date (YYYY/MM/DD)", "")

# COMMAND ----------

# browser.quit()

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
      
      #get_product_performance_data(browser,from_date, to_date)
      #time.sleep(10)
      #get_campaign_performance_data(browser,from_date, to_date)
      
      def call_data_set_method(method_name):
        switch={
          'product': get_product_performance_data(browser,from_date, to_date),
          
          'campaign': get_campaign_performance_data(browser,from_date, to_date),
          
          'sales shop': get_sales_shop_data(browser,from_date, to_date),
          
          'sales product': get_sales_product_performance_data(browser,from_date, to_date),
          
          'flash sale': get_flash_sale_performance_data(browser,from_date, to_date),
          }
        return switch.get(method_name,'Choose one of the following data set name: product, campaign, sales shop, sales product, flash sale')
      
      #print("select data set name which you want to download (product, campaign, sales shop, sales product, flash sale): ")
      dataset_name = dbutils.widgets.get("Dataset Name")
      call_data_set_method(dataset_name)
      excution_count = 6
    else:
      print("from_date should be less than to_date")
  except:
    excution_count = excution_count + 1
    browser.close()
    browser.quit()

# COMMAND ----------

#     browser.close()
#     browser.quit()

# COMMAND ----------

# # Create credential
# credentials = {
#     "email": "nguyen-ngoc.hanh@unilever.com",
#     "pass": "Pizza4ps700@"
# }
# element_load_timeout = 10 * 2
# poll_frequency = 2
    
# if not os.path.exists("/temp/Shopee/"):
#     print("Create new folder: Shopee")
#     dbutils.fs.mkdirs("file:/tmp/Shopee/")
# else:
#     print("Download folder ready: Shopee/")

# # LOG IN
# chrome_options = webdriver.ChromeOptions()
# chrome_options.add_argument('--disable-blink-features=AutomationControlled')
# chrome_options.add_argument('--no-sandbox')
# chrome_options.add_argument('--headless')
# chrome_options.add_argument('--auto-open-devtools-for-tabs') 

# prefs = {
#     "download.prompt_for_download": False,
#     "download.default_directory": "/tmp/Shopee/",
#     "download.directory_upgrade": True,
#     "safebrowsing.enabled": False,
#     "safebrowsing.disable_download_protection": True
# }
# chrome_options.add_experimental_option("prefs", prefs)
# chrome_driver = "/tmp/chromedriver/chromedriver"

# #browser.close()

# input_from_date = dbutils.widgets.get("From Date (YYYY/MM/DD)")
# input_to_date = dbutils.widgets.get("To Date (YYYY/MM/DD)")

# # input_from_date = '2022/05/01'
# # input_to_date = '2022/05/28'


# from_date = get_date_format(input_from_date)
# to_date = get_date_format(input_to_date)

# print(from_date, to_date)

# browser = webdriver.Chrome(executable_path=ChromeDriverManager(version = latest_chrome_version).install(), options=chrome_options)
# actions = ActionChains(browser) 

# if from_date <= to_date:
  
#   timeout = 10
#   url = r'https://brandportal.shopee.com/seller/login'
#   browser.get(url)
  
#   time.sleep(5)
#   login_ISR(browser, chrome_options, timeout, credentials)
  
#   #get_product_performance_data(browser,from_date, to_date)
#   #time.sleep(10)
#   #get_campaign_performance_data(browser,from_date, to_date)
  
#   def call_data_set_method(method_name):
#     switch={
#       'product': get_product_performance_data(browser,from_date, to_date),
      
#       'campaign': get_campaign_performance_data(browser,from_date, to_date),
      
#       'sales shop': get_sales_shop_data(browser,from_date, to_date),
      
#       'sales product': get_sales_product_performance_data(browser,from_date, to_date),
      
#       'flash sale': get_flash_sale_performance_data(browser,from_date, to_date),
#       }
#     return switch.get(method_name,'Choose one of the following data set name: product, campaign, sales shop, sales product, flash sale')
  
#   #print("select data set name which you want to download (product, campaign, sales shop, sales product, flash sale): ")
#   dataset_name = dbutils.widgets.get("Dataset Name")
#   print(dataset_name)
#   call_data_set_method(dataset_name)
# else:
#   print("from_date should be less than to_date")

# COMMAND ----------

# MAGIC %fs ls file:/tmp/Shopee/

# COMMAND ----------

import os, glob

#Loop Through the folder projects all files and deleting them one by one
for file in glob.glob("/tmp/Shopee/*"):
    os.remove(file)
    print("Deleted " + str(file))

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/tmp/Shopee/Marketing_solutions

# COMMAND ----------

dataset_name = dbutils.widgets.get("Dataset Name")
print(dataset_name)

# COMMAND ----------


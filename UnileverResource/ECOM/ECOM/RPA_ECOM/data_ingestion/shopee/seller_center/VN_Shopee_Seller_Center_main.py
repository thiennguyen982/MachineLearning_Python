# Databricks notebook source
pip install Selenium-Screenshot

# COMMAND ----------

pip install selenium==4.2.0

# COMMAND ----------

# MAGIC %run ./g-auth_setup_utility

# COMMAND ----------

# MAGIC %run ./Chrome_setup_utility

# COMMAND ----------

# MAGIC %run ./library_utility

# COMMAND ----------

def file_exists(path):
    try:
        #dbutils.fs.ls(path)
        os.path.exists(path)
        print("File is downloaded sucessfully")
        return True
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            return False
        else:
            raise

# COMMAND ----------

# MAGIC %run ./login_fun_utility

# COMMAND ----------

# MAGIC %run ./logic_app_trigger

# COMMAND ----------

# MAGIC %run ./dataset_functions_utility

# COMMAND ----------

# MAGIC %run ./move_file_utility

# COMMAND ----------

# MAGIC %run ./delete_file_utility

# COMMAND ----------

dbutils.widgets.dropdown("Shop Name", "Michiru official store", ["Michiru official store", "Unilever Việt Nam _ Health & Beauty", "Unilever - Chăm sóc Gia đình"])

dbutils.widgets.dropdown("Data Set Name", "Product Performance", ["Product Performance", "Traffic Overview", "Flash Sale", "Livestream"])

dbutils.widgets.dropdown("Date Range", "Past 7 Days", ["Yesterday", "Past 7 Days", "Past 30 Days", "By Monthly"])

year = datetime.datetime.today().year
years_list = list(range(2021, year + 1, 1))
str_years_list = list(map(str,years_list))

dbutils.widgets.dropdown("Year", str(year), str_years_list)

dbutils.widgets.dropdown("Month", "Jan", ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])

# COMMAND ----------

# Create credential
credentials = {
    "email": "unilevervn_beauty:CRMtool_2",
    "pass": "unilever@123"

}
element_load_timeout = 10 * 2
poll_frequency = 2
# Create folder for downloadq
if not os.path.exists("/temp/Shopee_seller_center/"):
    print("Create new folder: Shopee_seller_center")
    dbutils.fs.mkdirs("file:/tmp/Shopee_seller_center/")
else:
    print("Download folder ready: /tmp/Shopee_seller_center/")

# LOG IN
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--disable-blink-features=AutomationControlled')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--headless')
chrome_options.add_argument('--auto-open-devtools-for-tabs')


chrome_options.add_argument('window-size=1920x1080')

prefs = {
    "download.prompt_for_download": False,
    "download.default_directory": "/tmp/Shopee_Seller_Center/",
    "download.directory_upgrade": True,
    "safebrowsing.enabled": False,
    "safebrowsing.disable_download_protection": True
}
chrome_options.add_experimental_option("prefs", prefs)
chrome_driver = "/tmp/chromedriver/chromedriver"


excution_count = 1

while excution_count <= 5:
  
  try:
    browser = webdriver.Chrome(executable_path=ChromeDriverManager(version = latest_chrome_version).install(), options=chrome_options)
    actions = ActionChains(browser)
    
    timeout = 10
    url = r'https://banhang.shopee.vn/account/signin?next=%2F'
    browser.get(url)
    
    time.sleep(2)
    browser.find_element_by_xpath("//*[@id='app']/div[2]/div/div/div/div[3]/div/div/div/div[2]/button").click()
    
    #change the language
    time.sleep(10)
    browser.find_element(By.CLASS_NAME, "lang-content-wrapper").click()
    time.sleep(3)
    a = browser.find_element(by=By.XPATH, value='/html/body/div[2]/ul/li[1]')
    actions.move_to_element(a).click().perform()
    print('changed language')
    
    time.sleep(5)
    login_ISR(browser, chrome_options, timeout, credentials)
    
    time.sleep(5)
    
    try:
      #click on "send to email"
      email_button_xpath = "/html/body/div/main/div/div[1]/div/div/div/div/div/div/div[4]/button"
      browser.find_element(by=By.XPATH, value=email_button_xpath).click()
      print('clicked on send to email button')
    except:
      print(browser.find_element(by=By.CLASS_NAME, value='error-message').text)
      browser.close()
      
    OTP = get_OTP()
    print("Email OTP verification code:", OTP)
    
    time.sleep(1)
    #fill the OTP
    otp_space_xpath = '/html/body/div[1]/main/div/div[1]/div/div/div/div/div/div/div[3]/div/div/input'
    browser.find_element(by=By.XPATH, value=otp_space_xpath).send_keys(OTP)
    print('filled OTP')
    
    time.sleep(2)
    #click on 'verify'
    verify_button_xpath = "/html/body/div[1]/main/div/div[1]/div/div/div/div/div/div/button"
    browser.find_element(by=By.XPATH, value=verify_button_xpath).click()
    print('Sucessfully verified with OTP')
    
    try:
      time.sleep(8)
      browser.find_elements_by_class_name('shopee-icon shopee-modal__close').click()
      print('closed ad')
    except:
      print('No ads')
      
    #hover on to 'switch shop'
    time.sleep(20)
    a = browser.find_element(by=By.XPATH, value="//*[@id='app']/div[1]/div/div[2]/div/div[1]/div/div/div/span")
    actions.move_to_element(a).perform()
    
    #click on 'switch shop'
    time.sleep(3)
    swhitch_shop_xpath = "//*[@id='app']/div[1]/div/div[2]/div/div[2]/div/div/div/ul/li[1]/span[2]"
    browser.find_element(by=By.XPATH, value=swhitch_shop_xpath).click()
    print('clicked on switch shop')
    
    # Michiru official store
    # Unilever Việt Nam _ Health & Beauty
    # Unilever - Chăm sóc Gia đình
    shop_name = dbutils.widgets.get("Shop Name")
    data_set_name = dbutils.widgets.get("Data Set Name")
    date_range = dbutils.widgets.get("Date Range")
    year = dbutils.widgets.get("Year")
    month_name = dbutils.widgets.get("Month")
    
    time.sleep(10)
    print("Shop Name: ",shop_name)
    
    try:
      elements = browser.find_elements_by_class_name('shop-item-title')
    except:
      print('error in listing out shop names')
      
    try:
      for element in elements:
        if element.text == shop_name:
          element.click()
          print("clicked on " + shop_name + " shop name")
    except:
      print("Error in selecting the shop name")
      
    try:
      # click on 'Business_Insight'
      time.sleep(10)
      Business_Insight_xpath = "/html/body/div[1]/div[2]/div[1]/div/div/ul/li[3]/ul/li[1]/a/span"
      browser.find_element(by=By.XPATH, value=Business_Insight_xpath).click()
      print('clicked on Business_Insight')
    except:
      print('Error in clicking on Business_Insight')
      
    try:
      #fill the password
      time.sleep(10)
      verify_password_xpath = "/html/body/div[1]/div[2]/div/div/div/div/div/div/div[1]/form/div[2]/div/div[1]/div/div/input"
      browser.find_element(by=By.XPATH, value = verify_password_xpath).clear()
      browser.find_element(by=By.XPATH, value = verify_password_xpath).send_keys(credentials['pass'])
      print('filled password')
      
      #click on 'verify'
      time.sleep(2)
      verify_button_xpath = "/html/body/div[1]/div[2]/div/div/div/div/div/div/div[1]/form/div[3]/button[2]"
      browser.find_element(by=By.XPATH, value=verify_button_xpath).click()
      print("clicked on verify button")
    except:
      print("automaticaly loged-in")
    
    #-----------------------------------------------------------------------------------------------------------------
    
    time.sleep(5)
#     data_set_name = dbutils.widgets.get("Data Set Name")
#     date_range = dbutils.widgets.get("Date Range")
#     year = dbutils.widgets.get("Year")
#     month_name = dbutils.widgets.get("Month")
    
    if data_set_name == 'Product Performance':
      product_performance(browser, date_range, year,month_name)
      excution_count = 6
      print(excution_count)
      
    elif data_set_name == 'Traffic Overview':
      traffic_overview(browser, date_range, year,month_name)
      excution_count = 6
      print(excution_count)
      
    elif data_set_name == 'Flash Sale':
      flash_sale(browser, date_range, year,month_name)
      excution_count = 6
      print(excution_count)
      
    else:
      Livestream(browser, date_range, year,month_name)
      excution_count = 6
      print(excution_count)
      
  except:
    print("Error in opening the portal")
    browser.close()
    excution_count = excution_count + 1

# COMMAND ----------

# MAGIC %fs ls file:/tmp/Shopee_Seller_Center/

# COMMAND ----------

#%run ./move_file_utility

# COMMAND ----------

# DBTITLE 1,Move the file into ADLS
# shop_name = dbutils.widgets.get("Shop Name")
# data_set_name = dbutils.widgets.get("Data Set Name")
# date_range = dbutils.widgets.get("Date Range")
# year = dbutils.widgets.get("Year")
# month_name = dbutils.widgets.get("Month")
move_files_to_adls(shop_name, data_set_name, date_range, year, month_name)

# COMMAND ----------

# DBTITLE 1,delete file from ADB 
delete_file()

# COMMAND ----------

try:
  browser.close()
  browser.quit()
  print('browser os closed')
except:
  print('No browser is opened')

# COMMAND ----------


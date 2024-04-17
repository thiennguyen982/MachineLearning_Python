# Databricks notebook source
pip install Selenium-Screenshot

# COMMAND ----------

pip install selenium==4.2.0

# COMMAND ----------

# MAGIC %run ./g-auth_setup_utility

# COMMAND ----------

# MAGIC %run ./Chrome_setup_utility

# COMMAND ----------

# MAGIC %run ./logic_app_trigger

# COMMAND ----------

# MAGIC %run ./library_utility

# COMMAND ----------

import json
import logging
import os
import pandas as pd
import re
import datetime
from datetime import timedelta
import calendar
import time
import shutil
import chardet
from collections import OrderedDict
from Screenshot import Screenshot
import requests
#import globcd

from selenium import webdriver

from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

from webdriver_manager.chrome import ChromeDriverManager

logging.basicConfig(level=logging.INFO,
                    filename="error.log",
                    format='%(asctime)s %(message)s')

from __future__ import print_function

import os.path
from urllib.request import urlopen
from bs4 import BeautifulSoup

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import email
import base64

# COMMAND ----------

# import selenium
# selenium.__version__#'4.2.0'

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

# Login function
def login_ISR(driver, options, timeout, credentials):
  check = None
  while check is None:
    try:
      
      #WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.ID, 'app')))
      #filling the email id
      print('entered into login')
      userId_xpath = "/html/body/div/main/div/div[1]/div/div/div/div/div/div/div[2]/div[1]/div/div/div/input"
      driver.find_element(by=By.XPATH, value=userId_xpath).clear()
      driver.find_element(by=By.XPATH, value=userId_xpath).send_keys(credentials['email'])
      time.sleep(2)
      print('filled email')
      
      #filling password
      password_xpath = "/html/body/div/main/div/div[1]/div/div/div/div/div/div/div[2]/div[2]/div/div/input"
      driver.find_element(by=By.XPATH, value=password_xpath).clear()
      driver.find_element(by=By.XPATH, value=password_xpath).send_keys(credentials['pass'])
      time.sleep(2)
      print('filled password')
      
      #hit login button
      logIn_button_xpath = "/html/body/div/main/div/div[1]/div/div/div/div/div/div/button[2]"
      driver.find_element(by=By.XPATH, value=logIn_button_xpath).click()
      print('clicked on login button!')
      check = 'Sucessfully login!'
      print('Sucessfully login!')
      
    except:
      print("Cannot access! Browser closing ...")
      browser.close()
      browser.quit()

# COMMAND ----------

#time.sleep(90)
def get_OTP():  
  schema = { "otp": "not fetched" }
  otp= requests.post("https://prod-108.westeurope.logic.azure.com:443/workflows/3c940823adef4679b882b6c732458b49/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=EzIKXAZZgdVuxmrlzdPzx-uHiMJqq7QiMwV7uUVAni8",json=schema)
  final_otp = str(otp.text)[8:14]
  print(final_otp)
  return final_otp

# COMMAND ----------

dbutils.widgets.dropdown("Shop Name", "Michiru official store", ["Michiru official store", "Unilever Việt Nam _ Health & Beauty", "Unilever - Chăm sóc Gia đình"])

dbutils.widgets.dropdown("Data Set Name", "Product Performance", ["Product Performance", "Traffic Overview", "Livestream"])

dbutils.widgets.dropdown("Date Range", "Past 7 Days", ["Yesterday", "Past 7 Days", "Past 30 Days", "By Monthly"])

year = datetime.datetime.today().year
years_list = list(range(2021, year + 1, 1))
str_years_list = list(map(str,years_list))

dbutils.widgets.dropdown("Year", str(year), str_years_list)

dbutils.widgets.dropdown("Month", "Jan", ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",])

# COMMAND ----------

def product_performance(browser, date_range, year,month_name):
  print('entered into product_performance')
  try:
    tab_elements = browser.find_elements_by_class_name('text')
    for tab_element in tab_elements:
      #print(tab_element.text)
      if tab_element.text=="Product":
        tab_element.click()
        print('clicked on product tab')
        break
        
    time.sleep(5)
    pro_elements = browser.find_elements_by_class_name('name')
    for pro_element in pro_elements:
      #print(pro_element.text)
      if pro_element.text=="Product Performance":
        pro_element.click()
        print('clicked on Product Performance')
        break
        
    #click on date range
    time.sleep(8)
    date_range_xpath = "//*[@id='app']/div[2]/div/div[2]/div/section/div/div/div[3]/div/div[1]/div/div[1]/div/div[1]/div/span"
    button = browser.find_element(by=By.XPATH, value=date_range_xpath)
    browser.implicitly_wait(10)
    ActionChains(browser).move_to_element(button).click(button).perform()
    print('clicked on date range')    
    time.sleep(4)
    
    if date_range == 'Yesterday':
      Yesterday = "/html/body/div[4]/div/div/ul/li[2]/span[1]"                   
      browser.find_element(by=By.XPATH, value=Yesterday).click()
      print('clicked on last 7 days')  
    
    elif date_range == 'Past 7 Days':
      last_7_days = "/html/body/div[4]/div/div/ul/li[3]/span[1]"
      browser.find_element(by=By.XPATH, value=last_7_days).click()
      print('clicked on last 7 days')
      
    elif date_range == 'Past 30 Days':
      last_30_days = "/html/body/div[4]/div/div/ul/li[4]/span[1]"
      browser.find_element(by=By.XPATH, value=last_30_days).click()
      print('clicked on last 30 days')
      
    else:    
      time.sleep(2)
      By_month = "/html/body/div[4]/div/div/ul/li[8]/span"
      # hover to month
      a = browser.find_element(by=By.XPATH, value=By_month)
      actions.move_to_element(a).click().perform()
      print('hovered on By Month')
      
      time.sleep(5)
      months_elements = browser.find_elements_by_class_name('shopee-month-table__col')
      for month_element in months_elements:
        print(month_element.text)
        if month_element.text == month_name:
          month_element.click()
          print('clicked on Month')
          break
    
    try:
      time.sleep(8)
      export_xpath = "/html/body/div[1]/div[2]/div/div[2]/div/section/div/div/div[3]/div/div[3]/div[1]/div/button"
      export_button = browser.find_element(by=By.XPATH, value=export_xpath)
      browser.implicitly_wait(10)
      ActionChains(browser).move_to_element(export_button).click(export_button).perform()
      print('clicked on Export Data')
    except:
      print('Error in clicking on Export button')
      
    try:
      time.sleep(8)
      down_xpath = "//*[@id='app']/div[2]/div/div[2]/div/section/div/div/div[3]/div/div[3]/div[1]/div/div/div[2]/div/div/div[3]/div[3]/div[1]/div/div/div[2]/table/tbody/tr[1]/td[2]/div/div/button"
      down_button = browser.find_element(by=By.XPATH, value=down_xpath)
      browser.implicitly_wait(10)
      ActionChains(browser).move_to_element(down_button).click(down_button).perform()
      print('clicked on download button')
      print('Sucessfully download the Product Performance Dataset')
    except:
      print('Error in clicking on download button')
  
  except:
    print('Error in downloading the Product Performance Dataset')

# COMMAND ----------

def traffic_overview(browser, date_range, year,month_name):
  print('entered into traffic_overview')
  try:
    tab_elements = browser.find_elements_by_class_name('text')
    for tab_element in tab_elements:
      #print(tab_element.text)
      if tab_element.text=="Traffic":
        tab_element.click()
        print('clicked on Traffic tab')
        break
        
    time.sleep(5)
    pro_elements = browser.find_elements_by_class_name('name')
    for pro_element in pro_elements:
      #print(pro_element.text)
      if pro_element.text=="Traffic Overview":
        pro_element.click()
        print('clicked on Traffic Overview')
        break
        
    #click on date range
    time.sleep(8)
    date_range_xpath = "//*[@id='app']/div[2]/div/div[2]/div/div/div/div[3]/div/div[1]/div/div[1]/div/div[1]/div/span"                     
    button = browser.find_element(by=By.XPATH, value=date_range_xpath)
    browser.implicitly_wait(10)
    ActionChains(browser).move_to_element(button).click(button).perform()
    print('clicked on date range')
    
    time.sleep(4) 
    if date_range == 'Yesterday':
      Yesterday = "/html/body/div[4]/div/div/ul/li[1]/span[1]"                     
      browser.find_element(by=By.XPATH, value=Yesterday).click()
      print('clicked on last 7 days')   
    
    elif date_range == 'Past 7 Days':
      last_7_days = "/html/body/div[4]/div/div/ul/li[2]/span[1]"                     
      browser.find_element(by=By.XPATH, value=last_7_days).click()
      print('clicked on last 7 days')
      
    elif date_range == 'Past 30 Days':
      last_30_days = "/html/body/div[4]/div/div/ul/li[3]/span[1]"                      
      browser.find_element(by=By.XPATH, value=last_30_days).click()
      print('clicked on last 30 days')
      
    else:    
      time.sleep(2)
      By_month = "/html/body/div[4]/div/div/ul/li[7]/span"                
      # hover to month
      a = browser.find_element(by=By.XPATH, value=By_month)
      actions.move_to_element(a).click().perform()
      print('hovered on By Month')
      
      time.sleep(5)
      months_elements = browser.find_elements_by_class_name('shopee-month-table__col')
      for month_element in months_elements:
        print(month_element.text)
        if month_element.text == month_name:
          month_element.click()
          print('clicked on Month')
          break
    
    try:
      time.sleep(8)
      export_xpath = "//*[@id='app']/div[2]/div/div[2]/div/div/div/div[3]/div/button"
      export_button = browser.find_element(by=By.XPATH, value=export_xpath)
      browser.implicitly_wait(10)
      ActionChains(browser).move_to_element(export_button).click(export_button).perform()
      print('clicked on Export Data')
      print('Sucessfully download the Traffic Overview Dataset')
    except:
      print('Error in clicking on Export button')
  
  except:
    print('Error in downloading the Traffic Overview Dataset')

# COMMAND ----------

def Livestream(browser, date_range, year,month_name):
  print('entered into livesteam')
  try:
    tab_elements = browser.find_elements_by_class_name('text')
    for tab_element in tab_elements:
      #print(tab_element.text)
      if tab_element.text=="Marketing":
        tab_element.click()
        print('clicked on Marketing tab')
        break
        
    time.sleep(5)
    pro_elements = browser.find_elements_by_class_name('name')
    for pro_element in pro_elements:
      #print(pro_element.text)
      if pro_element.text=="Livestream":
        pro_element.click()
        print('clicked on livestream')
        break
        
    #click on date range
    time.sleep(8)
    date_range_xpath = "//*[@id='app']/div[2]/div/div[2]/div/div/div/div/div[4]/div/div[1]/div/div/div/div[1]/div/span"                  
    button = browser.find_element(by=By.XPATH, value=date_range_xpath)
    browser.implicitly_wait(10)
    ActionChains(browser).move_to_element(button).click(button).perform()
    print('clicked on date range')
    
    time.sleep(4)
    
    if date_range == 'Past 7 Days':
      last_7_days = "/html/body/div[4]/div/div/ul/li[1]/span[1]"                     
      browser.find_element(by=By.XPATH, value=last_7_days).click()
      print('clicked on last 7 days')
      
    elif date_range == 'Past 30 Days':
      last_30_days = "/html/body/div[4]/div/div/ul/li[2]/span[1]"                      
      browser.find_element(by=By.XPATH, value=last_30_days).click()
      print('clicked on last 30 days')
      
    else:    
      time.sleep(2)
      By_month = "/html/body/div[4]/div/div/ul/li[6]/span"                
      # hover to month
      a = browser.find_element(by=By.XPATH, value=By_month)
      actions.move_to_element(a).click().perform()
      print('hovered on By Month')
      
      time.sleep(5)
      months_elements = browser.find_elements_by_class_name('shopee-month-table__col')
      for month_element in months_elements:
        print(month_element.text)
        if month_element.text == month_name:
          month_element.click()
          print('clicked on Month')
          break
    
    try:
      time.sleep(8)
      export_xpath = "//*[@id='app']/div[2]/div/div[2]/div/div/div/div/div[4]/div/button"
      export_button = browser.find_element(by=By.XPATH, value=export_xpath)
      browser.implicitly_wait(10)
      ActionChains(browser).move_to_element(export_button).click(export_button).perform()
      print('clicked on Export Data')
      print('Sucessfully download the livestream Dataset')
    except:
      print('Error in clicking on Export button')
  
  except:
    print('Error in downloading the livestream Dataset')

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
    "download.default_directory": "/tmp/Shopee1/Seller_Center/",
    "download.directory_upgrade": True,
    "safebrowsing.enabled": False,
    "safebrowsing.disable_download_protection": True
}
chrome_options.add_experimental_option("prefs", prefs)
chrome_driver = "/tmp/chromedriver/chromedriver"

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
  #print('yes')
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
  time.sleep(10)
  shop_name = dbutils.widgets.get("Shop Name")
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
  data_set_name = dbutils.widgets.get("Data Set Name")
  #["Product Performance", "Traffic Overview", "Livestream"]
  date_range = dbutils.widgets.get("Date Range")
  year = dbutils.widgets.get("Year")
  month_name = dbutils.widgets.get("Month")
  
  if data_set_name == 'Product Performance':
    product_performance(browser, date_range, year,month_name)
   
  elif data_set_name == 'Traffic Overview':
    traffic_overview(browser, date_range, year,month_name)
    
  else:
    Livestream(browser, date_range, year,month_name)
    

except:
  print("Error in opening the portal")
  browser.close()

# COMMAND ----------

# MAGIC %fs ls file:/tmp/Shopee1/Seller_Center/

# COMMAND ----------

#browser.close()

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


#chrome_options.add_argument('window-size=1920x1080')
chrome_options.add_argument("window-size=1200x600")

prefs = {
    "download.prompt_for_download": False,
    "download.default_directory": "/tmp/Shopee1/Seller_Center/",
    "download.directory_upgrade": True,
    "safebrowsing.enabled": False,
    "safebrowsing.disable_download_protection": True
}
chrome_options.add_experimental_option("prefs", prefs)
chrome_driver = "/tmp/chromedriver/chromedriver"


browser = webdriver.Chrome(executable_path=ChromeDriverManager(version = latest_chrome_version).install(), options=chrome_options)
actions = ActionChains(browser)

timeout = 10
url = r'https://banhang.shopee.vn/account/signin?next=%2F'
browser.get(url)

time.sleep(5)
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
#print('yes')
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
#subaccount-name
a = browser.find_element(by=By.XPATH, value="//*[@id='app']/div[1]/div/div[2]/div/div[1]/div/div/div/span")
actions.move_to_element(a).perform()

#click on 'switch shop'
time.sleep(3)
swhitch_shop_xpath = "//*[@id='app']/div[1]/div/div[2]/div/div[2]/div/div/div/ul/li[1]/span[2]"

a = browser.find_element(by=By.XPATH, value=swhitch_shop_xpath)
actions.move_to_element(a).click().perform()
#browser.find_element(by=By.XPATH, value=swhitch_shop_xpath).click()
print('clicked on switch shop')


time.sleep(10)
shop_name = dbutils.widgets.get("Shop Name")
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
  
data_set_name = dbutils.widgets.get("Data Set Name")
#["Product Performance", "Traffic Overview", "Livestream"]
date_range = dbutils.widgets.get("Date Range")
year = dbutils.widgets.get("Year")
month_name = dbutils.widgets.get("Month")
  
#----------------------------------------------------------------------------------------------------------------
time.sleep(5)
tab_elements = browser.find_elements_by_class_name('text')
for tab_element in tab_elements:
  print(tab_element.text)
  if tab_element.text=="Product":
    tab_element.click()
    print('clicked on product tab')
    
time.sleep(5)
pro_elements = browser.find_elements_by_class_name('name')
for pro_element in pro_elements:
  print(pro_element.text)
  if pro_element.text=="Product Performance":
    pro_element.click()
    print('clicked on Product Performance')
    
#click on date range
time.sleep(8)
date_range_xpath = "//*[@id='app']/div[2]/div/div[2]/div/section/div/div/div[3]/div/div[1]/div/div[1]/div/div[1]/div/span"
button = browser.find_element(by=By.XPATH, value=date_range_xpath)
browser.implicitly_wait(10)
ActionChains(browser).move_to_element(button).click(button).perform()
print('clicked on date range')    
time.sleep(4)

if date_range == 'Past 7 Days':
  last_7_days = "/html/body/div[4]/div/div/ul/li[3]/span[1]"
  browser.find_element(by=By.XPATH, value=last_7_days).click()
  print('clicked on last 7 days')
  
elif date_range == 'Past 30 Days':
  last_30_days = "/html/body/div[4]/div/div/ul/li[4]/span[1]"
  browser.find_element(by=By.XPATH, value=last_30_days).click()
  print('clicked on last 30 days')
  
else:    
  time.sleep(2)
  By_month = "/html/body/div[4]/div/div/ul/li[8]/span"
  # hover to month
  a = browser.find_element(by=By.XPATH, value=By_month)
  actions.move_to_element(a).click().perform()
  print('hovered on By Month')
  
time.sleep(5)
months_elements = browser.find_elements_by_class_name('shopee-month-table__col')
for month_element in months_elements:
  print(month_element.text)
  if month_element.text == month_name:
    month_element.click()
    print('clicked on Month')
    break
    
time.sleep(8)
export_xpath = "/html/body/div[1]/div[2]/div/div[2]/div/section/div/div/div[3]/div/div[3]/div[1]/div/button"
export_button = browser.find_element(by=By.XPATH, value=export_xpath)
browser.implicitly_wait(10)
ActionChains(browser).move_to_element(export_button).click(export_button).perform()
print('clicked on Export Data')

time.sleep(8)
down_xpath = "//*[@id='app']/div[2]/div/div[2]/div/section/div/div/div[3]/div/div[3]/div[1]/div/div/div[2]/div/div/div[3]/div[3]/div[1]/div/div/div[2]/table/tbody/tr[1]/td[2]/div/div/button"
down_button = browser.find_element(by=By.XPATH, value=down_xpath)
browser.implicitly_wait(10)
ActionChains(browser).move_to_element(down_button).click(down_button).perform()
print('clicked on download button')
print('Sucessfully download the Product Performance Datase')

# COMMAND ----------

# url = r'https://banhang.shopee.vn/account/signin?next=%2F'

# url = "https://banhang.shopee.vn/datacenter/products/analysis/performance"

# url = "https://banhang.shopee.vn/datacenter/traffic/traffic/overview"

# url = "https://banhang.shopee.vn/datacenter/marketing/tools/crm"

# url = "https://banhang.shopee.vn/datacenter/marketing/content/livestreaming"

# url = "https://banhang.shopee.vn/datacenter/marketing/content/feed"

# COMMAND ----------

# # Create credential
# credentials = {
#     "email": "unilevervn_beauty:CRMtool_2",
#     "pass": "unilever@123"

# }
# element_load_timeout = 10 * 2
# poll_frequency = 2
# # Create folder for downloadq
# if not os.path.exists("/temp/Shopee_seller_center/"):
#     print("Create new folder: Shopee_seller_center")
#     dbutils.fs.mkdirs("file:/tmp/Shopee_seller_center/")
# else:
#     print("Download folder ready: /tmp/Shopee_seller_center/")

# # LOG IN
# chrome_options = webdriver.ChromeOptions()
# chrome_options.add_argument('--disable-blink-features=AutomationControlled')
# chrome_options.add_argument('--no-sandbox')
# chrome_options.add_argument('--headless')
# chrome_options.add_argument('--auto-open-devtools-for-tabs')


# chrome_options.add_argument('window-size=1920x1080')

# prefs = {
#     "download.prompt_for_download": False,
#     "download.default_directory": "/tmp/Shopee1/Seller_Center/",
#     "download.directory_upgrade": True,
#     "safebrowsing.enabled": False,
#     "safebrowsing.disable_download_protection": True
# }
# chrome_options.add_experimental_option("prefs", prefs)
# chrome_driver = "/tmp/chromedriver/chromedriver"

# browser = webdriver.Chrome(executable_path=ChromeDriverManager(version = latest_chrome_version).install(), options=chrome_options)
# actions = ActionChains(browser)

# timeout = 10
# url = r'https://banhang.shopee.vn/account/signin?next=%2F'
# browser.get(url)

# time.sleep(2)
# browser.find_element_by_xpath("//*[@id='app']/div[2]/div/div/div/div[3]/div/div/div/div[2]/button").click()

# #change the language 
# time.sleep(5)
# #browser.find_element_by_xpath("/html/body/div[1]/main/div/div[2]/div/div/span").click()
# browser.find_element_by_xpath("/html/body/div/main/div/div[2]/div/div[1]/span").click()
                               

# time.sleep(1)
# a = browser.find_element(by=By.XPATH, value='/html/body/div[2]/ul/li[1]')
# actions.move_to_element(a).click().perform()

# print('changed language')
# time.sleep(5)
# login_ISR(browser, chrome_options, timeout, credentials)

# driver = browser
# driver.find_element_by_xpath("/html/body/div/main/div/div[1]/div/div/div/div/div/div/div[2]/div[1]/div/div/div/input").clear()
# driver.find_element_by_xpath("/html/body/div/main/div/div[1]/div/div/div/div/div/div/div[2]/div[1]/div/div/div/input").send_keys(credentials['email'])
# time.sleep(2)

# #filling password
# driver.find_element_by_xpath("/html/body/div/main/div/div[1]/div/div/div/div/div/div/div[2]/div[3]/div/div/input").clear()
# driver.find_element_by_xpath("/html/body/div/main/div/div[1]/div/div/div/div/div/div/div[2]/div[3]/div/div/input").send_keys(credentials['pass'])
#                               #/html/body/div/main/div/div[1]/div/div/div/div/div/div/div[2]/div[3]/div/div/input
# time.sleep(2)

# #hit login button
# driver.find_element_by_xpath("/html/body/div/main/div/div[1]/div/div/div/div/div/div/button[2]").click()
# check = 'Sucessfully login!'

# time.sleep(5)
# #click on "send to email"
# #driver.find_element_by_xpath("/html/body/div[1]/main/div/div[1]/div/div/div/div/div/div/div[4]/button").click()
# driver.find_element_by_xpath("/html/body/div/main/div/div[1]/div/div/div/div/div/div/div[4]/button").click()
#                               #/html/body/div/main/div/div[1]/div/div/div/div/div/div/div[4]/button
                             

# #time.sleep(10)
# final_OTP = get_OTP()
# print("Email OTP verification code:", final_OTP)

# time.sleep(1)
# #fill the OTP
# driver.find_element_by_xpath('/html/body/div[1]/main/div/div[1]/div/div/div/div/div/div/div[3]/div/div/input').send_keys(final_OTP)

# time.sleep(2)
# #click on 'verify'
# driver.find_element_by_xpath("/html/body/div[1]/main/div/div[1]/div/div/div/div/div/div/button").click()

# #hover on to 'switch shop'
# time.sleep(20)
# #a = browser.find_element(by=By.XPATH, value="//*[@id='app']/div[1]/div/div[2]/div/div[1]/div/div/div")
#                                              #//*[@id="app"]/div[1]/div/div[2]/div/div[1]/div/div/div
# a = browser.find_element(by=By.XPATH, value="//*[@id='app']/div[1]/div/div[2]/div/div[1]/div/div/i/svg")
                                            
# actions.move_to_element(a).perform()

# #click on 'switch shop'
# time.sleep(3)
# driver.find_element_by_xpath("//*[@id='app']/div[1]/div/div[2]/div/div[2]/div/div/div/ul/li[1]/span[2]").click()

# # click on 'unilever_vietnam'
# time.sleep(6)
# #driver.find_element_by_xpath("//*[@id='app']/div[2]/div/div/div[4]/div/div[2]/div[1]/div/div[2]/div").click()
# driver.find_element_by_xpath("/html/body/div[1]/div[2]/div/div/div[4]/div/div[2]/div[1]/div/div[2]/div").click()

# # click on 'Business_Insight'
# time.sleep(10)
# driver.find_element_by_xpath("/html/body/div[1]/div[2]/div[1]/div/div/ul/li[3]/ul/li[1]/a/span").click()
# #/html/body/div[1]/div[2]/div[1]/div/div/ul/li[3]/ul/li[1]/a/span[1]

# try:
#     #fill the password
#     time.sleep(10)
#     driver.find_element_by_xpath("/html/body/div[1]/div[2]/div/div/div/div/div/div/div[1]/form/div[2]/div/div[1]/div/div/input").clear()
#     driver.find_element_by_xpath("/html/body/div[1]/div[2]/div/div/div/div/div/div/div[1]/form/div[2]/div/div[1]/div/div/input").send_keys(credentials['pass'])
    
#     #click on 'verify'
#     time.sleep(2)
#     driver.find_element_by_xpath("/html/body/div[1]/div[2]/div/div/div/div/div/div/div[1]/form/div[3]/button[2]").click()  
# except :
#     print("automaticaly loged-in")

# # #click on Product tab
# # time.sleep(5)
# # driver.find_element_by_xpath("/html/body/div[1]/div[2]/div/div[1]/nav/a[2]/span").click()


# # #click on Product Performance left side tab
# # time.sleep(2)
# # driver.find_element_by_xpath("//*[@id='app']/div[2]/div/div[2]/nav/section[1]/ul/li[2]/span").click()

# month  = datetime.datetime.now().month - 1

# month_num = str(month)
# datetime_object = datetime.datetime.strptime(month_num, "%m")

# month_name = datetime_object.strftime("%b")

# try:
#     time.sleep(5)
#     url = "https://banhang.shopee.vn/datacenter/products/analysis/performance"
    
#     browser.execute_script("window.open('" + url + "');")
    
#     #click on date range
#     # time.sleep(20)
#     # driver.find_element_by_xpath("//*[@id='app']/div[2]/div/div[2]/div/section/div/div/div[3]/div/div[1]/div/div/div/div[1]/div/div/span").click()
#     #                               #//*[@id="app"]/div[2]/div/div[2]/div/section/div/div/div[3]/div/div[1]/div
#     #                              # //*[@id="app"]/div[2]/div/div[2]/div/section/div/div/div[3]/div/div[1]/div/div/div/div[1]/div
#     # # hover to month
#     # a = driver.find_element(by=By.XPATH, value='/html/body/div[4]/div/div/ul/li[8]/span')
#     # actions.move_to_element(a).click().perform()
    
#     # time.sleep(2)
#     # q = browser.find_element_by_css_selector("[title^='" + month_name + "']")
#     # print("selected the time period")
    
    
#     # print("error in date selection for product performance")
    
    
#     print("Ready to download!")
#     time.sleep(20)
#     driver.find_element_by_xpath("//*[@id='app']/div[2]/div/div[2]/div/section/div/div/div[3]/div/div[3]/div/div/button").click()
#                               #//*[@id="app"]/div[2]/div/div[2]/div/section/div/div/div[3]/div/div[3]/div[1]/div/button
        
#     time.sleep(5)
#     driver.find_element_by_xpath("//*[@id='app']/div[2]/div/div[2]/div/section/div/div/div[3]/div/div[3]/div/div/div/div[2]/div/div/div[3]/div[3]/div[1]/div/div/div[2]/table/tbody/tr[1]/td[2]/div/div/button").click()
    
#     print("Dowloaded Product Performance data set")   
    
# except:
#     print('error in Product performance')
                               
# #-------------------------------------------------------------------------------------------------------------------
# time.sleep(8)
# url = "https://banhang.shopee.vn/datacenter/traffic/traffic/overview"

# browser.execute_script("window.open('" + url + "');")


# #click on date range
# try:
#     time.sleep(10)
#     driver.find_element_by_xpath("//*[@id='app']/div[2]/div/div[2]/div/section/div/div/div[3]/div/div[1]/div/div[1]/div/div[1]/div/span").click()
    
#     # hover to month
#     a = driver.find_element(by=By.XPATH, value='/html/body/div[5]/div/div/ul/li[8]/span')
#     actions.move_to_element(a).click().perform()
#     time.sleep(2)
# except:
#     print("error in date selection for Traffic Overview")
    
# print("Ready to download!")
# time.sleep(5)
# WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.ID, 'app')))
# driver.find_element_by_xpath("/html/body/div[1]/div[2]/div/div[2]/div/div/div/div[3]/div/button").click()
# print("Dowloaded Traffic Overview data set") 


# #-------------------------------------------------------------------------------------------------------

# time.sleep(8)
# url = "https://banhang.shopee.vn/datacenter/marketing/tools/crm"

# browser.execute_script("window.open('" + url + "');")


# #click on Chat Broadcast left side tab
# # time.sleep(2)
# # driver.find_element_by_xpath("//*[@id='app']/div[2]/div/div[2]/nav/section[1]/ul/li[6]/span").click()


# #click on date range
# try:
#     time.sleep(10)
#     driver.find_element_by_xpath("//*[@id='app']/div[2]/div/div[2]/div/section/div/div/div[3]/div/div[1]/div/div[1]/div/div[1]/div/span").click()
    
#     # hover to month
#     a = driver.find_element(by=By.XPATH, value='/html/body/div[5]/div/div/ul/li[8]/span')
#     actions.move_to_element(a).click().perform()
#     time.sleep(2)
# except:
#     print("error in date selection for Chat Broadcast")
    
# print("Ready to download!")
# time.sleep(10)
# driver.find_element_by_xpath("/html/body/div[1]/div[2]/div/div[2]/div/div/div/div[3]/div/button").click()
# print("Dowloaded Chat Broadcast data set")

# #-------------------------------------------------------------------------
# time.sleep(8)
# url = "https://banhang.shopee.vn/datacenter/marketing/content/livestreaming"

# browser.execute_script("window.open('" + url + "');")

# #click on date range
# try:
#     time.sleep(10)
#     driver.find_element_by_xpath("//*[@id='app']/div[2]/div/div[2]/div/section/div/div/div[3]/div/div[1]/div/div[1]/div/div[1]/div/span").click()
    
#     # hover to month
#     a = driver.find_element(by=By.XPATH, value='/html/body/div[5]/div/div/ul/li[8]/span')
#     actions.move_to_element(a).click().perform()
#     time.sleep(2)
# except:
#     print("error in date selection for Livestream")
    
# print("Ready to download!")
# time.sleep(5)
# driver.find_element_by_xpath("/html/body/div[1]/div[2]/div/div[2]/div/div/div/div/div[4]/div/button").click()
# print("Dowloaded Livestream data set")

# #-------------------------------------------------------------------------------------
# time.sleep(8)
# url = "https://banhang.shopee.vn/datacenter/marketing/content/feed"

# browser.execute_script("window.open('" + url + "');")


# #click on Chat Broadcast left side tab
# # time.sleep(2)
# # driver.find_element_by_xpath("//*[@id='app']/div[2]/div/div[2]/nav/section[1]/ul/li[6]/span").click()


# #click on date range
# try:
#     time.sleep(10)
#     driver.find_element_by_xpath("//*[@id='app']/div[2]/div/div[2]/div/section/div/div/div[3]/div/div[1]/div/div[1]/div/div[1]/div/span").click()
    
#     # hover to month
#     a = driver.find_element(by=By.XPATH, value='/html/body/div[5]/div/div/ul/li[8]/span')
#     actions.move_to_element(a).click().perform()
#     time.sleep(2)
# except:
#     print("error in date selection for Feed")
    
# print("Ready to download!")
# time.sleep(5)
# driver.find_element_by_xpath("/html/body/div[1]/div[2]/div/div[2]/div/div/div/div[3]/div/button").click()
# print("Dowloaded Feed set")

                               





# # from selenium.webdriver.common.by import By
# # from selenium.webdriver.support.ui import WebDriverWait
# # from selenium.webdriver.support import expected_conditions


# # element=WebDriverWait(driver,30).until(expected_conditions.element_to_be_clickable((By.XPATH,'//p[contains(.,"unilever_beauty_premium")]')))
# # print(element.text)


# # param = "unilever_beauty_premium"

# # driver.find_element_by_xpath('.//p[contains(text(),"unilever_beauty_premium")]').click()

# # https://banhang.shopee.vn/

# # #click on "unilever_beauty_premium"
# # driver.find_element_by_xpath("//*[@id='app']/div[2]/div/div/div[4]/div/div[2]/div[1]/div/div[2]/div").click()


# # #print(driver.find_element_by_xpath("//*[@id='app']/div[2]/div/div/div[4]/div/div[2]/div[1]/div/div[2]/div").text)

# # time.sleep(3)
# # driver.find_element(By.xpath ("//*[contains(text(), '" + param + "')]")).click()


# COMMAND ----------

# import datetime



# print(month_name)

# COMMAND ----------



# COMMAND ----------


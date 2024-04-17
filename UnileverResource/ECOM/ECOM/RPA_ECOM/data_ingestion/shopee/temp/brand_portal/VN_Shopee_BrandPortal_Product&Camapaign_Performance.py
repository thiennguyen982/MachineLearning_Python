# Databricks notebook source
!pip install webdriver_manager chromedriver_autoinstaller pandas

# COMMAND ----------

# MAGIC %sh cat /etc/*-release

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo add-apt-repository universe
# MAGIC sudo add-apt-repository multiverse

# COMMAND ----------

import requests
import selenium

def get_chrome_latest_release():
    url = "https://chromedriver.storage.googleapis.com/LATEST_RELEASE"
    response = requests.request("GET", url)
    return response.text

latest_chrome_version  = get_chrome_latest_release()
latest_chrome_version

# COMMAND ----------

# MAGIC %sh 
# MAGIC wget https://chromedriver.storage.googleapis.com/103.0.5060.53/chromedriver_linux64.zip /tmp/chromedriver_linux64.zip
# MAGIC rm -r /tmp/chromedriver
# MAGIC mkdir /tmp/chromedriver
# MAGIC unzip /tmp/chromedriver_linux64.zip -d /tmp/chromedriver/
# MAGIC sudo add-apt-repository ppa:canonical-chromium-builds/stage
# MAGIC /usr/bin/yes | sudo apt update
# MAGIC /usr/bin/yes | sudo apt install chromium-browser

# COMMAND ----------

# MAGIC %sh rm -f google-chrome-stable_current_amd64.deb

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo killall apt-get apt dpkg
# MAGIC sudo apt --fix-broken install -y
# MAGIC sudo apt install -y libxss1 libappindicator1 libindicator7 fonts-liberation libgbm1 libnspr4 libnss3 libwayland-server0
# MAGIC wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
# MAGIC

# COMMAND ----------

# MAGIC %sh sudo apt-get update -y
# MAGIC

# COMMAND ----------

# MAGIC %sh 
# MAGIC sudo dpkg -i ./google-chrome*.deb 

# COMMAND ----------

# MAGIC %sh google-chrome --product-version
# MAGIC

# COMMAND ----------

import chromedriver_autoinstaller
from selenium import webdriver
chrome_driver = "/tmp/chromedriver/chromedriver"

chromedriver_autoinstaller.install()


# COMMAND ----------

import subprocess

command = "chromedriver --version" 
completed_process = subprocess.run(command, shell=True, text=True, capture_output=True)
print(completed_process)
output = completed_process.stdout
output

# COMMAND ----------

import re
regex = "ChromeDriver (.*) .*"
chrome_version = re.search(regex, output).group(1)
print(chrome_version)

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

# COMMAND ----------

# current_datetime = datetime.datetime.now() + datetime.timedelta(hours=7)
# current_date = datetime.datetime.strftime(current_datetime, '%Y-%m-%d')
# mtd_begin = datetime.datetime.strftime((current_datetime - datetime.timedelta(days=1)).replace(day=1), '%Y-%m-%d')

# ### Create vars for daily report
# yesterday = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=1), '%Y.%m.%d')
# last_7day = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=7), '%Y.%m.%d')
# last_30day = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=30), '%Y.%m.%d')

# last_7day_begin = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=7), '%Y-%m-%d')
# last_30day_begin = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=30), '%Y-%m-%d')
# month_to_date = mtd_begin.replace("-", ".")

# pattern_yesterday = "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-{}_{}.xlsx".format(yesterday, yesterday)
# pattern_last_7days= "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-{}_{}.xlsx".format(last_7day, yesterday)
# pattern_last_30days = "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-{}_{}.xlsx".format(last_30day, yesterday)
# pattern_monthtodate = "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-{}_{}.xlsx".format(month_to_date, yesterday)


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
            WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.ID, 'app')))
            time.sleep(2)
            driver.find_element_by_id("email").clear()
            driver.find_element_by_id("email").send_keys(credentials['email'])
            time.sleep(2)
            # driver.find_element_by_id('email').send_keys(credentials['email'])
            driver.find_element_by_xpath("//*[@id='password']").clear()
            driver.find_element_by_xpath("//*[@id='password']").send_keys(credentials['pass'])
            time.sleep(2)
            driver.find_element_by_xpath("//*[@id='app']/div/div/div/div/div[2]/div/div/div[3]/form/div[3]/div/div/button").click()
            
            WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH, "//*[@id='app']/div/div/div/div/div[2]/div/div/div[2]/button")))
            browser.find_element_by_xpath("//*[@id='app']/div/div/div/div/div[2]/div/div/div[2]/button").click() ### Click GO
            check = 'Sucessfully login!'
            print(check)
        except:
            print("Cannot access! Browser closing ...")
            browser.close()
            browser.quit()

# COMMAND ----------

now = datetime.datetime.now()
year = now.year
month = now.month-1
if month <= 9:
    year_month = (str(year)+'-0'+str(month))
else:
    year_month = (str(year)+'-'+str(month))

# COMMAND ----------

def get_product_performance_data():
    time.sleep(5)
    url = "https://brandportal.shopee.com/seller/mkt/traffic/product"
    
    browser.execute_script("window.open('" + url + "');")
    
    #browser.find_element_by_xpath("//*[@id='app']/div/div[1]/ul/li[2]").click()
    
    time.sleep(5)
    browser.switch_to.window(browser.window_handles[-1])
    
    print("Clicked date Period")
    browser.find_element(by=By.XPATH, value='//*[@id="app"]/div/div[2]/div[2]/div/div/div[1]/div[2]/div/div/div[2]/div/form/div/div[2]/div/div/div/div/span').click()
    time.sleep(2)    
    
    # hover to month
    a = browser.find_element(by=By.XPATH, value='/html/body/div[2]/div/div/div/div/div[1]/div[7]')
    actions.move_to_element(a).click().perform()
    time.sleep(2)    
    
    #get current year and month
    now = datetime.datetime.now()
    year = now.year
    month = now.month-1
    if month <= 9:
        year_month = (str(year)+'-0'+str(month))
    else:
        year_month = (str(year)+'-'+str(month))
        
    # click on respective month
    q = browser.find_element_by_css_selector("[title^='" + year_month + "']")
    q.click()
    
    terminals = ["App", "Web"]
    for term in terminals:
        time.sleep(3)
        # Click Terminal
        browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[3]").click()
        
        if term == 'App':
            time.sleep(3)
            print("Clicked 'App'! from Terminal")
            browser.find_element_by_xpath("/html/body/div[3]/div/div/div/div[2]/div/div/div[2]").click()
            
        else:
            time.sleep(3)
            print("Clicked 'Web'! from Terminal")
            browser.find_element_by_xpath("/html/body/div[3]/div/div/div/div[2]/div/div/div[3]").click()
            
        try:
            time.sleep(3)
            browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[7]/div/button[2]").click()
            print("Refresh data!")
        except:
            browser.find_element_by_xpath("//*[@type='button'][@class='ant-btn track-click-brand-portal_product_performance-apply ant-btn-primary']").click()
            print("Apply clicked")
            
        print("Ready to download!")
        time.sleep(5)
        #browser.find_element_by_xpath("//button[@type='button'][@class='ant-btn src-components_v1-DownloadCustom---download--8LVJm track-click-brand-portal_product_performance-download src-components-Filters---download--3PIKt ant-btn-icon-only']").click()
        browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[7]/div/button[3]").click()
        
        print("Exporting data ...")
        time.sleep(30)
        list = os.listdir('/tmp/Shopee/Marketing_solutions/')
        filen_name = list[0][0:71]
        print(list[0][0:71])
        os.rename('/tmp/Shopee/Marketing_solutions/'+ filen_name + '.xlsx', 
                  '/tmp/Shopee/Marketing_solutions/'+ filen_name + '_' +term + '.xlsx')
    print("Download finished for Product")

# COMMAND ----------

def get_campaign_performance_data():
    url = "https://brandportal.shopee.com/seller/mkt/traffic/campaign"
    
    browser.execute_script("window.open('" + url + "');")
    
    time.sleep(5)
    browser.switch_to.window(browser.window_handles[-1])
    
    # Select Customize datepicker
    # Click Date range
    browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[2]/div/div/div/div/span").click()
    time.sleep(3)
    
    # hover to month
    a = browser.find_element(by=By.XPATH, value='/html/body/div[2]/div/div/div/div/div[1]/div[7]/div')
    actions.move_to_element(a).click().perform()
    time.sleep(2)
    
    # click on respective month
    q = browser.find_element_by_css_selector("[title^='" + year_month + "']")
    q.click()
    
    terminals = ["App", "Web"]
    for term in terminals:
        time.sleep(3)
        # Click Terminal
        browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[3]").click()
        
        if term == 'App':
            time.sleep(3)
            print("Clicked 'App'! from Terminal")
            browser.find_element_by_xpath("/html/body/div[3]/div/div/div/div[2]/div/div/div[2]").click()
        else:
            time.sleep(3)
            print("Clicked 'Web'! from Terminal")
            browser.find_element_by_xpath("/html/body/div[3]/div/div/div/div[2]/div/div/div[3]").click()
            
        try:
            time.sleep(3)
            browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[7]/div/button[2]").click()
            print("Refresh data!")
        except:
            browser.find_element_by_xpath("//*[@type='button'][@class='ant-btn track-click-brand-portal_product_performance-apply ant-btn-primary']").click()
            print("Apply clicked")
            
        print("Ready to download!")
        time.sleep(5)
        
        #browser.find_element_by_xpath("//button[@type='button'][@class='ant-btn src-components_v1-DownloadCustom---download--8LVJm track-click-brand-portal_product_performance-download src-components-Filters---download--3PIKt ant-btn-icon-only']").click()
        browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[7]/div/button[3]").click()
        
        print("Exporting data ...")
        time.sleep(40)
        
    print("Download finished for campaign")

# COMMAND ----------

# # Create credential
# credentials = {
#     "email": "nguyen-ngoc.hanh@unilever.com",
#     "pass": "Pizza4ps700@"
# }

# # Create folder for downloadq
# if not os.path.exists("file:/tmp/Shopee_BOT_ProductPerformance/"):
#     print("Create new folder: Shopee_BOT_ProductPerformance")
#     dbutils.fs.mkdirs("file:/tmp/Shopee_BOT_ProductPerformance/")
# else:
#     print("Download folder ready: Shopee_BOT_ProductPerformance")

# # LOG IN
# chrome_options = webdriver.ChromeOptions()
# chrome_options.add_argument('--no-sandbox')
# chrome_options.add_argument('--headless')
# chrome_options.add_argument('--auto-open-devtools-for-tabs')
# prefs = {
#     "download.prompt_for_download": False,
#     "download.default_directory": "/tmp/Shopee_BOT_ProductPerformance/",
#     "download.directory_upgrade": True,
#     "safebrowsing.enabled": False,
#     "safebrowsing.disable_download_protection": True
# }
# chrome_options.add_experimental_option("prefs", prefs)
# chrome_driver = "/tmp/chromedriver/chromedriver"

# browser = webdriver.Chrome(executable_path=ChromeDriverManager(version = latest_chrome_version).install(), options=chrome_options)

# browser.maximize_window()
# timeout = 10
# url = r'https://brandportal.shopee.com/seller/login'
# browser.get(url)


# COMMAND ----------

# Create credential
credentials = {
    "email": "nguyen-ngoc.hanh@unilever.com",
    "pass": "Pizza4ps700@"
    #"pass": "Hit@@marketshare1"
}
element_load_timeout = 10 * 2
poll_frequency = 2
    
if not os.path.exists("/temp/Shopee/Marketing_solutions/"):
    print("Create new folder: Shopee_ProductPerformance")
    dbutils.fs.mkdirs("file:/tmp/Shopee/Marketing_solutions/")
else:
    print("Download folder ready: Shopee/Marketing_solutions ")

# LOG IN
chrome_options = webdriver.ChromeOptions()
# chrome_options.add_argument('--ignore-ssl-errors=yes')
# chrome_options.add_argument('--ignore-certificate-errors')
# chrome_options.add_argument('--allow-insecure-localhost')
# chrome_options.add_argument('acceptInsecureCerts')
chrome_options.add_argument('--disable-blink-features=AutomationControlled')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--headless')
chrome_options.add_argument('--auto-open-devtools-for-tabs') 

prefs = {
    "download.prompt_for_download": False,
    "download.default_directory": "/tmp/Shopee/Marketing_solutions/",
    "download.directory_upgrade": True,
    "safebrowsing.enabled": False,
    "safebrowsing.disable_download_protection": True
}
chrome_options.add_experimental_option("prefs", prefs)
chrome_driver = "/tmp/chromedriver/chromedriver"

#browser = webdriver.Chrome(executable_path=ChromeDriverManager().install(), options=chrome_options)
browser = webdriver.Chrome(executable_path=ChromeDriverManager(version = latest_chrome_version).install(), options=chrome_options)

actions = ActionChains(browser) 

# browser = webdriver.Chrome(executable_path=ChromeDriverManager(version = latest_chrome_version).install(), options=chrome_options)

# browser.maximize_window()

#browser.maximize_window()
timeout = 10
url = r'https://brandportal.shopee.com/seller/login'
browser.get(url)

time.sleep(5)
login_ISR(browser, chrome_options, timeout, credentials)

#-------------------------------------------------------------------------------------------

#get_product_performance_data()

get_campaign_performance_data()

# time.sleep(5)
# url = "https://brandportal.shopee.com/seller/mkt/traffic/product"

# browser.execute_script("window.open('" + url + "');")

# #browser.find_element_by_xpath("//*[@id='app']/div/div[1]/ul/li[2]").click()

# time.sleep(5)
# browser.switch_to.window(browser.window_handles[-1])

# print("Clicked date Period")
# browser.find_element(by=By.XPATH, value='//*[@id="app"]/div/div[2]/div[2]/div/div/div[1]/div[2]/div/div/div[2]/div/form/div/div[2]/div/div/div/div/span').click()
# time.sleep(2)    
    
# # hover to month
# a = browser.find_element(by=By.XPATH, value='/html/body/div[2]/div/div/div/div/div[1]/div[7]')
# actions.move_to_element(a).click().perform()
# time.sleep(2)    
    
#get current year and month
# now = datetime.datetime.now()
# year = now.year
# month = now.month-1
# if month <= 9:
#     year_month = (str(year)+'-0'+str(month))
# else:
#     year_month = (str(year)+'-'+str(month))
        
# # click on respective month
# q = browser.find_element_by_css_selector("[title^='" + year_month + "']")
# q.click()
    

# terminals = ["App", "Web"]
# for term in terminals:
#     time.sleep(3)
#     # Click Terminal
#     browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[3]").click()
    
#     if term == 'App':
#         time.sleep(3)
#         print("Clicked 'App'! from Terminal")
#         browser.find_element_by_xpath("/html/body/div[3]/div/div/div/div[2]/div/div/div[2]").click()
#     else:
#         time.sleep(3)
#         print("Clicked 'Web'! from Terminal")
#         browser.find_element_by_xpath("/html/body/div[3]/div/div/div/div[2]/div/div/div[3]").click()
    
    
#     try:
#         time.sleep(3)
#         browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[7]/div/button[2]").click()
#         print("Refresh data!")
#     except:
#         browser.find_element_by_xpath("//*[@type='button'][@class='ant-btn track-click-brand-portal_product_performance-apply ant-btn-primary']").click()
#         print("Apply clicked")
        
#     print("Ready to download!")
#     time.sleep(5)
#     #browser.find_element_by_xpath("//button[@type='button'][@class='ant-btn src-components_v1-DownloadCustom---download--8LVJm track-click-brand-portal_product_performance-download src-components-Filters---download--3PIKt ant-btn-icon-only']").click()
#     browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[7]/div/button[3]").click()
    
#     print("Exporting data ...")
#     time.sleep(30)

# print("Download finished for Product")
#-----------------------------------------------------------------------------------

# url = "https://brandportal.shopee.com/seller/mkt/traffic/campaign"

# browser.execute_script("window.open('" + url + "');")

# time.sleep(5)
# browser.switch_to.window(browser.window_handles[-1])


# ### Select Customize datepicker
# # Click Date range
# browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[2]/div/div/div/div/span").click()
# time.sleep(3)

# # hover to month
# a = browser.find_element(by=By.XPATH, value='/html/body/div[2]/div/div/div/div/div[1]/div[7]/div')

# actions.move_to_element(a).click().perform()
# time.sleep(2)

# # click on respective month
# q = browser.find_element_by_css_selector("[title^='" + year_month + "']")
# q.click()

# terminals = ["App", "Web"]
# for term in terminals:
#     time.sleep(3)
#     # Click Terminal
#     browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[3]").click()
    
#     if term == 'App':
#         time.sleep(3)
#         print("Clicked 'App'! from Terminal")
#         browser.find_element_by_xpath("/html/body/div[3]/div/div/div/div[2]/div/div/div[2]").click()
#     else:
#         time.sleep(3)
#         print("Clicked 'Web'! from Terminal")
#         browser.find_element_by_xpath("/html/body/div[3]/div/div/div/div[2]/div/div/div[3]").click()
    
    
#     try:
#         time.sleep(3)
#         browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[7]/div/button[2]").click()
#         print("Refresh data!")
#     except:
#         browser.find_element_by_xpath("//*[@type='button'][@class='ant-btn track-click-brand-portal_product_performance-apply ant-btn-primary']").click()
#         print("Apply clicked")
        
#     print("Ready to download!")
#     time.sleep(5)
#     #browser.find_element_by_xpath("//button[@type='button'][@class='ant-btn src-components_v1-DownloadCustom---download--8LVJm track-click-brand-portal_product_performance-download src-components-Filters---download--3PIKt ant-btn-icon-only']").click()
#     browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[7]/div/button[3]").click()
    
#     print("Exporting data ...")
#     time.sleep(40)

# print("Download finished for campaign")

# COMMAND ----------

# MAGIC %fs ls file:/tmp/Shopee/Marketing_solutions/

# COMMAND ----------

import shutil
 
source = 'file:/tmp/Shopee/Marketing_solutions/'
destination = 'dbfs:/mnt/adls/user/naveen/Shopee/'

files_list = os.listdir('/tmp/Shopee/Marketing_solutions/')
 
#allfiles = os.listdir(source)

for file in files_list:
    dbutils.fs.cp(source+file, destination+file)

# COMMAND ----------

import os
list = os.listdir('/tmp/Shopee/Marketing_solutions/')
print(list[0][0:72])

# COMMAND ----------

# now = datetime.datetime.now()
# year = now.year
# month = now.month-1
# if month <= 9:
#     year_month = (str(year)+'-0'+str(month))
# else:
#     year_month = (str(year)+'-'+str(month))

# print(type(year_month))
# year_month1 = '2022-05'
# print(type(year_month1))

# COMMAND ----------

# MAGIC %sh fs ls

# COMMAND ----------

dbutils.fs.ls("/tmp/")

# COMMAND ----------


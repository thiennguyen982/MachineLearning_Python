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
import glob
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

def get_date_format(input_str_date):
    format = '%Y/%m/%d'
    date = datetime.datetime.strptime(input_str_date, format)
    return date.date()

# COMMAND ----------

def get_YearMonth(date):
    month = date.month
    if month <= 9:
        year_month = (str(date.year)+'-0'+str(date.month))
    else:
        year_month = (str(date.year)+'-'+str(date.month))
        
    return year_month

# COMMAND ----------

def select_FromTo_date(browser, from_date, to_date):
    
    now = datetime.datetime.now()    
    year_month = get_YearMonth(from_date)
        
    #select from_date
    if now.year == from_date.year:
        if now.month == from_date.month:
            time.sleep(2)
            
            try:
                #select from_date
                from_date_str = str(from_date)
                print(from_date_str)
                q = browser.find_element_by_css_selector("[title^='" + from_date_str + "']")
                q.click()
                time.sleep(2)
                print('selected from_date')
            except Exception as error:
                print('Month or Day is not available for the from_date')
        else:
            try:
                #click on month tab
                browser.find_element(by=By.XPATH, value='/html/body/div[2]/div/div/div/div/div[2]/div/div/div[1]/div/button[1]').click()
                time.sleep(2)
            except Exception as error:
                print('Month Tab is not available for the from_date')
            
            try:
                # click on respective month
                q = browser.find_element_by_css_selector("[title^='" + year_month + "']")
                q.click()
                time.sleep(2)
            except Exception as error:
                print('Month is not available for the from_date')
            
            try:
                #click on date
                from_date_str = str(from_date)
                q = browser.find_element_by_css_selector("[title^='" + from_date_str + "']")
                q.click()
                time.sleep(2)
                print('selected from_date')
            except Exception as error:
                print('DD is not available for the from_date')
            
    else:
        try:
            #click on year tab
            browser.find_element(by=By.XPATH, value='/html/body/div[2]/div/div/div/div/div[2]/div/div/div[1]/div/button[2]').click()
            time.sleep(2)
        except Exception as error:
                print('Year Tab is not available for the from_date')
        
        try:
            #select year
            q = browser.find_element_by_css_selector("[title^='" + str(from_date.year) + "']")
            q.click()
        except Exception as error:
                print('Year is not available for the from_date')
        
        try:
            #click on month tab
            browser.find_element(by=By.XPATH, value='/html/body/div[2]/div/div/div/div/div[2]/div/div/div[1]/div/button[1]').click()
            time.sleep(2)
        except Exception as error:
                print('Month Tab is not available for the from_date')
        
        try:
            # click on respective month
            q = browser.find_element_by_css_selector("[title^='" + year_month + "']")
            q.click()
            time.sleep(2)
        except Exception as error:
                print('Month is not available for the from_date')
        
        try:
            #click on date
            from_date_str = str(from_date)
            q = browser.find_element_by_css_selector("[title^='" + from_date_str + "']")
            q.click()
            time.sleep(2)
            print('selected from_date')
        except Exception as error:
                print('DD is not available for the from_date')
        
    #------------------------------------------------------------      
    now = datetime.datetime.now()    
    year_month = get_YearMonth(to_date)
    
    if from_date.year == to_date.year:
        try:
            #click on month tab for to_date
            browser.find_element(by=By.XPATH, value='/html/body/div[2]/div/div/div/div/div[2]/div/div/div[1]/div/button[1]').click()
            time.sleep(2)
        except Exception as error:
            print('Month Tab is not available for to_date')
            
        try:
            # click on respective month for to_date
            q = browser.find_element_by_css_selector("[title^='" + year_month + "']")
            q.click()
            time.sleep(2)
        except Exception as error:
            print('Month is not available for to_date')
            
        try:
            #click on date
            to_date_str = str(to_date)
            q = browser.find_element_by_css_selector("[title^='" + to_date_str + "']")
            q.click()
            time.sleep(2)
            print('selected to_date')
        except Exception as error:
            print('DD is not available for to_date')            
        
    else:
        try:
            #print('click on next year Arrow')
            browser.find_element(by=By.XPATH, value='/html/body/div[2]/div/div/div/div/div[2]/div/div/div[1]/button[4]').click()
            time.sleep(2)
        except Exception as error:
            print('next arrow button is not available for to_date')
           
        if from_date.month == to_date.month:
            try:
                to_date_str = str(to_date)
                q = browser.find_element_by_css_selector("[title^='" + to_date_str + "']")
                q.click()
                time.sleep(2)
                print('selected to_date')
            except Exception as error:
                print('DD is not available for to_date')
            
        else:
            try:
                #print('click on month list')
                browser.find_element(by=By.XPATH, value='/html/body/div[2]/div/div/div/div/div[2]/div/div/div[1]/div/button[1]').click()
                time.sleep(2)
            except Exception as error:
                print('Month Tab is not working for to_date')
                
            try:
                # click on respective month
                q = browser.find_element_by_css_selector("[title^='" + year_month + "']")
                q.click()
                time.sleep(2)
            except Exception as error:
                print('month is not available for to_date')
            
            try:
                #click on date
                to_date_str = str(to_date)
                q = browser.find_element_by_css_selector("[title^='" + to_date_str + "']")
                q.click()
                time.sleep(2)
                print('selected to_date')
            except Exception as error:
                print('DD is not available for to_date')

# COMMAND ----------

def get_product_performance_data(browser,from_date, to_date):
    time.sleep(5)
    url = "https://brandportal.shopee.com/seller/mkt/traffic/product"
    
    browser.execute_script("window.open('" + url + "');")
    
    time.sleep(5)
    browser.switch_to.window(browser.window_handles[-1])
    
    print("Clicked date Period")
    browser.find_element(by=By.XPATH, value='//*[@id="app"]/div/div[2]/div[2]/div/div/div[1]/div[2]/div/div/div[2]/div/form/div/div[2]/div/div/div/div/span').click()
    time.sleep(2)
    
    # hover to customize
    a = browser.find_element(by=By.XPATH, value='/html/body/div[2]/div/div/div/div/div[1]/div[10]/div')
    actions.move_to_element(a).click().perform()
    time.sleep(2)
    
    select_FromTo_date(browser, from_date, to_date)
    
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
        
        browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[7]/div/button[3]").click()
        
        print("Exporting data ...")
        time.sleep(30)
        
        filepath = '/tmp/Shopee/Marketing_solutions/'
        filen_name = max([os.path.join(filepath, f) for f in os.listdir(filepath)], key=os.path.getctime)
        print(filen_name)
        os.rename(filen_name , filen_name[:-15] + str(from_date) + '_' + str(to_date) + '_' +term + '.xlsx')   
        time.sleep(8)    
        
    print("Download finished for Product")

# COMMAND ----------

def get_campaign_performance_data(browser,from_date, to_date):
    url = "https://brandportal.shopee.com/seller/mkt/traffic/campaign"
    
    browser.execute_script("window.open('" + url + "');")
    
    time.sleep(5)
    browser.switch_to.window(browser.window_handles[-1])
    
    # Select Customize datepicker
    # Click Date range
    browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[2]/div/div/div/div/span").click()
    time.sleep(3)
    
    # hover to customize
    a = browser.find_element(by=By.XPATH, value='/html/body/div[2]/div/div/div/div/div[1]/div[10]/div')
    actions.move_to_element(a).click().perform()
    time.sleep(2)
    
    select_FromTo_date(browser, from_date, to_date)
    
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
        time.sleep(10)
        
        browser.find_element_by_xpath("//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[7]/div/button[3]").click()
        
        print("Exporting data ...")
        time.sleep(30)
        
        filepath = '/tmp/Shopee/Marketing_solutions/'        
        filen_name = max([os.path.join(filepath, f) for f in os.listdir(filepath)], key=os.path.getctime)
        print(filen_name)
        os.rename(filen_name , filen_name[:-15] + str(from_date) + '_' + str(to_date) + '_' +term + '.xlsx')
        time.sleep(10)
                
    print("Download finished for campaign")

# COMMAND ----------

# # Create credential
# credentials = {
#     "email": "nguyen-ngoc.hanh@unilever.com",
#     "pass": "Pizza4ps700@"
#     #"pass": "Hit@@marketshare1"
# }
# element_load_timeout = 10 * 2
# poll_frequency = 2
    
# if not os.path.exists("/temp/Shopee/Marketing_solutions/"):
#     print("Create new folder: Shopee")
#     dbutils.fs.mkdirs("file:/tmp/Shopee/Marketing_solutions/")
# else:
#     print("Download folder ready: Shopee/Marketing_solutions")

# # LOG IN
# chrome_options = webdriver.ChromeOptions()
# chrome_options.add_argument('--disable-blink-features=AutomationControlled')
# chrome_options.add_argument('--no-sandbox')
# chrome_options.add_argument('--headless')
# chrome_options.add_argument('--auto-open-devtools-for-tabs') 

# prefs = {
#     "download.prompt_for_download": False,
#     "download.default_directory": "/tmp/Shopee/Marketing_solutions/",
#     "download.directory_upgrade": True,
#     "safebrowsing.enabled": False,
#     "safebrowsing.disable_download_protection": True
# }
# chrome_options.add_experimental_option("prefs", prefs)
# chrome_driver = "/tmp/chromedriver/chromedriver"

# #browser.close()
# browser = webdriver.Chrome(executable_path=ChromeDriverManager(version = latest_chrome_version).install(), options=chrome_options)

# actions = ActionChains(browser) 

# input_from_date = '2022/05/01'
# input_to_date = '2022/05/28'

# from_date = get_date_format(input_from_date)
# to_date = get_date_format(input_to_date)

# print(from_date, to_date)

# if from_date <= to_date:
    
#     timeout = 10
#     url = r'https://brandportal.shopee.com/seller/login'
#     browser.get(url)
    
#     time.sleep(5)
#     login_ISR(browser, chrome_options, timeout, credentials)
    
#     get_product_performance_data(browser,from_date, to_date)
#     #time.sleep(10)
#     #get_campaign_performance_data(browser,from_date, to_date)
    
# else:
#     print("from_date should be less than to_date")

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
    print("Create new folder: Shopee")
    dbutils.fs.mkdirs("file:/tmp/Shopee/Marketing_solutions/")
else:
    print("Download folder ready: Shopee/Marketing_solutions")

# LOG IN
chrome_options = webdriver.ChromeOptions()
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

#browser.close()
browser = webdriver.Chrome(executable_path=ChromeDriverManager(version = latest_chrome_version).install(), options=chrome_options)

actions = ActionChains(browser) 

input_from_date = '2022/05/01'
input_to_date = '2022/05/28'

from_date = get_date_format(input_from_date)
to_date = get_date_format(input_to_date)

print(from_date, to_date)

if from_date <= to_date:
    
    timeout = 10
    url = r'https://brandportal.shopee.com/seller/login'
    browser.get(url)
    
    time.sleep(5)
    login_ISR(browser, chrome_options, timeout, credentials)
    
    #get_product_performance_data(browser,from_date, to_date)
    #time.sleep(10)
    #get_campaign_performance_data(browser,from_date, to_date)
    
    def call_data_set_method(dataset_name):
        print('in switch:', dataset_name)
        switch={            
            'product': print('product',get_product_performance_data(browser,from_date, to_date)),
                       #get_product_performance_data(browser,from_date, to_date),
            
            'campaign': get_campaign_performance_data(browser,from_date, to_date),
            
            'sales': get_sales_performance_data(browser,from_date, to_date),
            }
        return switch.get(dataset_name,'Choose one of the following data set name: product, campaign, sales')

    print("Enter data set name which you want to download (product, campaign or sales): ")
    dbutils.widgets.text("Dataset Name",'')
    dataset_name = dbutils.widgets.get("Dataset Name")
    print(dataset_name)
    call_data_set_method(dataset_name)
    
else:
    print("from_date should be less than to_date")

# COMMAND ----------

# dbutils.widgets.text("Data Set Name", "Enter data set name which you want to download (product, campaign or sales): ")
# dataset_name = dbutils.widgets.get("Data Set Name")
# print(dataset_name)

# COMMAND ----------

filepath = '/tmp/Shopee/Marketing_solutions/'
len(os.listdir(filepath))

# COMMAND ----------

#ant-notification-notice-message

# COMMAND ----------

# MAGIC %fs ls file:/tmp/Shopee/Marketing_solutions/

# COMMAND ----------

filepath = '/tmp/Shopee/Marketing_solutions/'
files = os.listdir(filepath)
print(files[0])
#print(files[0][:-15])
        
filen_name = max([os.path.join(filepath, f) for f in os.listdir(filepath)], key=os.path.getctime)
#os.rename(filen_name , filen_name[:-15] + str(from_date) + '_' + str(to_date) + '_' +term + '.xlsx')
#os.rename(filepath + files[0] , filepath + files[0][:-15] + str(from_date) + '_' + str(to_date) + '_' +'App' + '.xlsx')
#os.rename(files[0], files[0]) #+ str(from_date) + '_' + str(to_date) + '_' +'App' + '.xlsx')
print(filen_name)

# COMMAND ----------

import os, glob

#Loop Through the folder projects all files and deleting them one by one
for file in glob.glob("/tmp/Shopee/Marketing_solutions/*"):
    os.remove(file)
    print("Deleted " + str(file))

# COMMAND ----------

# MAGIC %fs rm file:/tmp/Shopee/Marketing_solutions/Off-platform_Traffic_Report---Product_Performance---Unilever-2021-06-01_20222021-06-01_20222021-06-01_2022-05-30_App.xlsx

# COMMAND ----------

#del str

# COMMAND ----------

import shutil
 
source = 'file:/tmp/Shopee/Marketing_solutions/'
destination = 'dbfs:/mnt/adls/user/naveen/Shopee/'

files_list = os.listdir('/tmp/Shopee/Marketing_solutions/')
 
#allfiles = os.listdir(source)

for file in files_list:
    dbutils.fs.cp(source+file, destination+file)

# COMMAND ----------

# input_from_date = '2021/03/31'
# input_to_date = '2021/02/01'

# from_date = get_date_format(input_from_date)
# to_date = get_date_format(input_to_date)

# if from_date <= to_date and (to_date-from_date).days <= 365:
#     print(from_date, to_date)
#     print((to_date-from_date).days)

# COMMAND ----------

# filepath = '/tmp/Shopee/Marketing_solutions/'
# filen_name = max([os.path.join(filepath, f) for f in os.listdir(filepath)], key=os.path.getmtime)
# filen_name = '/tmp/Shopee/Marketing_solutions/Off-platform_Traffic_Report---Product_Performance---Unilever-2022.05.30_Web.xlsx'
# print(filen_name[:-5])
# #os.rename(filen_name , filen_name[:-15] + str(from_date) + '_' + str(to_date) + '_' +term + '.xlsx')

# COMMAND ----------

dbutils.fs.ls("/tmp/")

# COMMAND ----------

# filen_name = max([os.path.join(filepath, f) for f in os.listdir(filepath)], key=os.path.getmtime)
# filen_name1 = filen_name[:-15]
# print(filen_name1)
# #os.rename('/tmp/Shopee/Marketing_solutions/'+ filen_name + '.xlsx', '/tmp/Shopee/Marketing_solutions/'+ filen_name + '_' +term + '.xlsx')

# COMMAND ----------


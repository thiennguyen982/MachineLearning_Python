# Databricks notebook source
pip install webdriver_manager selenium --upgrade chromedriver_autoinstaller pandas --upgrade

# COMMAND ----------

# MAGIC %sh cat /etc/*-release

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo add-apt-repository universe
# MAGIC sudo add-apt-repository multiverse
# MAGIC sudo apt update
# MAGIC

# COMMAND ----------

# MAGIC %sh 
# MAGIC apt list --upgradable
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt upgrade -y
# MAGIC

# COMMAND ----------

import requests

def get_chrome_latest_release():
    url = "https://chromedriver.storage.googleapis.com/LATEST_RELEASE"
    response = requests.request("GET", url)
    return response.text

latest_chrome_version = get_chrome_latest_release()
latest_chrome_version

# COMMAND ----------

# MAGIC %sh 
# MAGIC latest_chrome_version = `curl -sS https://chromedriver.storage.googleapis.com/LATEST_RELEASE`
# MAGIC url_download = 'https://chromedriver.storage.googleapis.com/'${latest_chrome_version}'/chromedriver_linux64.zip'
# MAGIC echo $url_download
# MAGIC
# MAGIC wget url_download /tmp/chromedriver_linux64.zip
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
output = completed_process.stdout
output

# COMMAND ----------

import re
regex = "ChromeDriver (.*) .*"
chrome_version = re.search(regex, output).group(1)
chrome_version

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
import glob

from selenium import webdriver

from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from webdriver_manager.chrome import ChromeDriverManager

logging.basicConfig(level=logging.INFO,
                    filename="error.log",
                    format='%(asctime)s %(message)s')


# COMMAND ----------

# MAGIC %md
# MAGIC ### DEFINE FUNCTION

# COMMAND ----------

# MAGIC %md
# MAGIC #### Login func

# COMMAND ----------

# Define funtions
def download_wait(path_to_downloads):
    seconds = 0
    dl_wait = True
    while dl_wait and seconds < 20:
        time.sleep(1)
        dl_wait = False
        for fname in os.listdir(path_to_downloads):
            if fname.endswith('.crdownload'):
                dl_wait = True
        seconds += 1
    return seconds

# Login function
def login_ISR(driver, options, timeout, credentials):
    check = None
    while check is None:
        try:
            WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.ID, 'app')))
            driver.find_element(By.ID,"email").clear()
            driver.find_element(By.ID,"email").send_keys(credentials['email'])
            # driver.find_element(By.ID,'email').send_keys(credentials['email'])
            driver.find_element(By.XPATH,"//*[@id='password']").clear()
            driver.find_element(By.XPATH,"//*[@id='password']").send_keys(credentials['pass'])
            driver.find_element(By.XPATH,"//*[@id='app']/div/div/div/div/div[2]/div/div/div[3]/form/div[3]/div/div/button").click()
            
            WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH, "//*[@id='app']/div/div/div/div/div[2]/div/div/div[2]/button")))
            browser.find_element(By.XPATH,"//*[@id='app']/div/div/div/div/div[2]/div/div/div[2]/button").click() ### Click GO
            check = 'Sucessfully login!'
            print(check)
        except:
            print("Cannot access! Browser closing ...")
            browser.close()
            browser.quit()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Refresh page function
# MAGIC

# COMMAND ----------

### Refresh
def refresh_page():
  browser.refresh()
  time.sleep(3)
  ### Select Customize datepicker
  date_filter = WebDriverWait(browser, 15).until(EC.element_to_be_clickable((By.XPATH, "//*[@class='ant-input'][@type='text']")))
  date_filter.click()
  try:
    browser.find_element(By.XPATH,"//*[text()='Customize']").click()
  except:
    customize_button = WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.XPATH, "//*[@class='src-components-DatePicker---optionLeft--_xEbk']")))
    customize_button.click()
    print("Else clicked!")
  print("Customize button clicked!")

#   ### Select Customize datepicker
#   date_filter = WebDriverWait(browser, 15).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='root']/section/section/section/main/div/div[2]/div/div[3]/div/div/div[1]/div[2]/span/input")))
#   date_filter.click()
#   time.sleep(1)
#   browser.find_element(By.XPATH,"//*[text()='Customize']").click()
#   browser.find_element(By.XPATH,"//*[text()='Customize']").click()
#   print("Customize button clicked!")
#   time.sleep(3)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Download daily single func

# COMMAND ----------

# Download daily files function
class download_files:
    @classmethod
    def __init__(cls, from_date, to_date):
        cls.from_date = from_date
        cls.to_date = to_date

    @classmethod
    def execute(cls):
        dates_range = [cls.from_date, cls.to_date]
        start, end = [datetime.datetime.strptime(_, "%Y-%m-%d") for _ in dates_range]
        # Create list of year-month combination
        if start == end:
            list_ym = [start.strftime("%Y-%m")]
        elif start < end:
            list_ym = list(OrderedDict(((start + timedelta(_)).strftime("%Y-%m"), None) for _ in range((end - start).days)).keys())
        else:
            print("Please choose rational period!")
        # Create list of dates
        if start == end:
            list_dates = [datetime.datetime.strftime(start, "%Y-%m-%d")]
        elif start < end:
            list_dates = [datetime.datetime.strftime(start + timedelta(days=x), "%Y-%m-%d") for x in range((end - start).days + 1)]
        else:
            print("Please choose rational period!")

        year = []
        month = []
        for i in list_ym:
            j = i.split("-")
            year.append(j[0])
            month.append(j[1])
        year_month = list(zip(year, month))

        for i in range(0, len(year_month)):
            year = year_month[i][0]
            month = year_month[i][1]
            # Create vars
            current_datetime = datetime.datetime.today()
            current_date = datetime.datetime.strftime(current_datetime, '%Y-%m-%d')
            yesterday = datetime.datetime.strftime(current_datetime - datetime.timedelta(days=1), '%Y-%m-%d')
            current_year = datetime.datetime.today().year
            current_month = datetime.datetime.strftime(datetime.datetime.today(), '%b')
            current_month_num = int(datetime.datetime.strftime(datetime.datetime.today(), '%m'))
            today = int(datetime.datetime.strftime(datetime.datetime.today(), '%d'))
            # Create button and value
            try:
                current_month_button = browser.find_element(By.XPATH, "//*[@type='button'][@class='ant-picker-month-btn']")
#                 current_month_button = browser.find_element(By.XPATH, "//*[text()='Aug']")
            except:
                current_month_button = browser.find_element(By.XPATH, "/html/body/div[2]/div/div/div/div/div[2]/div/div/div[1]/div/button[2]")
            else:
                print("Cannot find Current month button!!!")
            prev_month_button = browser.find_element(By.XPATH, "//button[@type='button'][@class='ant-picker-header-prev-btn']")
            next_month_button = browser.find_element(By.XPATH, "//button[@type='button'][@class='ant-picker-header-next-btn']")
            current_year_button = browser.find_element(By.XPATH, "//button[@class='ant-picker-year-btn']")
            prev_year_button = browser.find_element(By.XPATH, "//button[@class='ant-picker-header-super-prev-btn']")
            next_year_button = browser.find_element(By.XPATH, "//button[@class='ant-picker-header-super-next-btn']")
            date_range_button = browser.find_element(By.XPATH, "//input[@class='ant-input'] [@type = 'text']")
            all_dates = browser.find_elements(By.XPATH, "//table[@class='ant-picker-content']//td")
            

            def matching_year():
                # Adjust current Year to match Chosen Year
                current_year_value = current_year_button.text
                current_month_value = current_month_button.text
                # date_range_value = browser.find_element(By.XPATH, "//input[@class='ant-input'] [@type = 'text']").get_attribute('value')
                if current_year_value < year:
                    while current_year_value < year:
                        browser.execute_script("arguments[0].click();", next_year_button)
                        current_year_value = current_year_button.text
                    else:
                        print("*** Match year = " + current_year_value)
                elif current_year_value > year:
                    while current_year_value > year:
                        browser.execute_script("arguments[0].click();", prev_year_button)
                        current_year_value = current_year_button.text
                        print(current_year_value)
                    else:
                        print("*** Match year = " + current_year_value)
                else:
                    print("*** Year is already matched = " + current_year_value)

            def matching_month():
                # Adjust current Month to match Chosen Month
                current_month_value = str(datetime.datetime.strptime(current_month_button.text, "%b").month)
                current_year_value = current_year_button.text
                if int(current_month_value) < int(month):
                    while int(current_month_value) < int(month):
                        next_month_button.click()
                        current_month_value = str(datetime.datetime.strptime(current_month_button.text, "%b").month)
                    else:
                        print("   Match month = " + current_month_value)
                elif int(current_month_value) > int(month):
                    while int(current_month_value) > int(month):
                        prev_month_button.click()
                        current_month_value = str(datetime.datetime.strptime(current_month_button.text, "%b").month)
                    else:
                        print("  Match month = " + current_month_value)
                else:
                    pass

            def download():
                current_month_value = str(datetime.datetime.strptime(current_month_button.text, "%b").month)
                if int(current_month_value) < int(month):
                    while int(current_month_value) < int(month):
                        next_month_button.click()
                        current_month_value = str(datetime.datetime.strptime(current_month_button.text, "%b").month)
                    else:
                        print("Match pairs year-month = " + year + '-' + month)
                elif int(current_month_value) > int(month):
                    while int(current_month_value) > int(month):
                        prev_month_button.click()
                        current_month_value = str(datetime.datetime.strptime(current_month_button.text, "%b").month)
                    else:
                        print("Match pairs year-month = " + year + '-' + month)
                else:
                    print("Match pairs year-month = " + year + '-' + month)
                # Get all dates in current customised pannel

                all_dates = browser.find_elements(By.XPATH, "//table[@class='ant-picker-content']//td")
                for date_element in all_dates:
                    date = date_element.get_attribute("title")
                    for chosen_date in list_dates:
                        if (date == chosen_date) & (datetime.datetime.strptime(date, "%Y-%m-%d").strftime('%m') == month):
                            print('Selecting date: ' + chosen_date)
                            browser.execute_script("arguments[0].click();", date_element)
                            browser.execute_script("arguments[0].click();", date_element)
                            print("Clicked!")
                            # Click Apply + download
                            browser.find_element(By.XPATH,"//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[3]/div/div/div/div[2]/button[2]").click()
                            print("Refresh clicked")
                            time.sleep(3)
                            
                            browser.find_element(By.XPATH,"//*[@type='button'][@class = 'ant-btn src-components_v1-DownloadCustom---download--8LVJm track-click-brand-portal-sales-shop-dashboard-gross-data-filter-download src-components-Filters---download--3PIKt ant-btn-icon-only']").click()
                            print("Export data ...!")
                            time.sleep(5)
                            # Chooose customize date again for next round
                            date_filter = WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.XPATH, "//input [@class='ant-input'][@type = 'text']")))
                            date_filter.click()
                            print("Date input clicked!")
                            time.sleep(2)
                            try:
                              customize_button = WebDriverWait(browser, 15).until(EC.element_to_be_clickable((By.XPATH, "//*[@class='src-components-DatePicker---optionLeft--_xEbk']")))
                              customize_button.click()
                              print("Custiomize clicked!")
                              time.sleep(1)
                            except:
                              browser.find_element(By.XPATH,"//*[text()='Customize']").click()
                            else:
                              print("Custiomize clicked! \n")
                        else:
                            pass
#                     else:
#                         pass
            matching_year()
            matching_month()
            download()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Download period report func

# COMMAND ----------

# Download period report function
class download_period_file:
    @classmethod
    def __init__(cls, from_date, to_date):
        cls.from_date = from_date
        cls.to_date = to_date

    @classmethod
    def execute(cls):
        dates_range = [cls.from_date, cls.to_date]
        start, end = [datetime.datetime.strptime(_, "%Y-%m-%d") for _ in dates_range]
        # Create list of year-month combination
        if start == end:
            list_ym = [start.strftime("%Y-%m")]
        elif start < end:
            list_ym = list(OrderedDict(((start + timedelta( _ )).strftime("%Y-%m"), None) for _ in range((end - start).days)).keys())
        else:
            print("Please choose rational period!")
        # Create list of dates
        if start == end:
            list_dates = [datetime.datetime.strftime(start, "%Y-%m-%d")]
        elif start < end:
            list_dates = [datetime.datetime.strftime(start + timedelta(days=x), "%Y-%m-%d") for x in range((end - start).days + 1)]
        else:
            print("Please choose rational period!")
            
        print("List year_month:")
        print(*list_ym, sep = ", ") 

        year = []
        month = []
        for i in list_ym:
            j = i.split("-")
            year.append(j[0])
            month.append(j[1])
        year_month = list(zip(year, month))

        for i in range(0, len(year_month)):
            year = year_month[i][0]
            month = year_month[i][1]
            # Create vars
            current_datetime = datetime.datetime.today()
            current_date = datetime.datetime.strftime(current_datetime, '%Y-%m-%d')
            yesterday = datetime.datetime.strftime(current_datetime - datetime.timedelta(days=1), '%Y-%m-%d')
            current_year = datetime.datetime.today().year
            current_month = datetime.datetime.strftime(datetime.datetime.today(), '%b')
            current_month_num = int(datetime.datetime.strftime(datetime.datetime.today(), '%m'))
            today = int(datetime.datetime.strftime(datetime.datetime.today(), '%d'))
            # Create button and value
            current_month_button = browser.find_element(By.XPATH, "//*[@type='button'][@class='ant-picker-month-btn']")
            prev_month_button = browser.find_element(By.XPATH, "//*[@type='button'][@class='ant-picker-header-prev-btn']")
            next_month_button = browser.find_element(By.XPATH, "//*[@type='button'][@class='ant-picker-header-next-btn']")
            current_year_button = browser.find_element(By.XPATH, "//*[@class='ant-picker-year-btn']")
            prev_year_button = browser.find_element(By.XPATH, "//*[@class='ant-picker-header-super-prev-btn']")
            next_year_button = browser.find_element(By.XPATH, "//*[@class='ant-picker-header-super-next-btn']")
            date_range_button = browser.find_element(By.XPATH, "//input[@class='ant-input'] [@type = 'text']")
            all_dates = browser.find_elements(By.XPATH, "//table[@class='ant-picker-content']//td")

            def matching_year():
                # Adjust current Year to match Chosen Year
                current_year_value = current_year_button.text
                current_month_value = current_month_button.text
                # date_range_value = browser.find_element(By.XPATH, "//input[@class='ant-input'] [@type = 'text']").get_attribute('value')
                if current_year_value < year:
                    while current_year_value < year:
                        browser.execute_script("arguments[0].click();", next_year_button)
                        current_year_value = current_year_button.text
                    else:
                        print("*** Match year = " + current_year_value)
                elif current_year_value > year:
                    while current_year_value > year:
                        browser.execute_script("arguments[0].click();", prev_year_button)
                        current_year_value = current_year_button.text
                        print(current_year_value)
                    else:
                        print("*** Match year = " + current_year_value)
                else:
                    print("*** Year is already matched = " + current_year_value)

            def matching_month():
                # Adjust current Month to match Chosen Month
                current_month_value = str(datetime.datetime.strptime(current_month_button.text, "%b").month)
                current_year_value = current_year_button.text
                if int(current_month_value) < int(month):
                    while int(current_month_value) < int(month):
                        next_month_button.click()
                        current_month_value = str(datetime.datetime.strptime(current_month_button.text, "%b").month)
                    else:
                        print("   Match month = " + current_month_value)
                elif int(current_month_value) > int(month):
                    while int(current_month_value) > int(month):
                        prev_month_button.click()
                        current_month_value = str(datetime.datetime.strptime(current_month_button.text, "%b").month)
                    else:
                        print("  Match month = " + current_month_value)
                else:
                    pass

            def download():
                print("\n Starting download from:" + str(cls.from_date) + " to " + str(cls.to_date))
                current_month_value = str(datetime.datetime.strptime(current_month_button.text, "%b").month)
                if int(current_month_value) < int(month):
                    while int(current_month_value) < int(month):
                        next_month_button.click()
                        current_month_value = str(datetime.datetime.strptime(current_month_button.text, "%b").month)
                    else:
                        print("\n Match pairs year-month = " + year + '-' + month)
                elif int(current_month_value) > int(month):
                    while int(current_month_value) > int(month):
                        prev_month_button.click()
                        current_month_value = str(datetime.datetime.strptime(current_month_button.text, "%b").month)
                    else:
                        print("\n Match pairs year-month = " + year + '-' + month)
                else:
                    print("\n Match pairs year-month = " + year + '-' + month)
                # Get all dates in current customised pannel
                all_dates = browser.find_elements(By.XPATH, "//table[@class='ant-picker-content']//td")
                for date_element in all_dates:
                    date = date_element.get_attribute("title")
                    is_in_view = date_element.get_attribute("class")
                    if (date == cls.from_date == cls.to_date and is_in_view.startswith('ant-picker-cell ant-picker-cell-in-view')):
                        browser.execute_script("arguments[0].click();", date_element)
                        print('Selecting start date: ' + date)
                        time.sleep(3)
                        browser.execute_script("arguments[0].click();", date_element)
                        print('Selecting end date: ' + date)
                        time.sleep(3)
                        # Click Apply + download
#                         browser.find_element(By.XPATH,"//button[@type='button'][@class='ant-btn track-click-brand-portal-sales-shop-dashboard-gross-data-filter-apply ant-btn-primary']").click()
                        browser.find_element(By.CSS_SELECTOR,"button[class='ant-btn track-click-brand-portal-sales-shop-dashboard-gross-data-filter-apply ant-btn-primary']").click()
                        print("Refresh clicked")
                        time.sleep(5)
                        
                        browser.find_elements(By.XPATH, "//*[@class='ant-btn src-components_v1-DownloadCustom---download--8LVJm track-click-brand-portal-sales-shop-dashboard-gross-data-filter-download src-components-Filters---download--3PIKt ant-btn-icon-only']")[0].click() 
                        print("Export data ...!")
                        time.sleep(5)
              
                        ### Wait until file is downloaded
                        begin_date = cls.from_date.replace('-', '.')
                        end_date = cls.to_date.replace('-', '.')
                        pattern_period = "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx".format(begin_date, end_date)
                        wait_sec = 0
                        dl_wait = True
                        while dl_wait and wait_sec < 150:
                          try:
                              dbutils.fs.ls('file:/tmp/Shopee_BOT_SalesPerformance/' + pattern_period)
                              print("Downloaded " + "period from: " + cls.from_date + " to " + cls.to_date + " report! ...")
                              dl_wait = False
                          except Exception as e:
                              if 'java.io.FileNotFoundException' in str(e):
                                time.sleep(1)
                                wait_sec +=1
                              else:
                                raise

                        ### Chooose customize date again for next round
                        date_filter = WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.XPATH, "//input [@class='ant-input']")))
                        date_filter.click()
                        try:
                          customize_element = browser.find_element(By.CSS_SELECTOR,"div[class='src-components-DatePicker---option--fCbbJ src-components-DatePicker---active--2Xtb4'] div[class='src-components-DatePicker---optionLeft--_xEbk']")
                          browser.execute_script("arguments[0].click();", customize_element)
                        except:
                          browser.find_element(By.XPATH,"div[class='src-components-DatePicker---option--fCbbJ src-components-DatePicker---active--2Xtb4'] div[class='src-components-DatePicker---optionLeft--_xEbk']").click()
                    elif (date == cls.from_date and date != cls.to_date and is_in_view.startswith('ant-picker-cell ant-picker-cell-in-view')):
                        browser.execute_script("arguments[0].click();", date_element)
                        print('Selecting start date: ' + date)
                        time.sleep(3)
                    elif (date == cls.to_date and date != cls.from_date and is_in_view.startswith('ant-picker-cell ant-picker-cell-in-view')):
                        browser.execute_script("arguments[0].click();", date_element)
                        print('Selecting end date: ' + date)
                        time.sleep(3)
                        ### Click Apply + download
                        browser.find_element(By.XPATH,"//button[@class='ant-btn track-click-brand-portal-sales-shop-dashboard-gross-data-filter-apply ant-btn-primary']").click()
                        print("Refresh clicked")
                        time.sleep(3)
                        try:
                            browser.find_element(By.XPATH,"//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[3]/div/div/div/div[2]/button[3]").click()
                        except:
                            browser.find_element(By.XPATH,"button[class='ant-btn src-components_v1-DownloadCustom---download--8LVJm track-click-brand-portal-sales-shop-dashboard-gross-data-filter-download src-components-Filters---download--3PIKt ant-btn-icon-only']").click() 
                        print("Export data ...!")
                        time.sleep(5)
                        
                        ### Wait until file is downloaded
                        begin_date = cls.from_date.replace('-', '.')
                        end_date = cls.to_date.replace('-', '.')
                        pattern_period = "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx".format(begin_date, end_date)
                        wait_sec = 0
                        dl_wait = True
                        while dl_wait and wait_sec < 150:
                          try:
                              dbutils.fs.ls('file:/tmp/Shopee_BOT_SalesPerformance/' + pattern_period)
                              print("Downloaded " + "period from: " + cls.from_date + " to " + cls.to_date + " report! ... \n")
                              dl_wait = False
                          except Exception as e:
                              if 'java.io.FileNotFoundException' in str(e):
                                time.sleep(1)
                                wait_sec +=1
                              else:
                                raise

                        ### Chooose customize date again for next round
                        time.sleep(3)
                        date_filter = WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.XPATH, "//span[@class='shopee-react-icon sp-icon sp-icon-calendar']//*[name()='svg']")))
                        date_filter.click()
                        print("Date input clicked!")
                        time.sleep(3)
                        ### Customize date
                        customize_element = browser.find_element(By.XPATH,"//*[@class='src-components-DatePicker---optionLeft--_xEbk']")
                        browser.execute_script("arguments[0].click();", customize_element)
                        print('Customized Clicked for next round! \n')
                        
                    else:
                        pass
            matching_year()
            matching_month()
            download()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Checking existing file func

# COMMAND ----------

# Define funtions
def file_exists(path):
  try:
    dbutils.fs.ls(path)
    print("File is downloaded sucessfully")
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise

# COMMAND ----------

# MAGIC %md
# MAGIC #### Download yesterday report func

# COMMAND ----------

def download_yesterday():
    # Choose Yesterday
    browser.find_element(By.XPATH,"/html/body/div[2]/div/div/div/div/div[1]/div[1]").click()
    print("Clicked 'Yesterday'!")

    # Click Apply + download
    browser.find_element(By.XPATH,"//button[@type='button'][@class='ant-btn track-click-brand-portal-sales-shop-dashboard-gross-data-filter-apply ant-btn-primary']").click()
    print("Apply clicked")
    try:
        browser.find_element(By.XPATH,"//*[@id='root']/section/section/section/main/div/div[2]/div/div[3]/div/div/div[2]/button[3]").click()
    except:
        browser.find_element(By.XPATH,"//*[@type='button'][@class= 'ant-btn src-components_v1-DownloadCustom---download--8LVJm track-click-brand-portal-sales-shop-dashboard-gross-data-filter-download src-components-Filters---download--3PIKt ant-btn-icon-only']").click() 
    print("Export data ...!")
    time.sleep(5)
    # Wait until file is downloaded
    yesterday = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=1), '%Y.%m.%d')
    pattern_yesterday = "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx".format(yesterday, yesterday)
    
    wait_sec = 0
    dl_wait = True
    while dl_wait and wait_sec < 90:
      try:
          dbutils.fs.ls('file:/tmp/Shopee_BOT_SalesPerformance/' + pattern_yesterday)
          print('Downloaded yesterday report! \n')
          dl_wait = False
      except Exception as e:
          if 'java.io.FileNotFoundException' in str(e):
            time.sleep(1)
            wait_sec +=1
          else:
            raise

#     ### Chooose customize date again for next round
#     date_filter = WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='root']/section/section/section/main/div/div[2]/div/div[2]/div/div/div[1]/div[3]/span/span/div/span")))
#     date_filter.click()
#     try:
#       customize_element = browser.find_element(By.XPATH,"//*[@class = 'option___3NWy9 active___3WaM6']")
#       browser.execute_script("arguments[0].click();", customize_element)
#     except:
#       browser.find_element(By.XPATH,"//*[text()='Customize']").click()
      
    ### Chooose customize date again for next round
    date_filter = WebDriverWait(browser, 15).until(EC.element_to_be_clickable((By.XPATH, "//input [@class='ant-input'][@type = 'text']")))
    date_filter.click()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Download last 7 days report func

# COMMAND ----------

def download_last7days():
    # Choose Last 7 days
    browser.find_element(By.XPATH,"/html/body/div[2]/div/div/div/div/div[1]/div[2]").click()
    print("Clicked 'Last 7 days'!")

    # Click Apply + download
    browser.find_element(By.XPATH,"//button[@type='button'][@class='ant-btn track-click-brand-portal-sales-shop-dashboard-gross-data-filter-apply ant-btn-primary']").click()
    print("Apply clicked")
    try:
        browser.find_element(By.XPATH,"//*[@id='root']/section/section/section/main/div/div[2]/div/div[3]/div/div/div[2]/button[3]").click()
    except:
        browser.find_element(By.XPATH,"//*[@type='button'][@class= 'ant-btn src-components_v1-DownloadCustom---download--8LVJm track-click-brand-portal-sales-shop-dashboard-gross-data-filter-download src-components-Filters---download--3PIKt ant-btn-icon-only']").click() 
    print("Export data ...!")
    time.sleep(10)
    # Wait until file is downloaded
    last_7day = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=7), '%Y.%m.%d')
    yesterday = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=1), '%Y.%m.%d')
    pattern_last_7days = "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx".format(last_7day, yesterday)
    wait_sec = 0
    dl_wait = True
    while dl_wait and wait_sec < 90:
      try:
          dbutils.fs.ls('file:/tmp/Shopee_BOT_SalesPerformance/' + pattern_last_7days)
          print('Downloaded Last 7 days report! \n')
          dl_wait = False
      except Exception as e:
          if 'java.io.FileNotFoundException' in str(e):
            time.sleep(1)
            wait_sec +=1
          else:
            raise
#     ### Chooose customize date again for next round
#     date_filter = WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='root']/section/section/section/main/div/div[2]/div/div[2]/div/div/div[1]/div[3]/span/span/div/span")))
#     date_filter.click()
#     try:
#       customize_element = browser.find_element(By.XPATH,"//*[@class = 'option___3NWy9 active___3WaM6']")
#       browser.execute_script("arguments[0].click();", customize_element)
#     except:
#       browser.find_element(By.XPATH,"//*[text()='Customize']").click()
    
    # Chooose customize date again for next round
    date_filter = WebDriverWait(browser, 15).until(EC.element_to_be_clickable((By.XPATH,"//input [@class='ant-input'][@type = 'text']")))
    date_filter.click()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Download last 30 days report func

# COMMAND ----------

def download_last30days():
    # Choose Last 30 Days
    browser.find_element(By.XPATH,"/html/body/div[2]/div/div/div/div/div[1]/div[3]").click()
    print("Clicked 'Last 30 days'!")

    # Click Apply + download
    browser.find_element(By.XPATH,"//button[@type='button'][@class='ant-btn track-click-brand-portal-sales-shop-dashboard-gross-data-filter-apply ant-btn-primary']").click()
    print("Apply clicked")
    try:
        browser.find_element(By.XPATH,"//*[@id='root']/section/section/section/main/div/div[2]/div/div[3]/div/div/div[2]/button[3]").click()
    except:
        browser.find_element(By.XPATH,"//*[@type='button'][@class= 'ant-btn src-components_v1-DownloadCustom---download--8LVJm track-click-brand-portal-sales-shop-dashboard-gross-data-filter-download src-components-Filters---download--3PIKt ant-btn-icon-only']").click() 
    print("Export data ...!")
    time.sleep(15)
    # Wait until file is downloaded
    last_30day = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=30), '%Y.%m.%d')
    yesterday = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=1), '%Y.%m.%d')
    pattern_last_30days = "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx".format(last_30day, yesterday)
    wait_sec = 0
    dl_wait = True
    while dl_wait and wait_sec < 90:
      try:
          dbutils.fs.ls('file:/tmp/Shopee_BOT_SalesPerformance/' + pattern_last_30days)
          print('Downloaded Last 30 days report! \n')
          dl_wait = False
      except Exception as e:
          if 'java.io.FileNotFoundException' in str(e):
            time.sleep(1)
            wait_sec +=1
          else:
            raise

#     ### Chooose customize date again for next round
#     date_filter = WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='root']/section/section/section/main/div/div[2]/div/div[2]/div/div/div[1]/div[3]/span/span/div/span")))
#     date_filter.click()
#     try:
#       customize_element = browser.find_element(By.XPATH,"//*[@class = 'option___3NWy9 active___3WaM6']")
#       browser.execute_script("arguments[0].click();", customize_element)
#     except:
#       browser.find_element(By.XPATH,"//*[text()='Customize']").click()

    # Chooose customize date again for next round
    date_filter = WebDriverWait(browser, 15).until(EC.element_to_be_clickable((By.XPATH,"//input [@class='ant-input'][@type = 'text']")))
    date_filter.click()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Moving function

# COMMAND ----------

# Move to destination function
def move_to(pattern, destination):
    download_folder = prefs["download.default_directory"]
    downloaded_files = glob.glob(os.path.join(download_folder, pattern))
    regex = "{}(.*).xlsx".format(download_folder)
    file_name = []
    destination_files = []
    for file in downloaded_files:
        match = re.search(regex, file).group(1)
        target_file = destination + match + '.xlsx'
        file_name.append(match)
        destination_files.append(target_file)
        dbutils.fs.mv("file:" + file, target_file)
        print("Complete move: "+ target_file)
    print("Moving to destination completed!")
    return destination_files, file_name

# COMMAND ----------

# MAGIC %md
# MAGIC ### """EXECUTION"""

# COMMAND ----------

# MAGIC %md
# MAGIC #### Vars input & login

# COMMAND ----------

# Create credential
credentials = {
    "email": "nguyen-ngoc.hanh@unilever.com",
    "pass": "Pizza4ps700@"
}

# Create folder for download
if not os.path.exists("/tmp/Shopee_BOT_SalesPerformance/"):
  print("Create new folder: Shopee_BOT_SalesPerformance")
  dbutils.fs.mkdirs("file:/tmp/Shopee_BOT_SalesPerformance/")
else:
  print("Download folder ready: Shopee_BOT_SalesPerformance")

# LOG IN
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--headless')
chrome_options.add_argument('--auto-open-devtools-for-tabs')
prefs = {
    "download.prompt_for_download": False,
    "download.default_directory": "/tmp/Shopee_BOT_SalesPerformance/",
    "download.directory_upgrade": True,
    "safebrowsing.enabled": False,
    "safebrowsing.disable_download_protection": True
}
chrome_options.add_experimental_option("prefs", prefs)
chrome_driver = "/tmp/chromedriver/chromedriver"

browser = webdriver.Chrome(service=Service(ChromeDriverManager(version = latest_chrome_version).install()), options=chrome_options)

browser.maximize_window()
timeout = 30
url = r'https://brandportal.shopee.com/seller/login'
browser.get(url)


login_ISR(browser, chrome_options, timeout, credentials)
time.sleep(5)


### Select Customize datepicker
# Date Filter Click
browser.find_element(By.XPATH,"//*[@class='ant-input'][@type='text']").click()
time.sleep(1)

# Customize Click
browser.find_element(By.XPATH,"//*[text()='Customize']").click()
browser.find_element(By.XPATH,"//*[text()='Customize']").click()
print("Customize button clicked!")
time.sleep(1)


# COMMAND ----------

# ### DOWNLOAD CUSTOMISED DATA
# refresh_page()
# # print("Time of download: " + str(download_num) +"\n")

# from_date = '2022-08-10'
# to_date = '2022-08-10'

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

# Define  previous mtd period
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

"""Download previous reports"""

### Download Previous Yesterday
if file_exists('dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/sales_performance/' + pattern_previous_yesterday) == False:
  download_num = 1
  while (not file_exists('file:/tmp/Shopee_BOT_SalesPerformance/' + pattern_previous_yesterday)) and download_num <5:
    try:
      download_files(from_date=previous_yesterday, to_date=previous_yesterday).execute()
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
pattern_last_7days= "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx".format(last_7day_name, yesterday_name)
pattern_last_30days = "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx".format(last_30day_name, yesterday_name)
pattern_month_to_date = "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx".format(mtd_begin_name, yesterday_name)


# COMMAND ----------

### Download MTD report
download_num = 1
while (not file_exists('file:/tmp/Shopee_BOT_SalesPerformance/' + pattern_month_to_date)) and download_num <5:
  try:
    refresh_page()
    if mtd_begin > yesterday:
      mtd_begin_fix = datetime.datetime.strftime((current_datetime - datetime.timedelta(1)).replace(day=1), '%Y-%m-%d')
      download_period_file(from_date=mtd_begin_fix, to_date=yesterday).execute()
    else:
      download_period_file(from_date=mtd_begin, to_date=yesterday).execute()

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

### Download Last 7 day report
download_num = 1
while (not file_exists('file:/tmp/Shopee_BOT_SalesPerformance/' + pattern_last_7days)) and download_num <5:
  try:
    refresh_page()
    print("Num of download: " + str(download_num) +"\n")
    download_last7days()
#     download_files(from_date=last_7day_begin, to_date=yesterday).execute()  ### Alternative
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

### Download Last 30 day report
download_num = 1
while (not file_exists('file:/tmp/Shopee_BOT_SalesPerformance/' + pattern_last_30days)) and download_num <5:
  try:
    refresh_page()
    download_last30days()
#     download_files(from_date=last_30day_begin, to_date=yesterday).execute() ###Alternative
    time.sleep(1)
    print("Download processing")
    download_num +=1
    print("\n Download failed. Try to download = " + str(download_num))
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      download_num +=1
      print("\n Download failed. Try to download = " + str(download_num))
    else:
      raise

# COMMAND ----------

# MAGIC %fs ls "file:/tmp/Shopee_BOT_SalesPerformance"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Moving & rename Daily reports to destination 

# COMMAND ----------

#### Create path to save file
destination_path = "dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/sales_performance/"

download_folder = prefs["download.default_directory"]
yesterday_path = glob.glob(os.path.join(download_folder, pattern_yesterday))[0]
last7days_path = glob.glob(os.path.join(download_folder, pattern_last_7days))[0]
last30days_path = glob.glob(os.path.join(download_folder, pattern_last_30days))[0]
month_to_date_path = glob.glob(os.path.join(download_folder, pattern_month_to_date))[0]


yesterday_destination = download_folder + "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-DailyOfftake_yesterday.xlsx"
last7days_destination = download_folder + "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-DailyOfftake_last7days.xlsx"
last30days_destination = download_folder + "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-DailyOfftake_last30days.xlsx"
month_to_date_destination = download_folder + "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-DailyOfftake_monthtodate.xlsx"

# COMMAND ----------

### Create vars for daily report
yesterday_name = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=1), '%Y.%m.%d')
last_7day_name = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=7), '%Y.%m.%d')
last_30day_name = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=30), '%Y.%m.%d')

pattern_yesterday = "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx".format(yesterday_name, yesterday_name)
pattern_last_7days= "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx".format(last_7day_name, yesterday_name)
pattern_last_30days = "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-{}_{}.xlsx".format(last_30day_name, yesterday_name)

destination_path = "dbfs:/mnt/adls/staging/shopee/BrandPortal_BOT/sales_performance/"

download_folder = prefs["download.default_directory"]
yesterday_path = glob.glob(os.path.join(download_folder, pattern_yesterday))[0]
last7days_path = glob.glob(os.path.join(download_folder, pattern_last_7days))[0]
last30days_path = glob.glob(os.path.join(download_folder, pattern_last_30days))[0]
month_to_date_path = glob.glob(os.path.join(download_folder, pattern_month_to_date))[0]


yesterday_destination = download_folder + "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-DailyOfftake_yesterday.xlsx"
last7days_destination = download_folder + "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-DailyOfftake_last7days.xlsx"
last30days_destination = download_folder + "Brand_Portal-Business_Insights---Sales_Performance---Shop_Dashboard-Gross_Data-DailyOfftake_last30days.xlsx"

# COMMAND ----------

# Copy yesterday file with new name and move original file to destination
dbutils.fs.cp("file:" + yesterday_path, "file:" + yesterday_destination)
print("Complete copy file to: "+ yesterday_destination)

# Move/Rename last 7 days, last 30 days & MTD file to destination
dbutils.fs.cp("file:" + last7days_path, "file:" + last7days_destination)
print("\n Complete rename last 7 days file to: "+ last7days_destination)

dbutils.fs.cp("file:" + last30days_path, "file:" + last30days_destination)
print("\n Complete rename last 30 days file to: " + last30days_destination)

if month_to_date_path == yesterday_path:
  dbutils.fs.cp("file:" + yesterday_destination, "file:" + month_to_date_destination)
elif month_to_date_path == last7days_path:
  dbutils.fs.cp("file:" + last7days_destination, "file:" + month_to_date_destination)
elif month_to_date_path == last30days_path:
  dbutils.fs.cp("file:" + last30days_destination, "file:" + month_to_date_destination)
else:
  dbutils.fs.cp("file:" + month_to_date_path, "file:" + month_to_date_destination)
print("\n Complete rename Month to date file to: " + month_to_date_destination)

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

# MOVE daily sales performances to folder
check_path = "mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_SalesPerformance/"
daily_offtake_path = "mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_DailyOfftake/sales_performance/"

if file_exists(check_path + "shopee_sales_performance_gross_yesterday.csv"):
  dbutils.fs.mv("/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_SalesPerformance/shopee_sales_performance_gross_yesterday.csv", daily_offtake_path + "shopee_sales_performance_gross_yesterday.csv")
  print("Move yesterday file to Daily Offtake folder")
else:
  print("No exist file")

if file_exists(check_path + "shopee_sales_performance_gross_last7days.csv"):
  dbutils.fs.mv("/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_SalesPerformance/shopee_sales_performance_gross_last7days.csv", daily_offtake_path + "shopee_sales_performance_gross_last7days.csv")
  print("Move last 7 days file to Daily Offtake folder")
else:
  print("No exist file")

if file_exists(check_path + "shopee_sales_performance_gross_last30days.csv"):
  dbutils.fs.mv("/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_SalesPerformance/shopee_sales_performance_gross_last30days.csv", daily_offtake_path + "shopee_sales_performance_gross_last30days.csv")
  print("Move last 30 days file to Daily Offtake folder")
else:
  print("No exist file")

if file_exists(check_path + "shopee_sales_performance_gross_monthtodate.csv"):
  dbutils.fs.mv("/mnt/adls/staging/ecom/BOT/Shopee/BrandPortal_SalesPerformance/shopee_sales_performance_gross_monthtodate.csv", daily_offtake_path + "shopee_sales_performance_gross_monthtodate.csv")
  print("Move MTD file to Daily Offtake folder")
else:
  print("No exist file")

# COMMAND ----------

# MOVE daily sales performances to folder
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

### Remove daily report
dbutils.fs.rm("file:" + last7days_path)
dbutils.fs.rm("file:" + last30days_path)
dbutils.fs.rm("file:" + month_to_date_path)


# COMMAND ----------


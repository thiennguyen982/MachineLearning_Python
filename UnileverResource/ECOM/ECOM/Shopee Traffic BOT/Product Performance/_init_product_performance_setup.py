# Databricks notebook source
!pip install selenium --upgrade webdriver_manager chromedriver_autoinstaller Selenium-Screenshot matplotlib pandas --upgrade

# COMMAND ----------

# MAGIC %sh cat /etc/*-release

# COMMAND ----------

# MAGIC %sh sudo apt update
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo add-apt-repository universe
# MAGIC sudo add-apt-repository multiverse
# MAGIC sudo apt update
# MAGIC

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
# MAGIC sudo apt install libxss1 libappindicator1 libindicator7 fonts-liberation libgbm1 libnspr4 libnss3 libwayland-server0 libdbusmenu-glib4 libdbusmenu-gtk4 libgtk2.0-0 libgtk2.0-common -y
# MAGIC
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

# ### Check pandas version
# import pandas 
# if pandas.__version__ >= '1.3':
#   print("pandas is updated")
# else:
#   dbutils.library.installPyPI("pandas", "1.3.5")
#   dbutils.library.restartPython()

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
from Screenshot import Screenshot
import matplotlib.image as mpimg

from selenium import webdriver

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
# MAGIC ### DEFINE FUNCTIONS

# COMMAND ----------

# MAGIC %md
# MAGIC #### Login func

# COMMAND ----------

# Login function
def login_ISR(driver, options, timeout, credentials):
    check = None
    while check is None:
        try:
            WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.ID, 'app')))
            driver.find_element(By.ID, "email").clear()
            driver.find_element(By.ID,"email").send_keys(credentials['email'])
            # driver.find_element_by_id('email').send_keys(credentials['email'])
            driver.find_element(By.XPATH,"//*[@id='password']").clear()
            driver.find_element(By.XPATH,"//*[@id='password']").send_keys(credentials['pass'])
            driver.find_element(By.XPATH,"//*[@id='app']/div/div/div/div/div[2]/div/div/div[3]/form/div[3]/div/div/button").click()

            WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH, "//*[@id='app']/div/div/div/div/div[2]/div/div/div[2]/button")))
            browser.find_element(By.XPATH,"//*[@id='app']/div/div/div/div/div[2]/div/div/div[2]/button").click()  # Click GO
            check = 'Sucessfully login!'
            print(check)
        except:
            print("Cannot access! Browser closing ...")
            browser.close()
            browser.quit()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Refresh pages

# COMMAND ----------

def refresh_page():
  browser.refresh()
  browser.maximize_window()
  time.sleep(3)
  WebDriverWait(browser, timeout).until(EC.presence_of_element_located((By.XPATH, "//div[@class='shopee-react-input__inner shopee-react-input__inner--normal']")))
  # Select Customize datepicker
  browser.find_element(By.XPATH,"//div[@class='shopee-react-input__inner shopee-react-input__inner--normal']").click()

  print("Input Selected!")
  time.sleep(3)
  
  try:
      browser.find_element(By.XPATH,"/html/body/div[2]/div/div/div/div[2]/div/div/ul/li[10]/span").click()
  except:
      browser.find_element(By.XPATH,"//span[normalize-space()='Customize']").click()
  time.sleep(3)
  print("Customize button clicked!")
  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Checking exist file func

# COMMAND ----------

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
# MAGIC #### Download single report func

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
            current_datetime = datetime.datetime.now() + datetime.timedelta(hours=7)
            current_date = datetime.datetime.strftime(current_datetime, '%Y-%m-%d')
            yesterday = datetime.datetime.strftime(current_datetime - datetime.timedelta(days=1), '%Y-%m-%d')
            current_year = datetime.datetime.today().year
            current_month = datetime.datetime.strftime(datetime.datetime.today(), '%b')
            current_month_num = int(datetime.datetime.strftime(datetime.datetime.today(), '%m'))
            today = int(datetime.datetime.strftime(datetime.datetime.today(), '%d'))
            # Create button and value
            try:
                current_month_button = browser.find_element(By.XPATH, "/html/body/div[3]/div/div/div/div[2]/div/div/div[1]/div/button[1]")
#                 current_month_button = browser.find_element(By.XPATH, "//*[text()='Aug']")
            except:
                current_month_button = browser.find_element(By.XPATH, "//*[@type='button'][@tabindex= '-1'][@class='ant-picker-month-btn']")
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
                            date_element.click()
                            date_element.click()
                            print("Clicked!")
                            time.sleep(3)
                            browser.find_element(By.XPATH,"//*[@type='button'][@class= 'ant-btn track-click-brand-portal_product_performance-apply ant-btn-primary']").click()
                            print("Apply!")
                            time.sleep(5)
                            
                            browser.find_element(By.XPATH,"//button[@type='button'][@class='ant-btn src-components_v1-DownloadCustom---download--8LVJm track-click-brand-portal_product_performance-download src-components-Filters---download--3PIKt ant-btn-icon-only']").click()
                            print("Download Click ...!")
                            time.sleep(5)
                            # Chooose customize date again for next round
                            date_filter = WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.XPATH, "//input [@class='ant-input']")))
                            date_filter.click()
                            customize_button = WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.XPATH, "//*[@class='src-components-DatePicker---option--fCbbJ']")))
                            browser.execute_script("arguments[0].click();", customize_button)
                            print('Customized Clicked for next round!')
                        else:
                            pass
                    else:
                        pass
            matching_year()
            matching_month()
            download()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Download period report func

# COMMAND ----------

### Download period report function
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
            ### Create vars
            current_datetime = datetime.datetime.now() + datetime.timedelta(hours=7)
            current_date = datetime.datetime.strftime(current_datetime, '%Y-%m-%d')
            yesterday = datetime.datetime.strftime(current_datetime - datetime.timedelta(days=1), '%Y-%m-%d')
            current_year = (datetime.datetime.now() + datetime.timedelta(hours=7)).year
            current_month = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7), '%b')
            current_month_num = int(datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7), '%m'))
            today = int(datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7), '%d'))
            ### Create button and value
            try:
                current_month_button = browser.find_element(By.XPATH, "/html/body/div[3]/div/div/div/div[2]/div/div/div[1]/div/button[1]")
#                 current_month_button = browser.find_element(By.XPATH, "//*[text()='Aug']")
            except:
                current_month_button = browser.find_element(By.XPATH, "//*[@type='button'][@tabindex= '-1'][@class='ant-picker-month-btn']")
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
                print("\n Starting download from:" + str(cls.from_date) + " to " + str(cls.to_date))
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
                ### Get all dates in current customised pannel
                all_dates = browser.find_elements(By.XPATH, "//table[@class='ant-picker-content']//td")
                for date_element in all_dates:
                    date = date_element.get_attribute("title")
                    is_in_view = date_element.get_attribute("class")
                    if (date == cls.from_date == cls.to_date and is_in_view.startswith('ant-picker-cell ant-picker-cell-in-view')):
                        browser.execute_script("arguments[0].click();", date_element)
                        print('Selecting start date: ' + date)
                        time.sleep(1)
                        browser.execute_script("arguments[0].click();", date_element)
                        print('Selecting end date: ' + date)
                        time.sleep(1)
                        
                        ### Click Refresh + Download
                        # create action chain object
                        action = ActionChains(browser)
                        try:
                            print("Stuck at method 1") 
                            refresh_button = WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.XPATH, "//*[text()='Refresh']")))
                            refresh_button.click()
                            
                        except Exception as e:
                          if 'TimeoutException' in str(e) or 'element click intercepted' in str(e):
                            print("Try method 2")
                            refresh_button = WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[@class='ant-btn track-click-brand-portal_product_performance-apply ant-btn-primary']")))
                            refresh_button.click()
                          else:
                            raise
                        print("Click Refresh!")
                        time.sleep(5)
                        
                        browser.find_element(By.XPATH,"//button[@class='ant-btn src-components_v1-DownloadCustom---download--8LVJm track-click-brand-portal_product_performance-download src-components-Filters---download--3PIKt ant-btn-icon-only']").click()
                        print("Clicked Download!")
                        time.sleep(10)

                        ### Wait until file is downloaded
                        begin_date = cls.from_date.replace('-', '.')
                        end_date = cls.to_date.replace('-', '.')
                        pattern_period = "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-{}_{}.xlsx".format(begin_date, end_date)
                        wait_sec = 0
                        dl_wait = True
                        while dl_wait and wait_sec < 150:
                            try:
                                dbutils.fs.ls('file:/tmp/Shopee_BOT_ProductPerformance/' + pattern_period)
                                print("DOWNLOADED " + "period from: " + cls.from_date + " to " + cls.to_date + " report! ...")
                                dl_wait = False
                            except Exception as e:
                                if 'java.io.FileNotFoundException' in str(e):
                                    time.sleep(1)
                                    wait_sec += 1
                                else:
                                    raise

                        ### Chooose customize date again for next round
                        date_filter = WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.XPATH, "//input [@class='ant-input']")))
                        date_filter.click()
                        customize_element = browser.find_element(By.XPATH,"/html/body/div[2]/div/div/div/div[2]/div/div/ul/li[10]/span")
                        browser.execute_script("arguments[0].click();", customize_element)
                        print('Customized Clicked for next round! \n')
                    elif (date == cls.from_date and date != cls.to_date and is_in_view.startswith('ant-picker-cell ant-picker-cell-in-view')):
                        print('Current month = '+ current_month_value)
                        browser.execute_script("arguments[0].click();", date_element)
                        print('Selecting start date: ' + date)
                        time.sleep(3)
                    elif (date == cls.to_date and date != cls.from_date and is_in_view.startswith('ant-picker-cell ant-picker-cell-in-view')):
                        print('Current month = '+ current_month_value)
                        browser.execute_script("arguments[0].click();", date_element)
                        print('Selecting end date: ' + date)
                        time.sleep(3)
                        # Click Refresh + Download
                        refresh_button = WebDriverWait(browser, 10).until(EC.element_to_be_clickable((By.XPATH, "//*[@type='button'][@class= 'ant-btn track-click-brand-portal_product_performance-apply ant-btn-primary']")))
                        refresh_button.click()
#                         browser.find_element(By.XPATH,"//*[@type='button'][@class= 'ant-btn track-click-brand-portal_product_performance-apply ant-btn-primary']").click()
                        print("Click Refresh!")
                        time.sleep(3)

                        browser.find_element(By.XPATH,"//button[@type='button'][@class='ant-btn src-components_v1-DownloadCustom---download--8LVJm track-click-brand-portal_product_performance-download src-components-Filters---download--3PIKt ant-btn-icon-only']").click()
                        print("Clicked Download!")
                        time.sleep(3)

                        ### Wait until file is downloaded
                        begin_date = cls.from_date.replace('-', '.')
                        end_date = cls.to_date.replace('-', '.')
                        pattern_period = "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-{}_{}.xlsx".format(begin_date, end_date)
                        wait_sec = 0
                        dl_wait = True
                        while dl_wait and wait_sec < 150:
                            try:
                                dbutils.fs.ls('file:/tmp/Shopee_BOT_ProductPerformance/' + pattern_period)
                                print("DOWNLOADED " + "period from: " + cls.from_date + " to " + cls.to_date + " report! ...")
                                dl_wait = False
                            except Exception as e:
                                if 'java.io.FileNotFoundException' in str(e):
                                    time.sleep(1)
                                    wait_sec += 1
                                else:
                                    raise
                        # Chooose customize date again for next round
                        time.sleep(1)
                        date_filter = browser.find_element(By.XPATH, "//div[@class='shopee-react-input__inner shopee-react-input__inner--normal']")
                        browser.execute_script("arguments[0].click();", date_filter)
                        
                        customize_element = browser.find_element(By.XPATH,"//span[normalize-space()='Customize']")
                        browser.execute_script("arguments[0].click();", customize_element)
                        print('Customized Clicked for next round! \n')
                    else:
                        pass
            matching_year()
            matching_month()
            download()
            time.sleep(1)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Download yesterday report 

# COMMAND ----------

def download_yesterday():
    # Choose Yesterday
    browser.find_element(By.XPATH,"/html/body/div[2]/div/div/div/div/div[1]/div[1]").click()
    print("Clicked 'Yesterday'!")
    try:
      browser.find_element(By.XPATH,"//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[5]/div/button[2]").click()
      print("Refresh data!") 
    except:
      browser.find_element(By.XPATH,"//button[@type='submit']").click()
      print("Apply clicked")
    time.sleep(5)
    
    browser.find_element(By.XPATH,"//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[5]/div/button[3]").click()
    print("Exporting data ...")
    time.sleep(10)
    # Wait until file is downloaded
    yesterday = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=1), '%Y.%m.%d')
    pattern_yesterday = "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-{}_{}.xlsx".format(yesterday, yesterday)
    wait_sec = 0
    dl_wait = True
    while dl_wait and wait_sec < 30:
      try:
          dbutils.fs.ls('file:/tmp/Shopee_BOT_ProductPerformance/' + pattern_yesterday)
          print("Downloaded yesterday product report \n")
          dl_wait = False
      except Exception as e:
          if 'java.io.FileNotFoundException' in str(e):
            time.sleep(1)
            wait_sec +=1
          else:
            print("DOWNLOAD FAILED! \n")
            raise

    # Chooose customize date again for next round
    browser.find_element(By.XPATH,"//input [@class='ant-input']").click()
    customize_element = browser.find_element(By.XPATH,"/html/body/div[2]/div/div/div/div[2]/div/div/ul/li[10]/span")
    browser.execute_script("arguments[0].click();", customize_element)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Download last 7 days report

# COMMAND ----------

def download_last7days():
    # Choose Last 7 days
    browser.find_element(By.XPATH,"/html/body/div[2]/div/div/div/div/div[1]/div[2]").click()
    print("Clicked 'Last 7 days'!")

    try:
      browser.find_element(By.XPATH,"//*[@id='root']/section/section/section/main/div/div[2]/div/div[3]/div/div/div[2]/button[2]").click()
      print("Refresh data!")
    except:
      browser.find_element(By.XPATH,"//button[@type='submit']").click()
      print("Apply clicked")
      
    time.sleep(5)
    browser.find_element(By.XPATH,"//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[5]/div/button[3]").click()
    print("Exporting data ...")
    time.sleep(10)
    
    # Wait until file is downloaded
    last_7day = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=7), '%Y.%m.%d')
    yesterday = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=1), '%Y.%m.%d')
    pattern_last7days = "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-{}_{}.xlsx".format(last_7day, yesterday)
    wait_sec = 0
    dl_wait = True
    while dl_wait and wait_sec < 45:
      try:
          dbutils.fs.ls('file:/tmp/Shopee_BOT_ProductPerformance/' + pattern_last7days)
          print("Downloaded last 7 days product report \n")
          dl_wait = False
      except Exception as e:
          if 'java.io.FileNotFoundException' in str(e):
            time.sleep(1)
            wait_sec +=1
          else:
            print("DOWNLOAD FAILED! \n")
            raise

    ### Chooose customize date again for next round
    browser.find_element(By.XPATH,"//input [@class='ant-input']").click()
    customize_element = browser.find_element(By.XPATH,"/html/body/div[2]/div/div/div/div[2]/div/div/ul/li[10]/span")
    browser.execute_script("arguments[0].click();", customize_element)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Download last 30 days report

# COMMAND ----------

def download_last30days():
    # Choose Last 30 Days
    browser.find_element(By.XPATH,"/html/body/div[2]/div/div/div/div/div[1]/div[3]").click()

    try:
      browser.find_element(By.XPATH,"//*[@id='root']/section/section/section/main/div/div[2]/div/div[3]/div/div/div[2]/button[2]").click()
      print("Refresh data!")
    except:
      browser.find_element(By.XPATH,"//button[@type='submit']").click()
      print("Apply clicked")
    time.sleep(5)
    
    browser.find_element(By.XPATH,"//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[2]/div[2]/div/form/div/div[5]/div/button[3]").click()
    print("Exporting data ...")
    time.sleep(10)
    # Wait until file is downloaded
    last_30day = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=30), '%Y.%m.%d')
    yesterday = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(hours=7) - datetime.timedelta(days=1), '%Y.%m.%d')
    pattern_last30days = "Brand_Portal-Business_Insights---Product_Analysis---Product_Performance-{}_{}.xlsx".format(last_30day, yesterday)
    wait_sec = 0
    dl_wait = True
    while dl_wait and wait_sec < 45:
      try:
          dbutils.fs.ls('file:/tmp/Shopee_BOT_ProductPerformance/' + pattern_last30days)
          print("Downloaded last 30 days product report \n")
          dl_wait = False
      except Exception as e:
          if 'java.io.FileNotFoundException' in str(e):
            time.sleep(1)
            wait_sec +=1
          else:
            print("DOWNLOAD FAILED! \n")
            raise

    ### Chooose customize date again for next round
    browser.find_element(By.XPATH,"//input [@class='ant-input']").click()
    customize_element = browser.find_element(By.XPATH,"/html/body/div[2]/div/div/div/div/div[1]/div[10]")
    browser.execute_script("arguments[0].click();", customize_element)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Moving file func

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
        print("Moving from: "+ file)
        print("Complete move to: "+ target_file + "\n")
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

# Create folder for downloadq
if not os.path.exists("file:/tmp/Shopee_BOT_ProductPerformance/"):
  print("Create new folder: Shopee_BOT_ProductPerformance")
  dbutils.fs.mkdirs("file:/tmp/Shopee_BOT_ProductPerformance/")
else:
  print("Download folder ready: Shopee_BOT_ProductPerformance")

# LOG IN
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--headless')
chrome_options.add_argument("window-size=945,788")
chrome_options.add_argument('--auto-open-devtools-for-tabs')

prefs = {
    "download.prompt_for_download": False,
    "download.default_directory": "/tmp/Shopee_BOT_ProductPerformance/",
    "download.directory_upgrade": True,
    "safebrowsing.enabled": False,
    "safebrowsing.disable_download_protection": True
}

chrome_options.add_experimental_option("prefs", prefs)
chrome_driver = "/tmp/chromedriver/chromedriver"

browser = webdriver.Chrome(service=Service(ChromeDriverManager(version = latest_chrome_version).install()), options=chrome_options)

browser.maximize_window()
timeout = 10
url = r'https://brandportal.shopee.com/seller/login'
browser.get(url)
time.sleep(5)


# COMMAND ----------

# WebDriverWait(browser, timeout).until(EC.presence_of_element_located((By.ID, 'app')))
# browser.find_element(By.ID,"email").clear()
# driver.find_element_by_id("email").send_keys(credentials['email'])
# driver.find_element(By.XPATH,"//*[@id='password']").clear()
# driver.find_element(By.XPATH,"//*[@id='password']").send_keys(credentials['pass'])
# driver.find_element(By.XPATH,"//*[@id='app']/div/div/div/div/div[2]/div/div/div[3]/form/div[3]/div/div/button").click()

# WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH, "//*[@id='app']/div/div/div/div/div[2]/div/div/div[2]/button")))
# browser.find_element(By.XPATH,"//*[@id='app']/div/div/div/div/div[2]/div/div/div[2]/button").click()  # Click GO
# check = 'Sucessfully login!'
# print(check)

# COMMAND ----------

### LOGIN
login_ISR(browser, chrome_options, timeout, credentials)

WebDriverWait(browser, timeout).until(EC.presence_of_element_located((By.ID, 'app')))
browser.maximize_window()
time.sleep(5)

### Click to Product Performance
browser.find_element(By.XPATH, "//*[@id='app']/div/div[2]/div[1]/ul/li[2]/ul/li/span").click()

# COMMAND ----------

# MAGIC %fs ls FileStore
# MAGIC

# COMMAND ----------

# ### Screenshot
# ob = Screenshot.Screenshot()
# img_url = ob.full_Screenshot(browser, save_path=r"/tmp/Shopee_BOT_ProductPerformance/",image_name="screenshot.jpg")

# dbutils.fs.cp("file:/tmp/Shopee_BOT_ProductPerformance/screenshot.jpg", "dbfs:/FileStore/screenshot.jpg")

# COMMAND ----------

# %md
# ![screenshot_img](files/screenshot.jpg)

# COMMAND ----------

# displayHTML("<img src ='files/screenshot.jpg'>")


# COMMAND ----------

### Select Customize datepicker
# Click Date range
try:
  browser.find_element(By.XPATH, "//*[@id='app']/div/div[2]/div[2]/div/div/div/div[2]/div/div[3]/div/div/div/div[1]/div[4]/span/input").click()
except:
  try:
    browser.find_element(By.XPATH, "//span[@class='src-components-DatePicker---input--2EE-J ant-dropdown-trigger ant-input-affix-wrapper']").click()
  except:
    try:
      browser.find_element(By.XPATH, "//span[@class='shopee-react-icon sp-icon sp-icon-calendar']//*[name()='svg']").click()
    except:
      try:
        browser.find_element(By.XPATH, "//span[@class='src-components-DatePicker---inputPrefix--2Au3e']").click()
      except:
        print("Cannot locate element")

print("Datetime bar selected! ")
time.sleep(5)

# Click Customize
browser.find_element(By.XPATH, "//span[normalize-space()='Customize']").click()
print("Customize selected! ")


# COMMAND ----------

# browser.close()
# browser.quit()

# COMMAND ----------

# Create vars
current_datetime = datetime.datetime.now() + datetime.timedelta(hours=7)
current_date = datetime.datetime.strftime(current_datetime, '%Y-%m-%d')
yesterday = datetime.datetime.strftime(current_datetime - datetime.timedelta(days=1), '%Y-%m-%d')
previous_yesterday = datetime.datetime.strftime(current_datetime - datetime.timedelta(days=2), '%Y-%m-%d')

previous_last7days_begin = datetime.datetime.strftime(current_datetime - datetime.timedelta(days=14), '%Y-%m-%d')
previous_last7days_end = datetime.datetime.strftime(current_datetime - datetime.timedelta(days=8), '%Y-%m-%d')

previous_last30days_begin = datetime.datetime.strftime(current_datetime - datetime.timedelta(days=60), '%Y-%m-%d')
previous_last30days_end = datetime.datetime.strftime(current_datetime - datetime.timedelta(days=31), '%Y-%m-%d')

mtd_begin = datetime.datetime.strftime((current_datetime - datetime.timedelta(days=1)).replace(day=1), '%Y-%m-%d')


# COMMAND ----------


# Databricks notebook source
pip install Selenium-Screenshot

# COMMAND ----------



# COMMAND ----------

pip install selenium==4.2.0

# COMMAND ----------

# MAGIC %run ./g-auth_setup_utility

# COMMAND ----------

# MAGIC %run ./Chrome_setup_utility

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

import selenium
selenium.__version__#'4.2.0'

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
            driver.find_element_by_xpath("/html/body/div/main/div/div[1]/div/div/div/div/div/div/div[2]/div[1]/div/div/div/input").clear()
            driver.find_element_by_xpath("/html/body/div/main/div/div[1]/div/div/div/div/div/div/div[2]/div[1]/div/div/div/input").send_keys(credentials['email'])
            time.sleep(2)
            
            #filling password
            driver.find_element_by_xpath("/html/body/div/main/div/div[1]/div/div/div/div/div/div/div[2]/div[2]/div/div/input").clear()
            driver.find_element_by_xpath("/html/body/div/main/div/div[1]/div/div/div/div/div/div/div[2]/div[2]/div/div/input").send_keys(credentials['pass'])
            time.sleep(2)
            
            #hit login button
            driver.find_element_by_xpath("/html/body/div/main/div/div[1]/div/div/div/div/div/div/button[2]").click()
            check = 'Sucessfully login!'
        except:
            print("Cannot access! Browser closing ...")
            browser.close()
            browser.quit()

# COMMAND ----------

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']


def Get_Service():
    try:
        creds = None
        
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file('C:\\Users\\naveen.mulimani\\Gmail_OTP\\credentials.json', SCOPES)
                print('credentials Ok')
                creds = flow.run_local_server(port=0)
                
            # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
                
        service = build('gmail', 'v1', credentials=creds)
        
        return service
    except:
        print("Error in searching msg")
        
        

def search_msg(service, user_id, search_string):
    
    try:
        search_id = service.users().messages().list(userId=user_id, q=search_string).execute()
        
        number_results = search_id['resultSizeEstimate']
        
        final_list = []
        if number_results >0:
            message_ids = search_id['messages']
            
            for ids in message_ids:
                final_list.append(ids['id'])
                
            return final_list
        else:
            print('No Results are found in email')
            return ""
        
    except (errors.HttpError.error):
        print("Error in searching msg")         

def get_msg(service, user_id, msg_id):
    try:
        message = service.users().messages().get(userId=user_id, id=msg_id, format='raw').execute()
        
        msg_raw = base64.urlsafe_b64decode(message['raw'].encode('ASCII'))
        
        msg_str = email.message_from_bytes(msg_raw)
        
        content_types = msg_str.get_content_maintype()
        
        soup = BeautifulSoup(msg_str.get_payload(), features="html.parser")
        
        for script in soup(["script", "style"]):
            script.extract()
        text = soup.get_text()
        
        #remove \n \r and \t
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = '\n'.join(chunk for chunk in chunks if chunk)
        
        index = text.find("on the Email OTP verification page:")
        OTP = text[index+40:index+46]
        return OTP
    
    except :
        print("Error in get msg")
            

            
def get_OTP():
    
    user_id = 'me'
    search_string = ' Email OTP verification'
    
    #Get service
    service = Get_Service()   
    
    time.sleep(20)
    #search msg in Gmail search box and get those emails
    msg_id = search_msg(service, user_id, search_string)
    
    #consider latest email
    msg_id1 = msg_id[0]
    
    #extract body of email and search and get the OTP
    OTP = str(get_msg(service, user_id, msg_id1))
    
    #print("in fun Email OTP verification code:", OTP)
    return OTP

# COMMAND ----------

print(latest_chrome_version)

# COMMAND ----------

# Login function
def login_ISR(driver, options, timeout, credentials):
    check = None
    while check is None:
        try:
            #WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.ID, 'app')))
            time.sleep(2)
            #driver.find_element_by_id("username").clear()
            driver.find_element(By.CLASS_NAME, "username").clear()
            driver.find_element(By.CLASS_NAME, "username").send_keys(credentials['email'])
            time.sleep(2)
            #driver.find_element_by_xpath("//*[@id='password form-item']").clear()
            #driver.find_element_by_xpath("//*[@id='password form-item']").send_keys(credentials['pass'])
            driver.find_element(By.CLASS_NAME, "password form-item").clear()
            driver.find_element(By.CLASS_NAME, "password form-item").send_keys(credentials['pass'])
            time.sleep(2)
            #driver.find_element_by_xpath(login_button).click()
            
#             WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH, "//*[@id='app']/div/div/div/div/div[2]/div/div/div[2]/button")))
#             browser.find_element_by_xpath(go_button).click() ### Click GO
            check = 'Sucessfully login!'
            print(check)
        except:
            print("Cannot access! Browser closing ...")
            browser.close()
            browser.quit()

# COMMAND ----------

#  #Create credential
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


# chrome_options.add_argument("start-maximized");
# chrome_options.add_argument("disable-infobars")
# chrome_options.add_argument("--disable-extensions")

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
# time.sleep(10)
# browser.find_element(By.CLASS_NAME, "lang-content-wrapper").click()
                       

# time.sleep(3)
# a = browser.find_element(by=By.XPATH, value='/html/body/div[2]/ul/li[1]')
# actions.move_to_element(a).click().perform()

# print('changed language')
# # html = browser.page_source
# # time.sleep(5)
# # #driver = browser
# # ob = Screenshot.Screenshot()
# # img_url = ob.full_Screenshot(browser, save_path=r"/tmp/",image_name="your_screenshot3.jpg")

# #self.browser.find_element_by_xpath("'/html/body/div[2]/ul/li[1]'").get_attribute('outerHTML')

# # browser.find_element(By.CLASS_NAME, "shopee-input__input").clear()
# # browser.find_element(By.CLASS_NAME, "shopee-input__input").clear().send_keys(credentials['email'])

# # browser.find_element(By.CLASS_NAME, "username").clear()
# # browser.find_element(By.CLASS_NAME, "username").clear().send_keys(credentials['email'])


# # browser.find_element(By.CLASS_NAME, "shopee-input").clear()
# # browser.find_element(By.CLASS_NAME, "shopee-input").clear().send_keys(credentials['email'])

# # browser.find_element(By.CLASS_NAME, "shopee-input__inner shopee-input__inner--large").clear()
# # browser.find_element(By.CLASS_NAME, "shopee-input__inner shopee-input__inner--large").clear().send_keys(credentials['email'])
                                                                                                                    
# #browser.find_element(By.CLASS_NAME, "shopee-input__input").clear()
# #browser.find_element(By.CLASS_NAME, "shopee-input__input").send_keys(credentials['email'])



# #browser.find_element(By.CLASS_NAME, "//*[contains(text(),"Forgot")"])

# #browser.find_element(By.XPATH, "//input[@placeholder=’Email / Phone /Username’ and @type=’text’"])

# #driver = browser
# #driver.find_element_by_xpath("/html/body/div/main/div/div[1]/div/div/div/div/div/div/div[2]/div[1]/div/div/div/input").clear()
# #driver.find_element_by_xpath("/html/body/div/main/div/div[1]/div/div/div/div/div/div/div[2]/div[1]/div/div/div/input").send_keys(credentials['email'])

# # time.sleep(2)

# # from selenium.webdriver.support.ui import WebDriverWait 
# # WebDriverWait(driver, 120).until(lambda x: x.find_element(by=By.XPATH,value='/html/body/div/main/div/div[1]/div/div/div/div/div/div/div[2]/div[2]/div/div/input'))

# # #filling password
# #driver.find_element_by_xpath("/html/body/div/main/div/div[1]/div/div/div/div/div/div/div[2]/div[2]/div/div/input").clear()
#  browser.find_element_by_xpath("/html/body/div/main/div/div[1]/div/div/div/div/div/div/div[2]/div[2]/div/div/input").send_keys(credentials['pass'])
# # time.sleep(2)

# COMMAND ----------

browser.close()

# COMMAND ----------

# f = open("/dbfs/mnt/adls/staging/index.html", "w")
# f.write(html)
# f.close()

# COMMAND ----------

# MAGIC %fs ls /mnt/adls/staging/

# COMMAND ----------

dbutils.fs.cp("file:/tmp/your_screenshot3.jpg", "dbfs:/FileStore/screenshot3.jpg")

# COMMAND ----------

# MAGIC %fs ls file:/tmp/

# COMMAND ----------

#browser.close()

# COMMAND ----------

#  #Create credential
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


# chrome_options.add_argument("start-maximized");
# chrome_options.add_argument("disable-infobars")
# chrome_options.add_argument("--disable-extensions")

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
# #browser.find_element_by_xpath("/html/body/div/main/div/div[2]/div/div[1]/span").click()
# browser.find_element(By.CLASS_NAME, "lang-content-wrapper").click()
# #browser.find_element_by_xpath("/html/body/div/main/div/div[2]/div/div[1]/span").click()                               

# time.sleep(3)
# a = browser.find_element(by=By.XPATH, value='/html/body/div[2]/ul/li[1]')
# actions.move_to_element(a).click().perform()

# print('changed language')

# time.sleep(5)
# #login_ISR(browser, chrome_options, timeout, credentials)

# driver = browser
# # time.sleep(2)
# # html = driver.page_source
# # #print(driver)
# # soup_obj = BeautifulSoup(html, 'html.parser')
# # #print(soup_obj)
# # #driver.find_element_by_id("username").clear()
# # WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH, '//input[@class="shopee-input__input"][@placeholder="Email / Phone / Username"]')))
# # time.sleep(5)
# # driver.find_element(By.XPATH, '//input[@class="shopee-input__input"][@placeholder="Email / Phone / Username"]').clear()
# # #driver.find_element(By.XPATH, '//input[@class="shopee-input__input"]').clear()
# # time.sleep(5)
# # driver.find_element(By.XPATH, '//input[@class="shopee-input__input"][@placeholder="Email / Phone / Username"]').send_keys(credentials['email'])
# # time.sleep(2)
# # #driver.find_element_by_xpath("//*[@id='password form-item']").clear()
# # #driver.find_element_by_xpath("//*[@id='password form-item']").send_keys(credentials['pass'])
# # driver.find_element(By.CLASS_NAME, "password form-item").clear()
# # driver.find_element(By.CLASS_NAME, "password form-item").send_keys(credentials['pass'])
# # time.sleep(2)

# #----------------------------------------------------

# #driver = browser
# #driver.find_element_by_xpath("/html/body/div[1]/main/div/div[1]/div/div/div/div/div/div/div[2]/div[1]/div/div/div/input").clear()
# #driver.find_element_by_xpath("/html/body/div[1]/main/div/div[1]/div/div/div/div/div/div/div[2]/div[1]/div/div/div/input").clear()
# #driver.find_element(By.CLASS_NAME, "shopee-input__inner shopee-input__inner--large").clear()
# #driver.find_element(By.CLASS_NAME, "shopee-input").clear()

# driver.find_element(By.CLASS_NAME, "filter-input form-item").clear()
                             
# #driver.find_element_by_xpath("/html/body/div/main/div/div[1]/div/div/div/div/div/div/div[2]/div[1]/div/div/div/input").send_keys(credentials['email'])
# time.sleep(2)

# # #filling password
# # driver.find_element_by_xpath("/html/body/div/main/div/div[1]/div/div/div/div/div/div/div[2]/div[3]/div/div/input").clear()
# # driver.find_element_by_xpath("/html/body/div/main/div/div[1]/div/div/div/div/div/div/div[2]/div[3]/div/div/input").send_keys(credentials['pass'])
# #                               #/html/body/div/main/div/div[1]/div/div/div/div/div/div/div[2]/div[3]/div/div/input
# # time.sleep(2)

# # #hit login button
# # driver.find_element_by_xpath("/html/body/div/main/div/div[1]/div/div/div/div/div/div/button[2]").click()
# # check = 'Sucessfully login!'

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

browser = webdriver.Chrome(executable_path=ChromeDriverManager(version = latest_chrome_version).install(), options=chrome_options)
actions = ActionChains(browser)

timeout = 10
url = r'https://banhang.shopee.vn/account/signin?next=%2F'
browser.get(url)

time.sleep(2)
browser.find_element_by_xpath("//*[@id='app']/div[2]/div/div/div/div[3]/div/div/div/div[2]/button").click()

#change the language 
time.sleep(5)
#browser.find_element_by_xpath("/html/body/div[1]/main/div/div[2]/div/div/span").click()
browser.find_element_by_xpath("/html/body/div/main/div/div[2]/div/div[1]/span").click()
                               

time.sleep(1)
a = browser.find_element(by=By.XPATH, value='/html/body/div[2]/ul/li[1]')
actions.move_to_element(a).click().perform()

print('changed language')
time.sleep(5)
login_ISR(browser, chrome_options, timeout, credentials)

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

import datetime



print(month_name)

# COMMAND ----------



# COMMAND ----------


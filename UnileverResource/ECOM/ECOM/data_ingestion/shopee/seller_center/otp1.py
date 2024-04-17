# Databricks notebook source
# MAGIC %run ./library_utility

# COMMAND ----------

import os
import google_auth_oauthlib.flow
import googleapiclient.errors
from googleapiclient import discovery

# Disable OAuthlib's HTTPS verification when running locally.
# *DO NOT* leave this option enabled in production.
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

# From Google Console 
client_secrets_file = '/dbfs/mnt/adls/user/naveen/OTP/credentials_new2.json' # YOUR FILE NAME
scopes = ['https://www.googleapis.com/auth/yt-analytics.readonly']
api_service_name = "youtubeAnalytics"
api_version = "v2"

flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(client_secrets_file, scopes)
    
#print(flow.run_local_server(port=0))
#credentials = flow.run_console()
flow.run_console()
    
#print(credentials.to_json())

# COMMAND ----------

print(client_secrets_file)

# COMMAND ----------

import os
import google_auth_oauthlib.flow
import googleapiclient.errors
from googleapiclient import discovery
from google.oauth2.credentials import Credentials

# Disable OAuthlib's HTTPS verification when running locally.
# *DO NOT* leave this option enabled in production.
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

# From Google Console 
client_secrets_file = "/dbfs/mnt/adls/user/naveen/OTP/credentials1.json" # YOUR FILE NAME
scopes = ['https://www.googleapis.com/auth/yt-analytics.readonly']
api_service_name = "youtubeAnalytics"
api_version = "v2"

creds = Credentials.from_authorized_user_file(
        "/dbfs/mnt/adls/user/naveen/OTP/token.json", scopes)
if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            client_secrets_file, scopes) #enter your scopes and secrets file
        creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open('/dbfs/mnt/adls/user/naveen/OTP/token.json', 'w') as token:
        token.write(creds.to_json())
    
# youtube_analytics = googleapiclient.discovery.build(
#         api_service_name, api_version, credentials=creds)

# COMMAND ----------

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

# COMMAND ----------

# gmailId = 'uvn.analytics@gmail.com'
# passWord = 'analytics.unilever@113'

# driver = browser#webdriver.Chrome(ChromeDriverManager().install())
# driver.get(r'https://accounts.google.com/signin/v2/identifier?continue='+\
# 'https%3A%2F%2Fmail.google.com%2Fmail%2F&service=mail&sacu=1&rip=1'+\
# '&flowName=GlifWebSignIn&flowEntry = ServiceLogin')
# driver.implicitly_wait(15)

# loginBox = driver.find_element_by_xpath('//*[@id ="identifierId"]')
# loginBox.send_keys(gmailId)

# nextButton = driver.find_elements_by_xpath('//*[@id ="identifierNext"]')
# nextButton[0].click()

# passWordBox = driver.find_element_by_xpath('//*[@id="password"]/div[1]/div/div[1]/input')
# passWordBox.send_keys(passWord)

# nextButton = driver.find_elements_by_xpath('//*[@id ="passwordNext"]')
# nextButton[0].click()

# print('Login Successful...!!')

# COMMAND ----------

# from selenium import webdriver
# from webdriver_manager.chrome import ChromeDriverManager

#print('Enter the gmailid and password')
#gmailId, passWord = map(str, input().split())

gmailId = 'uvn.analytics@gmail.com'
passWord = 'analytics.unilever@113'
try:
	driver = browser#webdriver.Chrome(ChromeDriverManager().install())
	driver.get(r'https://accounts.google.com/signin/v2/identifier?continue='+\
	'https%3A%2F%2Fmail.google.com%2Fmail%2F&service=mail&sacu=1&rip=1'+\
	'&flowName=GlifWebSignIn&flowEntry = ServiceLogin')
	driver.implicitly_wait(15)

	loginBox = driver.find_element_by_xpath('//*[@id ="identifierId"]')
	loginBox.send_keys(gmailId)

	nextButton = driver.find_elements_by_xpath('//*[@id ="identifierNext"]')
	nextButton[0].click()

	passWordBox = driver.find_element_by_xpath(
		'//*[@id ="password"]/div[1]/div / div[1]/input')
	passWordBox.send_keys(passWord)

	nextButton = driver.find_elements_by_xpath('//*[@id ="passwordNext"]')
	nextButton[0].click()

	print('Login Successful...!!')
except:
	print('Login Failed')


# COMMAND ----------


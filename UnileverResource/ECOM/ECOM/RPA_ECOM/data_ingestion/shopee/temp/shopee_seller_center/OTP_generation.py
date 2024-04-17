# Databricks notebook source
# MAGIC %run ./library_utility

# COMMAND ----------

# import json
# import logging
# import os
# import pandas as pd
# import re
# import datetime
# from datetime import timedelta
# import calendar
# import time
# import shutil
# import chardet
# from collections import OrderedDict
# #from Screenshot import Screenshot
# #import globcd

# from selenium import webdriver

# from selenium.webdriver.common.by import By
# from selenium.webdriver.common.action_chains import ActionChains
# from selenium.webdriver.support import expected_conditions
# from selenium.webdriver.support.wait import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.common.by import By
# from selenium.webdriver.common.keys import Keys
# from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

# from webdriver_manager.chrome import ChromeDriverManager

# logging.basicConfig(level=logging.INFO,
#                     filename="error.log",
#                     format='%(asctime)s %(message)s')

# from __future__ import print_function

# import os.path
# from urllib.request import urlopen
# from bs4 import BeautifulSoup

# from google.auth.transport.requests import Request
# from google.oauth2.credentials import Credentials
# from google_auth_oauthlib.flow import InstalledAppFlow
# from googleapiclient.discovery import build
# from googleapiclient.errors import HttpError
# import email
# import base64

# COMMAND ----------

# # If modifying these scopes, delete the file token.json.
# SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']


# def Get_Service():
#     try:
#         creds = None
        
#         if os.path.exists('token.json'):
#             creds = Credentials.from_authorized_user_file('token.json', SCOPES)
#         # If there are no (valid) credentials available, let the user log in.
#         if not creds or not creds.valid:
#             if creds and creds.expired and creds.refresh_token:
#                 creds.refresh(Request())
#             else:
#                 flow = InstalledAppFlow.from_client_secrets_file('file:/tmp/Shopee/credentials.json', SCOPES)
#                 print('credentials Ok')
#                 creds = flow.run_local_server(port=0)
                
#             # Save the credentials for the next run
#             with open('token.json', 'w') as token:
#                 token.write(creds.to_json())
                
#         service = build('gmail', 'v1', credentials=creds)
        
#         return service
#     except:
#         print("Error in searching msg")
        
        

# def search_msg(service, user_id, search_string):
    
#     try:
#         search_id = service.users().messages().list(userId=user_id, q=search_string).execute()
        
#         number_results = search_id['resultSizeEstimate']
        
#         final_list = []
#         if number_results >0:
#             message_ids = search_id['messages']
            
#             for ids in message_ids:
#                 final_list.append(ids['id'])
                
#             return final_list
#         else:
#             print('No Results are found in email')
#             return ""
        
#     except:
#         print("Error in searching msg")         

# def get_msg(service, user_id, msg_id):
#     try:
#         message = service.users().messages().get(userId=user_id, id=msg_id, format='raw').execute()
        
#         msg_raw = base64.urlsafe_b64decode(message['raw'].encode('ASCII'))
        
#         msg_str = email.message_from_bytes(msg_raw)
        
#         content_types = msg_str.get_content_maintype()
        
#         soup = BeautifulSoup(msg_str.get_payload(), features="html.parser")
        
#         for script in soup(["script", "style"]):
#             script.extract()
#         text = soup.get_text()
        
#         #remove \n \r and \t
#         lines = (line.strip() for line in text.splitlines())
#         chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
#         text = '\n'.join(chunk for chunk in chunks if chunk)
        
#         index = text.find("on the Email OTP verification page:")
#         OTP = text[index+40:index+46]
#         return OTP
    
#     except :
#         print("Error in get msg")
            

            
# def get_OTP():
    
#     user_id = 'me'
#     search_string = ' Email OTP verification'
    
#     #Get service
#     service = Get_Service()   
    
#     time.sleep(20)
#     #search msg in Gmail search box and get those emails
#     msg_id = search_msg(service, user_id, search_string)
    
#     #consider latest email
#     msg_id1 = msg_id[0]
    
#     #extract body of email and search and get the OTP
#     OTP = str(get_msg(service, user_id, msg_id1))
    
#     #print("in fun Email OTP verification code:", OTP)
#     return OTP

# COMMAND ----------

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
def Get_Service():
    
        creds = None
        
        if os.path.exists('/dbfs/mnt/adls/user/naveen/OTP/token.json'):
            creds = Credentials.from_authorized_user_file('/dbfs/mnt/adls/user/naveen/OTP/token.json', SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file('/dbfs/mnt/adls/user/naveen/OTP/credentials_new9.json', SCOPES)
                print('credentials Ok')
                creds = flow.run_local_server(port=0)
                
            # Save the credentials for the next run
            with open('/dbfs/mnt/adls/user/naveen/OTP/token.json', 'w') as token:
                token.write(creds.to_json())
                
        service = build('gmail', 'v1', credentials=creds)
        
        return service
service = Get_Service()

# COMMAND ----------

import time
#time.sleep(5)
import requests
schema = { "otp": "not fetched" }
otp= requests.post("https://prod-108.westeurope.logic.azure.com:443/workflows/3c940823adef4679b882b6c732458b49/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=EzIKXAZZgdVuxmrlzdPzx-uHiMJqq7QiMwV7uUVAni8",json=schema)  
print(str(otp.text)[8:14])
print(str(otp.text))

# COMMAND ----------

print(str(otp.text))

# COMMAND ----------

otp_str = str(otp.text)[8:14]
print((otp_str))

# COMMAND ----------



# COMMAND ----------

'https://prod-108.westeurope.logic.azure.com:443/workflows/3c940823adef4679b882b6c732458b49/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=EzIKXAZZgdVuxmrlzdPzx-uHiMJqq7QiMwV7uUVAni8'

# COMMAND ----------

flow = InstalledAppFlow.from_client_secrets_file(
            client_secrets_file, scopes) #enter your scopes and secrets file
        creds = flow.run_local_server(port=0)

# COMMAND ----------

# MAGIC %sh
# MAGIC curl -X GET -H 'Authorization: Bearer <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>' https://adb-7141874556691420.0.azuredatabricks.net/api/2.0/sql/warehouses/

# COMMAND ----------

dbutils.notebook.entry_point.getDbutils().notebook() \
  .getContext().tags().get("browserHostName").get()

# COMMAND ----------


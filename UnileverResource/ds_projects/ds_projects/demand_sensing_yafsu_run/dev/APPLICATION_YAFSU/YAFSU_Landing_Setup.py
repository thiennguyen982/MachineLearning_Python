# Databricks notebook source
import requests
import os 
import pandas as pd 

# COMMAND ----------

dbutils.widgets.text("TASK_ID", "e6mvXJrBwUeSWii6CsMPpZYAJ-oz")
dbutils.widgets.text("EMAIL", "Lai-Trung-Minh.Duc@unilever.com")
dbutils.widgets.text("HORIZON", "13")
dbutils.widgets.text("DATATYPE", "EXCEL_XLSX")

TASK_ID = dbutils.widgets.get("TASK_ID")
EMAIL = dbutils.widgets.get("EMAIL")
HORIZON = int(dbutils.widgets.get("HORIZON"))
DATATYPE = dbutils.widgets.get("DATATYPE")

# COMMAND ----------

print(TASK_ID, EMAIL, HORIZON)

# COMMAND ----------

LINK_NOTIFICATION_EMAIL = "https://prod-30.westeurope.logic.azure.com:443/workflows/09183d068ba44b51ab2db3cef08b3c62/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=FPN2CxxRpfd5eFsZ1NMpjM5BxiQJWhtHiftQjhThbms"

LINK_TRIGGER_ADLS_SHAREPOINT = "https://prod-29.westeurope.logic.azure.com:443/workflows/2bfcbe1cc12540c1920bebbb3ad0be44/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=rbQvEUdOuYrHRX9pLeRZGrowp97gacr9S8NSKuNG4Fo"

def utils_send_email(TASK_ID, EMAIL, SUBJECT, CONTENT, HTML_CONTENT, ATTACHMENTS):    
    json_obj = {
        "TASK_ID": TASK_ID, 
        "EMAIL": EMAIL,
        "SUBJECT": SUBJECT,
        "CONTENT": CONTENT, 
        "HTML_CONTENT": HTML_CONTENT,
        "ATTACHMENTS": ATTACHMENTS
    }
    req = requests.post(LINK_NOTIFICATION_EMAIL, json=json_obj)
    return req.status_code

def utils_trigger_adls_copy_sharepoint(TASK_ID, OUTPUT_ARR, DEBUG_ARR):
    json_obj = {
        "TASK_ID": TASK_ID, 
        "OUTPUT_ARR": OUTPUT_ARR,
        "DEBUG_ARR": DEBUG_ARR
    }
    req = requests.post(LINK_TRIGGER_ADLS_SHAREPOINT, json=json_obj)
    return req.status_code

# COMMAND ----------

# TASK_ID = "DEMO"
ROOT_PATH = f"/dbfs/mnt/adls/YAFSU/{TASK_ID}"
INPUT_PATH = f"{ROOT_PATH}/INPUT/"
########################################
EDA_REPORT_PATH = f"{ROOT_PATH}/EDA_HTML/"
ERROR_LOG_PATH = f"{ROOT_PATH}/ERROR_LOG/"
DEBUG_PATH = f"{ROOT_PATH}/DEBUG/"
TEMP_PATH = f"{ROOT_PATH}/TEMP/"
FORECAST_TEMP = f"{ROOT_PATH}/FORECAST_TEMP/"
OUTPUT_PATH = f"{ROOT_PATH}/OUTPUT/"
CONFIG_PATH = f"{ROOT_PATH}/CONFIG/"

for path in [EDA_REPORT_PATH, ERROR_LOG_PATH, DEBUG_PATH, TEMP_PATH, OUTPUT_PATH, CONFIG_PATH, FORECAST_TEMP]:
    os.makedirs(path, exist_ok=True)

# COMMAND ----------

#### Read Config File ####
df_config_temp = pd.read_excel(CONFIG_PATH + "YAFSU_CONFIG_FILE.xlsx", sheet_name="CONFIG", header=None).T
df_config_temp.columns = df_config_temp.iloc[0]
df_config_temp = df_config_temp[1:]

KEY_TS = df_config_temp['KEY_TS'].dropna().unique()
KEY_CUSTOMER = df_config_temp['KEY_CUSTOMER'].dropna().unique()
KEY_PRODUCT = df_config_temp['KEY_PRODUCT'].dropna().unique()
KEY_OUTPUT_GROUP = df_config_temp['KEY_OUTPUT_GROUP'].dropna().unique()
TARGET_VARS_ARR = df_config_temp['TARGET_VARS_ARR'].dropna().unique()
EXTERNAL_NUMERICAL_VARS_ARR = df_config_temp['EXTERNAL_NUMERICAL_VARS_ARR'].dropna().unique()
EXTERNAL_CATEGORICAL_VARS_ARR = df_config_temp['EXTERNAL_CATEGORICAL_VARS_ARR'].dropna().unique()

# COMMAND ----------

for col in df_config_temp.columns:
    print (col)
    print (df_config_temp[col].dropna().unique())
    print ('------------------------------------')
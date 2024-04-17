# Databricks notebook source
# MAGIC %run "../EnvironmentSetup"

# COMMAND ----------

# MAGIC %run "./YAFSU_Landing_Setup"

# COMMAND ----------

# MAGIC %run "./YAFSU_Utils"

# COMMAND ----------

import pickle as pkl 


# COMMAND ----------

def post_processing(filename):
  print (filename)
  output_data = pkl.load(open(FORECAST_TEMP + '/' + filename, 'rb'))
  result_dict = {}
  for item_group in output_data:
    for item in item_group:
      if item['MODEL_NAME'] not in result_dict.keys():
        result_dict[item['MODEL_NAME']] = []
      if item['ERROR'] == 'NO ERROR':
        result_dict[item['MODEL_NAME']].append(item['OUTPUT'])
  for key in result_dict.keys():
    if len(result_dict[key]) > 0:
      union_df = pd.concat(result_dict[key])
      union_df.to_parquet(OUTPUT_PATH + f'RESULT_{key}_' + filename + ".parquet", index=False)
      write_excel_file(union_df, f'RESULT_{key}_' + filename, sheet_name='DATA', dbfs_directory=OUTPUT_PATH)

# COMMAND ----------

for filename in os.listdir(FORECAST_TEMP):
  post_processing(filename)

# COMMAND ----------

import json
import requests

def upload_to_sharepoint():
  output_json_arr = []
  for filename in os.listdir(OUTPUT_PATH):
    file_path = f'unilever/YAFSU/{TASK_ID}/OUTPUT/'
    output_json_arr.append({
      'FILEPATH': file_path + filename,
      'FILENAME': filename
    })
  requests.post(
    'https://prod-29.westeurope.logic.azure.com:443/workflows/2bfcbe1cc12540c1920bebbb3ad0be44/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=rbQvEUdOuYrHRX9pLeRZGrowp97gacr9S8NSKuNG4Fo',
    json = {
      'TASK_ID': TASK_ID,
      'OUTPUT_ARR': output_json_arr,
      'DEBUG_ARR': output_json_arr
    }
  )

upload_to_sharepoint()

# COMMAND ----------

# upload_to_sharepoint()
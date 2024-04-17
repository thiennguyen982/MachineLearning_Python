# Databricks notebook source
# MAGIC %run "../EnvironmentSetup"

# COMMAND ----------

data_dict = pkl.load(open('/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/TEMP_DATA/DEMO_NEW_YAFSU.pkl', 'rb'))

# COMMAND ----------

data_dict['BASELINE']['NixtlaUnivariateModels']

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %run "../Library/SimpleLagModels"

# COMMAND ----------

TASK_ID = "apfq65CrKEKkl8_UquyXoZYAGp9L" 
EMAIL = "Do-Phuong.Nghi@unilever.com"
INPUT_FORMAT = "XLSX"
KEY_ARR = "BANNER,REGION,CATEGORY,DPNAME".split(",")
TARGET_VARIABLE_ARR = ["ACTUALSALE"]
EXTERNAL_NUMERICAL_VARIABLE_ARR = [""]
EXTERNAL_CATEGORICAL_VARIABLE_ARR = [""]
OUTPUT_GROUP_ARR = "CATEGORY"


TASK_ID = "SNOP"

DEBUG_FOLDER = f"/dbfs/mnt/adls/YAFSU/{TASK_ID}/DEBUG/"

INPUT_FOLDER = f"/dbfs/mnt/adls/YAFSU/{TASK_ID}/INPUT/"
DEBUG_FOLDER = f"/dbfs/mnt/adls/YAFSU/{TASK_ID}/DEBUG/"
OUTPUT_FOLDER = f"/dbfs/mnt/adls/YAFSU/{TASK_ID}/OUTPUT/"
TEMP_FOLDER = "/tmp/TEMP/"

os.makedirs(INPUT_FOLDER, exist_ok=True)
os.makedirs(DEBUG_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
os.makedirs(TEMP_FOLDER, exist_ok=True)

# COMMAND ----------

TASK_ID = "SNOP"
df = pd.read_parquet()

# COMMAND ----------

def read_input_dataset(INPUT_FOLDER, INPUT_FORMAT):
    if INPUT_FORMAT == 'XLSX':
        df_input = read_excel_folder(INPUT_FOLDER, sheet_name='DATA')
    elif INPUT_FORMAT == 'PARQUET':
        for file_item in os.listdir(INPUT_FOLDER):
            os.rename(INPUT_FOLDER + file_item, INPUT_FOLDER + file_item + ".parquet")
            
        df_input = pd.read_parquet(INPUT_FOLDER)
    return df_input

df_input = read_input_dataset(INPUT_FOLDER, INPUT_FORMAT)

# COMMAND ----------

if "KEY" not in df_input.columns:
    df_input['KEY'] = df_input[KEY_ARR].astype(str).apply("|".join, axis=1)

df_input['DATE'] = df_input['YEARWEEK'].astype(str)
df_input['DATE'] = pd.to_datetime(df_input['DATE'] + "-2", format = "%G%V-%w")
df_input['YEARWEEK'] = df_input['YEARWEEK'].astype(int)

# Remove all Week 53
df_input = df_input[(df_input['YEARWEEK'] % 100 != 53)]

# COMMAND ----------

df_calendar = pd.read_excel("/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/LANDING-MASTER/Master ML Calendar.xlsx", sheet_name='BASEWEEK-CALENDAR')
df_calendar = df_calendar[['YEARWEEK', 'HOLIDAYNAME', 'SPECIALEVENTNAME']]

# COMMAND ----------


df_validate_arr = []
for key, df_group in df_input.groupby('KEY'):
  train_max_date = df_group.query('FUTURE == "N"')['DATE'].max()

  df_group = df_group.set_index('DATE')
  df_group = df_group.asfreq('W-TUE')
  df_group = df_group.sort_index()

  df_group['YEARWEEK'] = df_group.index.isocalendar().year * 100 + df_group.index.isocalendar().week

  df_group[TARGET_VARIABLE_ARR] = df_group[TARGET_VARIABLE_ARR].fillna(0)
  df_group[KEY_ARR] = df_group[KEY_ARR].fillna(method='bfill')
  if EXTERNAL_NUMERICAL_VARIABLE_ARR != [""]:
    df_group[EXTERNAL_NUMERICAL_VARIABLE_ARR] = df_group[EXTERNAL_NUMERICAL_VARIABLE_ARR].fillna(method='bfill')
  if EXTERNAL_CATEGORICAL_VARIABLE_ARR != [""]:
    df_group[EXTERNAL_CATEGORICAL_VARIABLE_ARR] = df_group[EXTERNAL_CATEGORICAL_VARIABLE_ARR].fillna(method='bfill')

  df_group['FUTURE'].loc[(df_group.index > train_max_date)] = "Y"
  df_group['FUTURE'].loc[(df_group.index <= train_max_date)] = "N"

  df_validate_arr.append(df_group)

df_validate = pd.concat(df_validate_arr)
df_validate = df_validate.merge(df_calendar, on='YEARWEEK')

# COMMAND ----------

df_validate_arr = []
for key, df_group in df_validate.groupby('KEY'):
  for lag in range(1, 5):
    df_group[[f'HOLIDAYNAME_PAST_{lag}', f'SPECIALEVENTNAME_PAST_{lag}']] = df_group[['HOLIDAYNAME', 'SPECIALEVENTNAME']].shift(lag)
    df_group[[f'HOLIDAYNAME_FORWARD_{lag}', f'SPECIALEVENTNAME_FORWARD_{lag}']] = df_group[['HOLIDAYNAME', 'SPECIALEVENTNAME']].shift(-1*lag)
    df_group = df_group.fillna(method='bfill')
  df_validate_arr.append(df_group)

df_validate = pd.concat(df_validate_arr)


# COMMAND ----------

holiday_cols = ['HOLIDAYNAME', 'SPECIALEVENTNAME',
       'HOLIDAYNAME_PAST_1', 'SPECIALEVENTNAME_PAST_1',
       'HOLIDAYNAME_FORWARD_1', 'SPECIALEVENTNAME_FORWARD_1',
       'HOLIDAYNAME_PAST_2', 'SPECIALEVENTNAME_PAST_2',
       'HOLIDAYNAME_FORWARD_2', 'SPECIALEVENTNAME_FORWARD_2',
       'HOLIDAYNAME_PAST_3', 'SPECIALEVENTNAME_PAST_3',
       'HOLIDAYNAME_FORWARD_3', 'SPECIALEVENTNAME_FORWARD_3',
       'HOLIDAYNAME_PAST_4', 'SPECIALEVENTNAME_PAST_4',
       'HOLIDAYNAME_FORWARD_4', 'SPECIALEVENTNAME_FORWARD_4']
df_validate = pd.get_dummies(df_validate, prefix=holiday_cols, columns=holiday_cols)
df_validate = df_validate[[col for col in list(df_validate.columns) if "NORMAL" not in col]]

# COMMAND ----------

df_validate = df_validate.rename(columns = lambda x:re.sub('[^A-Za-z0-9_]+', '', x)) 

# COMMAND ----------

if EXTERNAL_NUMERICAL_VARIABLE_ARR == [""]:
  EXTERNAL_NUMERICAL_VARIABLE_ARR = [col for col in df_validate.columns if "HOLIDAYNAME" in col or "SPECIALEVENTNAME" in col]
else: EXTERNAL_NUMERICAL_VARIABLE_ARR = EXTERNAL_NUMERICAL_VARIABLE_ARR + [col for col in df_validate.columns if "HOLIDAYNAME" in col or "SPECIALEVENTNAME" in col]

# COMMAND ----------

df_validate['DATE'] = df_validate['YEARWEEK'].astype(str)
df_validate['DATE'] = pd.to_datetime(df_validate['DATE'] + "-2", format = "%G%V-%w")
df_validate['YEARWEEK'] = df_validate['YEARWEEK'].astype(int)

# COMMAND ----------

from tqdm import tqdm
result_arr = []
for key, df_group in tqdm(df_validate.groupby('KEY')):
  result = simple_lag_models(key, df_group, target_var='ACTUALSALE', exo_vars=EXTERNAL_NUMERICAL_VARIABLE_ARR, categorical_vars=[])
  result_arr.append(result)

# COMMAND ----------

dbutils.notebook.run("../Application/YAFSU_FaaS_Interface", timeout_seconds=60*60, arguments={
  "TASK_ID": TASK_ID,
  "EMAIL": EMAIL,
  "INPUT_FORMAT": INPUT_FORMAT,
  "KEY_ARR": KEY_ARR,
  "TARGET_VARIABLE_ARR": TARGET_VARIABLE_ARR,
  "EXTERNAL_NUMERICAL_VARIABLE_ARR": EXTERNAL_NUMERICAL_VARIABLE_ARR,
  "EXTERNAL_CATEGORICAL_VARIABLE_ARR": EXTERNAL_CATEGORICAL_VARIABLE_ARR,
  "OUTPUT_GROUP_ARR": OUTPUT_GROUP_ARR
})

# COMMAND ----------



# COMMAND ----------

import os 
import pandas as pd 
import pickle as pkl

os.listdir(DEBUG_FOLDER)

# COMMAND ----------

df_dict = pkl.load(open(DEBUG_FOLDER + 'FORECAST_RAW_RESULT_DICT.pkl', 'rb'))

# COMMAND ----------

df_dict
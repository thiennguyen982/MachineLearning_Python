# Databricks notebook source
# %run "../EnvironmentSetup"

# COMMAND ----------

# MAGIC %run "../Library/YAFSU_All_Models"

# COMMAND ----------

# DBTITLE 1,Initialize & Get Param Input
dbutils.widgets.text("TASK_ID", "(Task ID from MS Tasks Planner)")
dbutils.widgets.text("EMAIL", "(Email of Requester)")
dbutils.widgets.text("INPUT_FORMAT", "(XLSX or PARQUET)")
dbutils.widgets.text("KEY_ARR", "(List of components to create unique Time-series)")
dbutils.widgets.text("TARGET_VARIABLE_ARR", "(List of target variables)")
dbutils.widgets.text("EXTERNAL_NUMERICAL_VARIABLE_ARR", "(List of external variables - numerical)")
dbutils.widgets.text("EXTERNAL_CATEGORICAL_VARIABLE_ARR", "(List of external variables - categorical)")
dbutils.widgets.text("OUTPUT_GROUP_ARR", "(Output GroupBy to Export)")


TASK_ID = dbutils.widgets.get("TASK_ID")
EMAIL = dbutils.widgets.get("EMAIL")
INPUT_FORMAT = dbutils.widgets.get("INPUT_FORMAT")
KEY_ARR = dbutils.widgets.get("KEY_ARR")
TARGET_VARIABLE_ARR = dbutils.widgets.get("TARGET_VARIABLE_ARR")
EXTERNAL_NUMERICAL_VARIABLE_ARR = dbutils.widgets.get("EXTERNAL_NUMERICAL_VARIABLE_ARR")
EXTERNAL_CATEGORICAL_VARIABLE_ARR = dbutils.widgets.get("EXTERNAL_CATEGORICAL_VARIABLE_ARR")
OUTPUT_GROUP_ARR = dbutils.widgets.get("OUTPUT_GROUP_ARR")

# TASK_ID = "apfq65CrKEKkl8_UquyXoZYAGp9L" 
# EMAIL = "Do-Phuong.Nghi@unilever.com"
# INPUT_FORMAT = "XLSX"
# KEY_ARR = "BANNER,REGION,CATEGORY,DPNAME"
# TARGET_VARIABLE_ARR = "ACTUALSALE"
# EXTERNAL_NUMERICAL_VARIABLE_ARR = ""
# EXTERNAL_CATEGORICAL_VARIABLE_ARR = ""
# OUTPUT_GROUP_ARR = "CATEGORY"

#########################################################################################

# TASK_ID = "DEMO"
# EMAIL = "lai-trung-minh.duc@unilever.com"
# INPUT_FORMAT = "XLSX"
# KEY_ARR = "BANNER,DISTRIBUTOR,CATEGORY,DPNAME"
# TARGET_VARIABLE_ARR = "PCS_Friday,PCS_Monday,PCS_Saturday,PCS_Thursday,PCS_Tuesday,PCS_Wednesday,PCS_Week,PCS_S1,PCS_S2"
# EXTERNAL_NUMERICAL_VARIABLE_ARR = ""
# EXTERNAL_CATEGORICAL_VARIABLE_ARR = ""

#########################################################################################

KEY_ARR = KEY_ARR.split(",") if "," in KEY_ARR else [KEY_ARR]
TARGET_VARIABLE_ARR = TARGET_VARIABLE_ARR.split(",") if "," in TARGET_VARIABLE_ARR else [TARGET_VARIABLE_ARR]

EXTERNAL_NUMERICAL_VARIABLE_ARR = EXTERNAL_NUMERICAL_VARIABLE_ARR.split(",") if "," in EXTERNAL_NUMERICAL_VARIABLE_ARR else [EXTERNAL_NUMERICAL_VARIABLE_ARR]
EXTERNAL_CATEGORICAL_VARIABLE_ARR = EXTERNAL_CATEGORICAL_VARIABLE_ARR.split(",") if "," in EXTERNAL_CATEGORICAL_VARIABLE_ARR else [EXTERNAL_CATEGORICAL_VARIABLE_ARR]
EXTERNAL_NUMERICAL_VARIABLE_ARR = [] if EXTERNAL_NUMERICAL_VARIABLE_ARR == [''] else EXTERNAL_NUMERICAL_VARIABLE_ARR
EXTERNAL_CATEGORICAL_VARIABLE_ARR = [] if EXTERNAL_CATEGORICAL_VARIABLE_ARR == [''] else EXTERNAL_CATEGORICAL_VARIABLE_ARR
OUTPUT_GROUP_ARR = OUTPUT_GROUP_ARR.split(",") if "," in OUTPUT_GROUP_ARR else [OUTPUT_GROUP_ARR]

# COMMAND ----------

INPUT_FOLDER = f"/dbfs/mnt/adls/YAFSU/{TASK_ID}/INPUT/"
DEBUG_FOLDER = f"/dbfs/mnt/adls/YAFSU/{TASK_ID}/DEBUG/"
OUTPUT_FOLDER = f"/dbfs/mnt/adls/YAFSU/{TASK_ID}/OUTPUT/"
TEMP_FOLDER = "/tmp/TEMP/"

os.makedirs(INPUT_FOLDER, exist_ok=True)
os.makedirs(DEBUG_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
os.makedirs(TEMP_FOLDER, exist_ok=True)

# COMMAND ----------

# DBTITLE 1,Read Input Dataset
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

# def subset_data(df_input):
#     df_subset = df_input.groupby(['BANNER', 'DISTRIBUTOR'])['PCS_Week'].sum().reset_index()

#     df_subset['MAX BANNER'] = df_subset.groupby(['BANNER'])['PCS_Week'].transform('max')

#     df_subset['CHOSEN'] = df_subset['MAX BANNER'] == df_subset['PCS_Week']

#     df_subset = df_subset.query("CHOSEN == True")

#     for distributor in ['0050100162', '0050100212', '0015453430', '0050100017', '0015479003', '0050100203']:
#         df_output = df_input.query(f"DISTRIBUTOR == '{distributor}'")
#         df_output.to_excel(TEMP_FOLDER + f"{distributor}_LINES_{df_output.shape[0]}.xlsx", index=False, sheet_name='DATA')

#     import shutil
#     for file_item in os.listdir(TEMP_FOLDER):
#         shutil.copy(TEMP_FOLDER + file_item, INPUT_FOLDER + file_item)

# COMMAND ----------

# DBTITLE 1,Feature Builder & Calendar Mapping
df_calendar = pd.read_excel("/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/LANDING-MASTER/Master ML Calendar.xlsx", sheet_name='BASEWEEK-CALENDAR')
df_calendar = df_calendar[['YEARWEEK', 'HOLIDAYNAME', 'SPECIALEVENTNAME']]

if 'YEARWEEK' not in df_input.columns and 'DATE' in df_input.columns:
    df_input['DATE'] = pd.to_datetime(df_input['DATE'])
    df_input['YEARWEEK'] = df_input['DATE'].dt.isocalendar().year*100 + df_input['DATE'].dt.isocalendar().week

# Remove all Week 53
df_input = df_input[(df_input['YEARWEEK'] % 100 != 53)]

# Create KEY
if "KEY" not in df_input.columns:
    df_input['KEY'] = df_input[KEY_ARR].astype(str).apply("|".join, axis=1)

# Convert YEARWEEK to DATE
df_input['DATE'] = df_input['YEARWEEK'].astype(str)
df_input['DATE'] = pd.to_datetime(df_input['DATE'] + "-2", format = "%G%V-%w")
df_input['YEARWEEK'] = df_input['YEARWEEK'].astype(int)

# This is for debug model only
# df_input['FUTURE'] = "N"
# df_input['FUTURE'].loc[(df_input['YEARWEEK'] >= 202240)] = "Y"

# COMMAND ----------

#### Add FUTURE SKU ####
def fillin_value_missing_date(df_input):
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
    df_validate = df_validate.reset_index()
    df_validate = df_validate.rename(columns={'index': 'DATE'})
    return df_validate

def build_holidays_features(df_validate, df_calendar):
    df_validate = df_validate.merge(df_calendar, on='YEARWEEK')

    df_validate_arr = []
    for key, df_group in df_validate.groupby('KEY'):
        for lag in range(1, 5):
            df_group[[f'HOLIDAYNAME_PAST_{lag}', f'SPECIALEVENTNAME_PAST_{lag}']] = df_group[['HOLIDAYNAME', 'SPECIALEVENTNAME']].shift(lag)
            df_group[[f'HOLIDAYNAME_FORWARD_{lag}', f'SPECIALEVENTNAME_FORWARD_{lag}']] = df_group[['HOLIDAYNAME', 'SPECIALEVENTNAME']].shift(-1*lag)
            df_group = df_group.fillna(method='bfill')
        df_validate_arr.append(df_group)

    df_validate = pd.concat(df_validate_arr)

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
    df_validate = df_validate.rename(columns = lambda x:re.sub('[^A-Za-z0-9_]+', '', x)) 
    return df_validate


df_validate = fillin_value_missing_date(df_input)
df_validate = build_holidays_features(df_validate, df_calendar)
#### Check Data Sufficient

# COMMAND ----------

holidays_columns = [col for col in df_validate.columns if 'HOLIDAYNAME' in col or 'SPECIALEVENTNAME' in col]
EXTERNAL_NUMERICAL_VARIABLE_ARR = EXTERNAL_NUMERICAL_VARIABLE_ARR + holidays_columns

# COMMAND ----------

# DBTITLE 1,Model Running
forecast_result_dict = {}
# forecast_result_dict['DATA_INPUT'] = df_input
EXTERNAL_NUMERICAL_VARIABLE_ARR = [re.sub('[^A-Za-z0-9_]+', '', x) for x in EXTERNAL_NUMERICAL_VARIABLE_ARR]
EXTERNAL_CATEGORICAL_VARIABLE_ARR = [re.sub('[^A-Za-z0-9_]+', '', x) for x in EXTERNAL_CATEGORICAL_VARIABLE_ARR]

for target_var in TARGET_VARIABLE_ARR:       
    forecast_result_dict[target_var] = YAFSU_solution_run(df_validate, target_var, exo_vars=EXTERNAL_NUMERICAL_VARIABLE_ARR, categorical_vars=EXTERNAL_CATEGORICAL_VARIABLE_ARR)

# COMMAND ----------

import pickle as pkl 
pkl.dump(forecast_result_dict, open(DEBUG_FOLDER + "FORECAST_RAW_RESULT_DICT.pkl", "wb"))
pkl.dump(df_input, open(DEBUG_FOLDER + "FORECAST_RAW_INPUT.pkl", "wb"))

# COMMAND ----------

display(forecast_result_dict[TARGET_VARIABLE_ARR[0]]['SimpleLagModels'])

# COMMAND ----------

# ### DEBUG ###
# import pickle as pkl
# TASK_ID = "42xWrm7hJUKpgWhW_mL4v5YAI8uB"
# DEBUG_FOLDER = f"/dbfs/mnt/adls/YAFSU/{TASK_ID}/DEBUG/"
# forecast_result_dict = pkl.load(open(DEBUG_FOLDER + "FORECAST_RAW_RESULT_DICT.pkl", "rb"))
# forecast_result_dict['EST DEMAND']['SimpleLagModels']['ERROR_EST DEMAND'].iloc[0]

# COMMAND ----------

df_forecast_arr = []
for target_var in TARGET_VARIABLE_ARR:
    df_forecast = forecast_result_dict[target_var]['SimpleLagModels']
    df_forecast = df_forecast[['DATE', 'KEY', f'FC_LAG_{target_var}']]
    df_forecast_arr.append(df_forecast)

df_forecast_arr = pd.concat(df_forecast_arr)
df_forecast_arr = df_forecast_arr.groupby(['DATE', 'KEY']).sum().reset_index()
df_forecast_arr['YEARWEEK'] = df_forecast_arr['DATE'].dt.isocalendar().year * 100 + df_forecast_arr['DATE'].dt.isocalendar().week
df_forecast_arr.drop(columns=['DATE'], inplace=True)

df_input = df_input.merge(df_forecast_arr, on=['KEY', 'YEARWEEK'], how='left')
df_input = df_input.fillna(0)

# COMMAND ----------

# KEY_ARR.remove('DPNAME')

# COMMAND ----------

def write_excel_dataframe(df_all, groupby_arr, dbfs_directory):
    tasks = []
    df_all[groupby_arr] = df_all[groupby_arr].astype(str)
    for key, df_group in df_all.groupby(groupby_arr):
        file_name = key
        if type(file_name) != str: 
            file_name = '_'.join(file_name)
        sheet_name = 'DATA'
        task = REMOTE_write_excel_file.remote(df_group, file_name, sheet_name, dbfs_directory)
        tasks.append(task)
        
    tasks = ray.get(tasks)
    return 1

write_excel_dataframe(df_input, groupby_arr=OUTPUT_GROUP_ARR, dbfs_directory=OUTPUT_FOLDER)

# COMMAND ----------

import requests
URL = "https://prod-180.westeurope.logic.azure.com:443/workflows/62431f1f63af4d44913277437f0561be/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=qj-z36ZxyFstF5mURWmitq4njpVzVbI5Wpipfb6Hq64"
body_json = {
    "Email": EMAIL,
    "ADLS_Output_Files": os.listdir(OUTPUT_FOLDER),
    "ADLS_Debug_Files": os.listdir(DEBUG_FOLDER),
    "TaskID": TASK_ID
}

r = requests.post(URL, json=body_json)
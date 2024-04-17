# Databricks notebook source
# MAGIC %run "../Library/YAFSU_All_Models"

# COMMAND ----------

import logging
logger = spark._jvm.org.apache.log4j
logging.getLogger("py4j.clientserver").setLevel(logging.ERROR)

# COMMAND ----------

DF = pd.read_parquet("/dbfs/mnt/adls/YAFSU/SNOP/SNOP_BASELINE.parquet")
DF = DF[(DF['CATEGORY'].isin(['FABSEN', 'FABSOL', 'HAIR', 'HNH', 'IC',
       'ORAL', 'SAVOURY', 'SKINCARE', 'SKINCLEANSING', 'TBRUSH']))]
# DF = DF[(DF['CATEGORY'].isin(['HAIR', 'FABSOL']))]
# DF = DF.query("BANNER == 'NATIONWIDE'")
# DF_CATE = DF.query("BANNER == 'NATIONWIDE'").query("CATEGORY == 'FABSOL'")
# DF_PROD = DF.query("KEY == 'NATIONWIDE|FABSOL|OMO RED 6000 GR' ")
# nixtla_forecast_univariate_models(key="SURF LIQUID PREMIUM POUCH 3.5KG", df_group=DF_PROD, target_var='BASELINE', exo_vars=[], categorical_vars=[])

# COMMAND ----------

df_result_dict = {}
# for target_var in ['ACTUALSALE', 'DAILY_AVG_SALES', 'BASELINE', 'DAILY_AVG_SALES_BASELINE']:
# for target_var in ['ACTUALSALE']:
for target_var in ['DAILY_AVG_SALES', 'DAILY_AVG_SALES_BASELINE']:
    exo_vars = []
    categorical_vars = []
    forecast_dict = YAFSU_solution_run(DF, target_var, exo_vars, categorical_vars)
    df_result_dict[target_var] = forecast_dict

# COMMAND ----------

import pickle
pickle.dump(df_result_dict, open("/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/TEMP_DATA/DEMO_NEW_YAFSU.pkl", "wb"))

# COMMAND ----------

# import pandas as pd 
# import pickle as pkl

# data = pkl.load(open('/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/TEMP_DATA/DEMO_NEW_YAFSU.pkl', 'rb'))
# df = data['DAILY_AVG_SALES_BASELINE']['NixtlaUnivariateModels']

# df = df[['KEY', 'YEARWEEK', 'ACTUALSALE', 'DAILY_AVG_SALES', 'BASELINE',
#        'FUTURE', 'DATE', 'DTWORKINGDAY', 'DAILY_AVG_SALES_BASELINE', 'BANNER',
#        'CATEGORY', 'DPNAME', 
#        'LGBMRegressor_DAILY_AVG_SALES_BASELINE', 
#        'FC_ML_MEAN_DAILY_AVG_SALES_BASELINE',
#        'FC_DL_MEAN_DAILY_AVG_SALES_BASELINE',
#        'FC_MEAN_ALL_DAILY_AVG_SALES_BASELINE',
#        'ERROR_DAILY_AVG_SALES_BASELINE'
#        ]]
# write_excel_dataframe(df, 'CATEGORY', 'DATA', dbfs_directory='/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/TEMP_DATA/BASELINE_FOLDER/')

# COMMAND ----------



# COMMAND ----------


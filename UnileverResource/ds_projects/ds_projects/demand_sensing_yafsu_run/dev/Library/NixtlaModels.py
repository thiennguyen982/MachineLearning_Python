# Databricks notebook source
# MAGIC %run "../EnvironmentSetup"

# COMMAND ----------

# MAGIC %run "../Library/YAFSU_Utils"

# COMMAND ----------

from neuralforecast import NeuralForecast
from neuralforecast.models import NBEATS, NHITS, NBEATSx, LSTM, TCN, GRU

from mlforecast import MLForecast
import lightgbm as lgb
import xgboost as xgb
from sklearn.ensemble import RandomForestRegressor

from mlforecast.target_transforms import Differences
from numba import njit
from window_ops.expanding import expanding_mean
from window_ops.rolling import rolling_mean

# COMMAND ----------

def _nixtla_deeplearning_models(df_train, df_future, horizon, df_static=None, 
                        futr_exog_list=None, hist_exog_list=None, stat_exog_list=None):

  models = [
    NBEATS(input_size=2 * horizon, h=horizon, max_epochs=100),
    NHITS(input_size=2 * horizon, h=horizon, max_epochs=100),
    LSTM(input_size=2 * horizon, h=horizon, max_epochs=100),
    TCN(input_size=2 * horizon, h=horizon, max_epochs=100),
    GRU(input_size=2 * horizon, h=horizon, max_epochs=100)
    # NHITS(input_size=2 * horizon, h=horizon, max_epochs=100,
    #         futr_exog_list=futr_exog_list, 
    #         hist_exog_list=hist_exog_list,
    #         stat_exog_list=stat_exog_list
    #       ), 
    # NBEATSx(input_size=2 * horizon, h=horizon, max_epochs=100, progress_bar_refresh_rate=0,
    #         futr_exog_list=futr_exog_list, 
    #         hist_exog_list=hist_exog_list,
    #         stat_exog_list=stat_exog_list)
    ]
  
  nixtla = NeuralForecast(
    models=models,
    freq='W-THU',
    # num_threads=1
  )

  if df_static != None:
    nixtla.fit(df=df_train, static_df=df_static)
  else:
    nixtla.fit(df=df_train)

  if futr_exog_list != None:
    df_prediction = nixtla.predict(futr_df=df_future)
  else:
    df_prediction = nixtla.predict()

  df_prediction = df_prediction.reset_index()
  df_prediction['FC_DL_MEAN'] = df_prediction.mean(axis=1)

  return df_prediction

# COMMAND ----------

def _nixtla_machinelearning_models(df_train, df_future, horizon):
  models = [
    lgb.LGBMRegressor(),
    xgb.XGBRegressor(),
    RandomForestRegressor()
  ]

  nixtla = MLForecast(
    models=models,
    freq='W-THU',
    lags=[1, 2, 4, 13, 26, 52],
    lag_transforms={
        1: [(rolling_mean, 4), (rolling_mean, 13), (rolling_mean, 26), expanding_mean],
        2: [(rolling_mean, 4), (rolling_mean, 13), (rolling_mean, 26), expanding_mean],
        4: [(rolling_mean, 4), (rolling_mean, 13), (rolling_mean, 26)],
        13: [(rolling_mean, 4), (rolling_mean, 13), (rolling_mean, 26)],
        26: [(rolling_mean, 4), (rolling_mean, 13), (rolling_mean, 26)],
        52: [(rolling_mean, 4)],
    },
    date_features=['year', 'month', 'week'],
    target_transforms=[Differences([1])],
    # num_threads=1
  )

  nixtla.fit(df_train)

  df_prediction = nixtla.predict(horizon)
  df_prediction['FC_ML_MEAN'] = df_prediction.mean(axis=1)

  return df_prediction

# COMMAND ----------

# df_data = pd.read_parquet("/dbfs/mnt/adls/YAFSU/SNOP/INPUT/")
# key = 'NATIONWIDE|FABSOL|OMO LIQUID MATIC FL EXPERT (POU) 3.7KG'
# df_demo = df_data.query(f"KEY == '{key}' ")
# df_demo = df_demo.reset_index()

# df_train_data = df_data[(df_data['FUTURE'] == 'N')]
# df_train_data = df_train_data[['KEY', 'DATE', 'BASELINE']]
# key = "NATIONWIDE|FABSOL|Omo Liquid Matic CFT SS (pou) 3.7KG".upper()
# df_train_data = df_train_data.query(f"KEY == '{key}' ")
# df_train_data.columns = ['unique_id', 'ds', 'y']
# df_train_data['ds'] = pd.to_datetime(df_train_data['ds'])

# ml_predict = machinelearning_models(df_train_data, df_future=None, horizon=26)
# deepneural_predict = deeplearning_models(df_train_data, df_future=None, horizon=26)
# df_result = ml_predict.merge(deepneural_predict, on=['unique_id', 'ds'])
# df_result['FC_MEAN'] = df_result.mean(axis=1)

# COMMAND ----------

def nixtla_forecast_univariate_models(key, df_group, target_var, exo_vars, categorical_vars):
  df = df_group

  error_messages = ""
  horizon = len(df[(df['FUTURE'] == 'Y')])
  if horizon < 4:
    error_messages = "Future Horizon < 4 Weeks"
    return pd.DataFrame([{
      "KEY": key, 
      f"ERROR_{target_var}": error_messages
    }])

  train_max_date = df.query('FUTURE == "N"')['DATE'].max()
  forecast_min_date = df.query('FUTURE == "Y"')['DATE'].min()

  df = df.rename(columns={'DATE': 'ds'})
  df = df.set_index('ds')
  df = df.asfreq('W-TUE')
  df = df.sort_index()
  df['FUTURE'].loc[(df.index > train_max_date)] = "Y"
  df['FUTURE'].loc[(df.index <= train_max_date)] = "N"
  df[target_var] = df[target_var].fillna(0)
  df = df.fillna(method='bfill')
  df = df.reset_index()

  df_train = df[(df['FUTURE'] == 'N')]
  df_train = df_train[['KEY', 'ds', target_var]]
  df_train.columns = ['unique_id', 'ds', 'y']

  horizon = len(df[(df['FUTURE'] == 'Y')])

  ml_predict, deepneural_predict = pd.DataFrame(), pd.DataFrame() 
  try:
    ml_predict = machinelearning_models(df_train, df_future=None, horizon=horizon)
  except Exception as ex:
    error_messages = error_messages + "ML:" + str(ex) + "\n"
  
  try:
    deepneural_predict = deeplearning_models(df_train, df_future=None, horizon=horizon)
  except Exception as ex:
    error_messages = error_messages + "DL:" + str(ex) + "\n"

  if len(ml_predict) == 0 and len(deepneural_predict) == 0:
    return pd.DataFrame([{
            "KEY": key, 
            f"ERROR_{target_var}": error_messages
    }])

  if len(ml_predict) > 0 and len(deepneural_predict) == 0:
    df_prediction = ml_predict
  if len(ml_predict) == 0 and len(deepneural_predict) > 0:
    df_prediction = deepneural_predict
  if len(ml_predict) > 0 and len(deepneural_predict) > 0:
    df_prediction = ml_predict.merge(deepneural_predict, on=['unique_id', 'ds'])

  forecast_cols = [col for col in df_prediction.columns if col != 'unique_id' and col != 'ds']
  df_prediction[f'FC_MEAN_ALL_{target_var}'] = df_prediction[forecast_cols].mean(axis=1)
  df_prediction[f'FC_MEDIAN_ALL_{target_var}'] = df_prediction[forecast_cols].median(axis=1)
  df_prediction[f'FC_STD_ALL_{target_var}'] = df_prediction[forecast_cols].std(axis=1)
  cols_rename_dict = {'unique_id': 'KEY', 'ds': 'DATE'}

  for col in forecast_cols:
    cols_rename_dict[col] = f'{col}_{target_var}'
  df_prediction.rename(columns=cols_rename_dict, inplace=True)
  
  df_group = df_group.merge(df_prediction, on=['KEY', 'DATE'], how='left')
  df_group = df_group.fillna(0)

  return df_group
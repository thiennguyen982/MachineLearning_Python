# Databricks notebook source
# MAGIC %pip install "flaml[ts_forecast]"
# MAGIC %pip install ESRNN
# MAGIC %pip install neuralforecast

# COMMAND ----------

# MAGIC %run "../EnvironmentSetup"

# COMMAND ----------

from flaml import AutoML

# COMMAND ----------

df_data = pd.read_parquet("/dbfs/mnt/adls/YAFSU/SNOP/INPUT/")
key = 'NATIONWIDE|FABSOL|OMO LIQUID MATIC FL EXPERT (POU) 3.7KG'
df_demo = df_data.query(f"KEY == '{key}' ")
df_demo = df_demo.reset_index()

# COMMAND ----------

df_demo

# COMMAND ----------



# COMMAND ----------

from neuralforecast import NeuralForecast
from neuralforecast.models import NBEATS, NHITS

def neuralforecast_forecast(key, df_group, target_var):
  df_group = df_group.sort_values(by=['YEARWEEK'])
  df = df_group
  df['ds'] = df['DATE']
  df = df.set_index('DATE')
  df = df.asfreq('W-TUE')
  df = df.fillna(0)
  # df = df.reset_index()

  df = df[['KEY', 'ds', target_var, 'DTWORKINGDAY', 'FUTURE']]
  df.columns = ['unique_id', 'ds', 'y', 'x', 'FUTURE']

  train_df = df[(df['FUTURE'] == 'N')][['unique_id', 'ds', 'x', 'y']]
  test_df = df[['unique_id', 'ds', 'x', 'y']]

  horizon = 52
  models = [NBEATS(input_size=2 * horizon, h=horizon, max_epochs=50),
            NHITS(input_size=2 * horizon, h=horizon, max_epochs=50)]
  nf = NeuralForecast(models=models, freq='W-TUE')
  nf.fit(df=train_df, verbose=0)
  
  y_hat_df = nf.predict().reset_index()
  return y_hat_df

y_hat_df = neuralforecast_forecast(key, df_group=df_demo, target_var='BASELINE')

# COMMAND ----------

y_hat_df['LGBM'] = pd.Series(df_result)

# COMMAND ----------

df_demo_result = df_demo 
df_demo_result.rename(columns={'DATE': 'ds'}, inplace = True)
df_demo_result = df_demo_result.merge(y_hat_df, on='ds', how='left')
# df_demo_result = df_demo_result.set_index('DATE')
# df_demo_result = df_demo_result.asfreq('W-TUE')


# COMMAND ----------

display(df_demo_result)

# COMMAND ----------

def indirect_forecast_models_feature_engineering(key, df_group, target_var):
  df_group = df_group.sort_values(by=['YEARWEEK'])
  
  cols_to_lag = [target_var]
  # Calculate Moving Average
  for moving_average in [4, 8, 13]:
    try:
      df_group[f'MA{moving_average}_{target_var}'] = df_group[target_var].rolling(moving_average).mean().fillna(0)
    except:
      df_group[f'MA{moving_average}_{target_var}'] = 0
    cols_to_lag.append(f'MA{moving_average}_{target_var}')

  # Calculate Lags of Target_Var and Moving_Average
  exo_vars = []
  for col in cols_to_lag: 
    for lag in range(52, 52 + 13 + 1):
      lag_name = f'LAG_{lag}_{col}'
      exo_vars.append(lag_name)
      try:      
        df_group[lag_name] = df_group[col_to_lag].shift(lag).fillna(method="bfill").fillna(method="ffill").fillna(0)
      except:
        df_group[lag_name] = 0
    
  return df_group, exo_vars

# COMMAND ----------

from sklearn.preprocessing import SplineTransformer
from sklearn.compose import ColumnTransformer

def periodic_spline_transformer(period, n_splines=None, degree=3):
    if n_splines is None:
        n_splines = period
    n_knots = n_splines + 1  # periodic and include_bias is True
    return SplineTransformer(
        degree=degree,
        n_knots=n_knots,
        knots=np.linspace(0, period, n_knots).reshape(n_knots, 1),
        extrapolation="periodic",
        include_bias=True,
    )



def indirect_forecast_models(key, df_group, target_var, exo_vars, categorical_vars):
  df_group, additional_exo_vars = indirect_forecast_models_feature_engineering(key, df_group, target_var)
  # model_exo_vars = additional_exo_vars + exo_vars
  model_exo_vars = exo_vars

  df = df_group[['YEARWEEK', 'FUTURE', target_var, *model_exo_vars, *categorical_vars]]
  df['WEEK'] = df['YEARWEEK'] % 100
  df['YEAR'] = df['YEARWEEK'] // 100
  df['DATE'] = df['YEARWEEK'].astype(str)
  df['DATE'] = pd.to_datetime(df['DATE'] + "-2", format="%G%V-%w")
  
  df = df.set_index('DATE')
  df = df.asfreq('W-TUE')
  # df = df.resample("W-TUE").mean()
  df = df.fillna(0)
  df = df.reset_index()
  # df = df.asfreq('W-TUE')
  # df = df.fillna(0)

  df = df.drop(columns=['YEARWEEK'])

  # splines = periodic_spline_transformer(52, n_splines=13).fit_transform(df[['WEEK']])
  # splines_df = pd.DataFrame(
  #     splines,
  #     columns=[f"spline_{i}" for i in range(splines.shape[1])],
  # )
  # df = pd.concat([df, splines_df], axis="columns")
  
  # df = pd.get_dummies(df, prefix=['WEEK'], columns=['WEEK'])
  # print (df.columns)

  df_train = df.query('FUTURE == "N"')
  df_forecast = df.query('FUTURE == "Y"')

  y_train = df_train[target_var]
  X_train = df_train.drop(columns=[target_var, 'FUTURE'])
  X_forecast = df_forecast.drop(columns=[target_var, 'FUTURE'])  

  automl = AutoML()
  automl_settings = {
    "time_budget": 5,  # in seconds
    "metric": 'mape',
    "task": 'ts_forecast_regression',
    "estimator_list": ['lgbm', 'rf', 'xgboost'],
    "eval_method": "holdout",
    "log_type": "all",
    "label": target_var,
    "period": 13
  }

  automl.fit(X_train, y_train, **automl_settings)

  prediction = automl.predict(X_forecast)

  return prediction

# COMMAND ----------



# COMMAND ----------

df_result

# COMMAND ----------

df_result = indirect_forecast_models(key, df_group=df_demo, target_var='BASELINE', exo_vars=[], categorical_vars=[])
df_demo['VALIDATE'] = 0
df_demo['VALIDATE'].loc[(df_demo['FUTURE'] == 'Y')] = pd.Series(df_result)
display(df_demo)

# COMMAND ----------



# COMMAND ----------

# df_demo = df_demo.reset_index()


# COMMAND ----------

display(pd.DataFrame({'FC': df_result}))

# COMMAND ----------

import pickle as pkl
import pandas as pd 

data = pkl.load(open('/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/TEMP_DATA/FORECAST_RESULT_MULTI_BASELINE_ONLY.pkl', 'rb'))

# COMMAND ----------

data_actualsale = data['ACTUALSALE']
data_actualsale = data_actualsale[['DATE', 'KEY', 'PREDICTION_ACTUALSALE']]

# COMMAND ----------

data_actualsale[['BANNER', 'CATEGORY', 'DPNAME']] = data_actualsale['KEY'].str.split('|', expand=True)

# COMMAND ----------

data_actualsale[data_actualsale['CATEGORY'] == 'IC']['BANNER'].unique()

# COMMAND ----------

import pandas as pd

# pd.set_option("display.max_rows", None, "display.max_columns", None)
multi_df = pd.read_csv(
    "https://raw.githubusercontent.com/srivatsan88/YouTubeLI/master/dataset/nyc_energy_consumption.csv"
)

# preprocessing data
multi_df["timeStamp"] = pd.to_datetime(multi_df["timeStamp"])
multi_df = multi_df.set_index("timeStamp")
multi_df = multi_df.resample("D").mean()
multi_df["temp"] = multi_df["temp"].fillna(method="ffill")
multi_df["precip"] = multi_df["precip"].fillna(method="ffill")
multi_df = multi_df[:-2]  # last two rows are NaN for 'demand' column so remove them
multi_df = multi_df.reset_index()

# Using temperature values create categorical values
# where 1 denotes daily tempurature is above monthly average and 0 is below.
def get_monthly_avg(data):
    data["month"] = data["timeStamp"].dt.month
    data = data[["month", "temp"]].groupby("month")
    data = data.agg({"temp": "mean"})
    return data

monthly_avg = get_monthly_avg(multi_df).to_dict().get("temp")

def above_monthly_avg(date, temp):
    month = date.month
    if temp > monthly_avg.get(month):
        return 1
    else:
        return 0

multi_df["temp_above_monthly_avg"] = multi_df.apply(
    lambda x: above_monthly_avg(x["timeStamp"], x["temp"]), axis=1
)

del multi_df["month"]  # remove temperature column to reduce redundancy

# split data into train and test
num_samples = multi_df.shape[0]
multi_time_horizon = 180
split_idx = num_samples - multi_time_horizon
multi_train_df = multi_df[:split_idx]
multi_test_df = multi_df[split_idx:]

multi_X_test = multi_test_df[
    ["timeStamp", "precip", "temp", "temp_above_monthly_avg"]
]  # test dataframe must contain values for the regressors / multivariate variables
multi_y_test = multi_test_df["demand"]

# initialize AutoML instance
automl = AutoML()

# configure AutoML settings
settings = {
    "time_budget": 10,  # total running time in seconds
    "metric": "mape",  # primary metric
    "task": "ts_forecast",  # task type
    "log_file_name": "energy_forecast_categorical.log",  # flaml log file
    "eval_method": "holdout",
    "log_type": "all",
    "label": "demand",
}

# train the model
automl.fit(dataframe=multi_df, **settings, period=multi_time_horizon)

# predictions
print(automl.predict(multi_X_test))

# COMMAND ----------

pd.DataFrame({'CHECK': automl.predict(multi_X_test)}).plot()
# Databricks notebook source
# %run "../EnvironmentSetup"

# COMMAND ----------

from skforecast.ForecasterAutoreg import ForecasterAutoreg

from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor
from lightgbm import LGBMRegressor

import ray
import re
import warnings

# COMMAND ----------

def simple_lag_models(key, df_group, target_var, exo_vars, categorical_vars):
    warnings.filterwarnings("ignore")
    if type(key) != str:
        key = '|'.join(key)    
    # print (key)
    
    try: 
        df = df_group[['DATE', 'FUTURE', target_var, *exo_vars, *categorical_vars]]
        
        if len(categorical_vars) > 0:
            df = pd.get_dummies(df, prefix=categorical_vars, columns=categorical_vars)
            df = df.rename(columns = lambda x:re.sub('[^A-Za-z0-9_]+', '', x))        
            # df.columns = [col.replace(" ", "_") for col in df.columns]
            categorical_vars_ohe = []
            for col in list(df.columns):
                for cate_var in categorical_vars:
                    if cate_var in col:
                        categorical_vars_ohe.append(col)
            
            exo_vars = exo_vars + categorical_vars_ohe
        
        
        train_max_date = df.query('FUTURE == "N"')['DATE'].max()
        forecast_min_date = df.query('FUTURE == "Y"')['DATE'].min()

        
        df = df.rename(columns={'DATE': 'ds'})
        df = df.set_index('ds')
        # print (df[df.index.duplicated()])
        df = df.asfreq('W-TUE')
        df = df.sort_index() 
                
        df['FUTURE'].loc[(df.index > train_max_date)] = "Y"
        df['FUTURE'].loc[(df.index <= train_max_date)] = "N"
        df[target_var] = df[target_var].fillna(0)
        # df['YEARWEEK'] = df.index.dt.isocalendar[0]*100 + df.index.dt.isocalendar[1]
        df = df.fillna(method='bfill')
        
        df_train = df.query('FUTURE == "N"')
        df_forecast = df.query('FUTURE == "Y"')
        forecast_steps = len(df_forecast)
        
        # Define the Model Config: Lags Transform and Model Setup
        lags_arr = [52, 26, 13, 4]
        algorithms_arr = [
            { "NAME": "RF", "MODEL": RandomForestRegressor(random_state=123) }, 
            { "NAME": "XGB", "MODEL": XGBRegressor(random_state=123) },
            { "NAME": "LGBM", "MODEL": LGBMRegressor(random_state=123)},
            # { "NAME": "CATB", "MODEL": CatBoostRegressor(random_state=123)},
        ]
        # Create model array
        models_arr = []
        for lag in lags_arr:
            for algo in algorithms_arr:
                models_arr.append(
                    {
                        "NAME": f"{algo['NAME']}_{lag}",
                        "MODEL": ForecasterAutoreg(
                            regressor = algo['MODEL'],
                            lags = lag
                        )
                    }
                )
        # list_models_name = list(map(lambda x: x['NAME'] ,models_arr))
        # Finish create model array => len(lags_arr) * len(algorithms_arr)
        list_models_name = []
        df_result = pd.DataFrame()
        for model_obj in models_arr:
            model = model_obj['MODEL']            
            name = model_obj['NAME']
            try:
                if len(exo_vars) > 0:
                    model.fit(
                        y = df_train[target_var],
                        exog = df_train[exo_vars],                    
                    )
                    
                    predictions = model.predict(steps=forecast_steps, exog = df_forecast[exo_vars])
                else:
                    model.fit(
                        y = df_train[target_var]
                    )

                    predictions = model.predict(steps=forecast_steps)
                
                
                df_result[name] = predictions
                list_models_name.append(name)
            except Exception as ex_sub:
                # df_result[f'ERROR_{target_var}_{name}'] = str(ex_sub)
                pass
        
        df_result['KEY'] = key
        df_result = df_result.loc[(df_result.index >= forecast_min_date)]
        df_result = df_result.reset_index()
        df_result = df_result.rename(columns={'index': 'DATE'})
        # df_result['YEARWEEK'] = df_result['DATE'].dt.isocalendar().year * 100 + df_result['DATE'].dt.isocalendar().week
        
        df_result[f'FC_LAG_{target_var}'] = df_result[list_models_name].median(axis=1).round(5)

        # df_result[f'PREDICTION_CV_{target_var}'] = df_result[list_models_name].std(axis=1) / df_result[list_models_name].mean(axis=1)        
        # df_result[f'PREDICTION_{target_var}'] = df_result[f'PREDICTION_{target_var}'].round(5)
        # df_result[f'PREDICTION_CV_{target_var}'] = df_result[f'PREDICTION_CV_{target_var}'].round(2)
        # df_result['PREDICTION_COUNT'] = len(list_models_name)

        # df_result[f'ERROR_{target_var}'] = "NO ERROR"
        return df_result

    except Exception as ex:
        # print (key + "\n" + str(traceback.format_exc()))
        result_obj = {
            "KEY": key, 
            f"ERROR_{target_var}": str(ex)
        }
        return pd.DataFrame([result_obj])
        pass
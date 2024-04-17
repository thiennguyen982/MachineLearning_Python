# Databricks notebook source
# MAGIC %md
# MAGIC # Data Engineering & Feature Engineering

# COMMAND ----------

def quick_data_transforms(df, exo_vars, categorical_vars):
    '''
    appends exo and cat variables to the dataframe, returns the data with additional columns and a list of external variables
    '''
    df_transform = df
    if len(categorical_vars) > 0:
        df_transform = pd.get_dummies(
            df_transform,
            columns=categorical_vars,
            prefix=[f"CATE_{col}" for col in categorical_vars],
        )
    for col in exo_vars:
        df_transform.rename(columns={col: "EXO_" + str(col)}, inplace=True)

    external_vars_for_models = [
        col
        for col in df_transform.columns
        if col.startswith("EXO_") or col.startswith("CATE_")
    ]
    return df_transform, external_vars_for_models

# COMMAND ----------

# MAGIC %md
# MAGIC # Machine Learning Forecast - NIXTLA

# COMMAND ----------

from mlforecast import MLForecast
import lightgbm as lgb
import xgboost as xgb
from sklearn.ensemble import RandomForestRegressor, HistGradientBoostingRegressor
from sklearn.linear_model import Lasso, ElasticNet, BayesianRidge, Ridge

from mlforecast.target_transforms import Differences
from numba import njit
from window_ops.expanding import expanding_mean
from window_ops.rolling import rolling_mean


@with_fail_safe
def nixtla_mlforecast_model(
    key_ts, df_group, target_var, exo_vars, categorical_vars, horizon
):

    df_transform, external_vars_for_models = quick_data_transforms(
        df=df_group, exo_vars=exo_vars, categorical_vars=categorical_vars
    ) # call the exo-cum-categorical transform function to return the dataframe and the external variable list
    df_transform.rename(
        columns={"KEY_TS": "unique_id", "DATE": "ds", target_var: "y"}, inplace=True
    ) #rename the columns KEY_TS, DATE and the target var in the returned dataframe
    max_history_yearweek = df_transform.query("FUTURE == 'N' ")["YEARWEEK"].max() #select the last sales week
    df_transform = df_transform[
        ["unique_id", "ds", "y", "YEARWEEK", *external_vars_for_models]
    ] #select the only the necessary columns

    df_train = df_transform.query(f"YEARWEEK <= {max_history_yearweek} ") # create training dataset
    df_future = df_transform.query(f"YEARWEEK > {max_history_yearweek} ") # create dataframe that contains datapoints after the cutoff week
    horizon = len(df_future) if len(df_future) > 0 else horizon #???

    df_external_regressors = df_transform[
        ["unique_id", "ds", "YEARWEEK", *external_vars_for_models]
    ] #create a data with only keys and external variables, excluding the target var

    models = [
        lgb.LGBMRegressor(),
        xgb.XGBRegressor(),
        RandomForestRegressor(),
        Lasso(),
        ElasticNet(),
        HistGradientBoostingRegressor(),
    ] #create a list of ML models for forecasting

    lag_transform_config = {
            1: [
                (rolling_mean, 4),
                (rolling_mean, 13),
                (rolling_mean, 26),
                expanding_mean,
            ],
            2: [
                (rolling_mean, 4),
                (rolling_mean, 13),
                (rolling_mean, 26),
                expanding_mean,
            ],
            4: [(rolling_mean, 4), (rolling_mean, 13), (rolling_mean, 26)],
            13: [(rolling_mean, 4), (rolling_mean, 13), (rolling_mean, 26)],
            26: [(rolling_mean, 4), (rolling_mean, 13), (rolling_mean, 26)],
            # 52: [(rolling_mean, 4)],
        } # create a dictionary of rolling means
    lags_config = [1, 2, 4, 13, 26]
    
    if len(df_train) > 52:
      lags_config.append(52)
      lag_transform_config.update({
        52: [(rolling_mean, 4)]
      })

    nixtla_run = MLForecast(
        models=models,
        freq="W-THU", #sets Thursday as the end of week date
        lags=lags_config,
        lag_transforms=lag_transform_config,
        date_features=["year", "month", "week"],
        target_transforms=[Differences([1])],
        # num_threads=1
    )

    nixtla_run.fit(df_train)
    # print (df_train.shape, '|', df_external_regressors.shape)
    # df_prediction = nixtla_run.predict(horizon, X_df=df_external_regressors)

    if len(df_external_regressors.columns) > 3:
        df_prediction = nixtla_run.predict(
            horizon, dynamic_dfs=[df_external_regressors]
        )
    else:
        df_prediction = nixtla_run.predict(horizon)

    num = df_prediction._get_numeric_data()
    num[num < 0] = 0

    df_prediction[f"NIXTLA_MLFC_{target_var}_MEDIAN"] = df_prediction.median(
        axis=1, numeric_only=True, skipna=True
    )
    df_prediction[f"NIXTLA_MLFC_{target_var}_MEAN"] = df_prediction.mean(
        axis=1, numeric_only=True, skipna=True
    )
    df_prediction = df_prediction.sort_values(by="ds")

    df_prediction["LAG_FC"] = range(1, df_prediction.shape[0] + 1)
    df_prediction["YEARWEEK"] = (
        df_prediction["ds"].dt.isocalendar().year * 100
        + df_prediction["ds"].dt.isocalendar().week
    )
    df_prediction.rename(columns={"unique_id": "KEY_TS", "ds": "DATE"}, inplace=True)

    return df_prediction

# COMMAND ----------

# MAGIC %md
# MAGIC # Statistical Forecast - Linkedin GreyKite

# COMMAND ----------

@with_fail_safe
def linkedin_greykite_model(
    key_ts, df_group, target_var, exo_vars, categorical_vars, horizon
):
    raise Exception("Not Implemented Yet")

# COMMAND ----------

# MAGIC %md
# MAGIC # ALL IN ONE FUNCTION - If have new methods, then put the function into this

# COMMAND ----------

@ray.remote(num_cpus=1)
def REMOTE_YAFSU_AutoForecast_all_models(
    key_ts, df_group, target_var, exo_vars, categorical_vars, horizon
):
    sys.stdout, sys.stderr = open(os.devnull, "w"), open(os.devnull, "w")
    warnings.filterwarnings("ignore")

    mlforecast_functions = [nixtla_mlforecast_model, linkedin_greykite_model]

    forecast_outputs = [
        func(
            key_ts=key_ts,
            df_group=df_group,
            target_var=target_var,
            exo_vars=exo_vars,
            categorical_vars=categorical_vars,
            horizon=horizon,
        )
        for func in mlforecast_functions
    ]

    return forecast_outputs

    #########################################


def YAFSU_AutoForecast_main_run(df_validated):
    exo_vars, categorical_vars, horizon = (
        EXTERNAL_NUMERICAL_VARS_ARR,
        EXTERNAL_CATEGORICAL_VARS_ARR,
        HORIZON,
    )
    for target_var in TARGET_VARS_ARR:
        forecast_outputs_consolidate = ray.get(
            [
                REMOTE_YAFSU_AutoForecast_all_models.remote(
                    key_ts, df_group, target_var, exo_vars, categorical_vars, horizon
                )
                for key_ts, df_group in df_validated.groupby("KEY_TS")
            ]
        )

        # print (forecast_outputs_consolidate)
        with open(FORECAST_TEMP + f"FC_{target_var}.pkl", "wb") as f:
            pkl.dump(forecast_outputs_consolidate, f)
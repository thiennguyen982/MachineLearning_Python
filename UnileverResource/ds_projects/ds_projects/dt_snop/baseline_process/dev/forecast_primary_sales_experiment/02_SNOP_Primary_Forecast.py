# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Load Data

# COMMAND ----------

# MAGIC %run "/Repos/lai-trung-minh.duc@unilever.com/SC_DT_Forecast_Project/EnvironmentSetup"

# COMMAND ----------

from scipy.stats import linregress
import re
from datetime import timedelta, date

import plotly.graph_objects as go
import plotly.express as px


# COMMAND ----------

DF = pd.read_csv("/Workspace/Users/ng-minh-hoang.dat@unilever.com/Forecast Primary Sales/Primary_baseline_clean.csv")
DF = DF.drop('Unnamed: 0', axis = 1)
DF["DATE"] = pd.to_datetime(DF["DATE"])
DF = DF.rename(columns = {"DTWORKINGDAY":"WORKINGDAY"})
print(DF.shape)
DF.head(2)

# COMMAND ----------

# DF = DF.query("CATEGORY == 'FABSOL'") 

# COMMAND ----------

# def accuracy_check(key, df_group, actual_col, predict_col_arr, Ratio=1):

#     performance = dict()
#     sum_actualsale = df_group[actual_col].sum()

#     performance = {"CATEGORY": key, "Sum_actualsale": sum_actualsale}

#     for predict_col in predict_col_arr:
#         df_group[predict_col] = df_group[predict_col] * Ratio

#         error = sum((df_group[actual_col] - df_group[predict_col]).abs())
#         accuracy = 1 - error / df_group[actual_col].sum()
#         sum_predictsale = df_group[predict_col].sum()

#         performance["Sum_predictsale_" + predict_col] = sum_predictsale
#         performance["Accuracy_" + predict_col] = accuracy
#         performance["Error_" + predict_col] = error

#     return performance

# actual_col = "PRI_SALES"
# predict_col_arr = [
#     "Sec2Pri_SALES_WEEK/MONTH_normalized",
#     "Sec2Pri_SALES_WEEK/MONTH",
#     "Sec2Pri_SALES_WEEK/QUARTER_normalized",
#     "Sec2Pri_SALES_WEEK/QUARTER",
# ]

# COMMAND ----------

# DF = pd.read_parquet(
#     "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/TEMP_DATA/BASELINE.parquet"
# )

# DF_NATIONWIDE = (
#     DF.groupby(["CATEGORY", "DPNAME", "YEARWEEK"])[["ACTUALSALE", "BASELINE"]]
#     .sum()
#     .reset_index()
# )
# # KEY ban dau la` theo BANNER DT (HCME, CENTRAL...) append them data KEY NATIONWIDE groupby nhu tren 45k + 13k
# DF_NATIONWIDE["KEY"] = (
#     "NATIONWIDE|" + DF_NATIONWIDE["CATEGORY"] + "|" + DF_NATIONWIDE["DPNAME"]
# )
# DF = DF.append(DF_NATIONWIDE)

# # # ############# DEBUG #########################
# # DF = DF_NATIONWIDE  # Only run for NATIONWIDE
# # DF = DF.query("CATEGORY == 'FABSOL'") 
# # # ############# DEBUG #########################

# DF = DF[["KEY", "YEARWEEK", "ACTUALSALE", "BASELINE"]]

# ######################################

# df_calendar = pd.read_excel(
#     "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/LANDING-MASTER/Master ML Calendar.xlsx",
#     sheet_name="BASEWEEK-CALENDAR",
#     engine="openpyxl",
# )
# df_calendar = df_calendar[["YEARWEEK", "DTWORKINGDAY"]]

# df_calendar_custom = pd.read_excel(
#     "/dbfs/FileStore/tables/CUSTOM_CALENDAR_FEATURE.xlsx"
# )
# # CALENDAR_FEATURES = list(df_calendar_custom.columns)
# # CALENDAR_FEATURES.remove("YEARWEEK")
# CALENDAR_FEATURES = [
#     "MONTH",
#     "YEAR",
#     "RANK",
#     "WEEK",
#     "QUARTER",
#     "END_PREVMONTH_DAYCOUNT",
# ]

# COMMAND ----------

# CALENDAR_FEATURES

# COMMAND ----------

# df_calendar_custom.head(2)

# COMMAND ----------

# MAGIC %md
# MAGIC # Utils Calculation
# MAGIC - Phasing Ratio
# MAGIC - General Regression Trend

# COMMAND ----------

def calculate_phasing_ratio(key, df_group, target_var):
    warnings.filterwarnings("ignore")

    df_group = df_group[["KEY", "YEARWEEK", "YEAR", "MONTH", "WORKINGDAY", "FUTURE", target_var]]
    df_group["DAILY"] = np.where(
        df_group["WORKINGDAY"] == 0, 0, df_group[target_var] / df_group["WORKINGDAY"]
    )
    df_group = df_group.sort_values("YEARWEEK")
    # df_group["DATE"] = pd.to_datetime(
    #     df_group["YEARWEEK"].astype(str) + "-4", format="%G%V-%w"
    # )
    # df_group["YEAR"] = df_group["DATE"].dt.isocalendar().year
    # df_group["MONTH"] = df_group["DATE"].dt.month
    # df_group["WEEK"] = df_group["DATE"].dt.isocalendar().week

    df_group["COUNT WEEKS"] = (
        df_group.groupby(["YEAR", "MONTH"])["YEARWEEK"].transform("count").astype(int)
    )
    df_group["WEEK ORDER"] = (
        df_group.groupby(["YEAR", "MONTH"])["YEARWEEK"].transform("rank").astype(int)
    )

    df_history = df_group.query("FUTURE == 'N' ")
    df_history["RATIO WEEK/MONTH"] = df_history["DAILY"] / df_history.groupby(
        ["YEAR", "MONTH"]
    )["DAILY"].transform(sum)

    df_ratio_generic = (
        df_history.groupby(["COUNT WEEKS", "WEEK ORDER"])["RATIO WEEK/MONTH"]
        .mean()
        .reset_index()
    )
    df_ratio_generic = df_ratio_generic.query(" `COUNT WEEKS` >= 4")
    df_ratio_generic = df_ratio_generic.rename(
        columns={"RATIO WEEK/MONTH": "RATIO_GENERIC"}
    )

    df_group = df_group.merge(df_ratio_generic, on=["COUNT WEEKS", "WEEK ORDER"])

    df_group["KEY"] = key
    df_group = df_group[["KEY", "YEARWEEK", "RATIO_GENERIC"]]

    return df_group


@ray.remote
def REMOTE_calculate_phasing_ratio(key, df_group, target_var):
    try:
        result = calculate_phasing_ratio(key, df_group, target_var)
        result = {"KEY": key, "ERROR": "NO ERROR", "OUTPUT": result}
    except Exception as ex:
        result = {"KEY": key, "ERROR": str(ex), "OUTPUT": pd.DataFrame()}

    return result

# COMMAND ----------

def calculate_future_trendline(key, df_group, target_var, exo_vars, categorical_vars):
    warnings.filterwarnings("ignore")

    df_group = df_group[["KEY", "YEARWEEK","YEAR", "MONTH", "WORKINGDAY", "FUTURE", target_var]]
    df_group["DAILY"] = np.where(
        df_group["WORKINGDAY"] == 0, 0, df_group[target_var] / df_group["WORKINGDAY"]
    )
    df_group = df_group.sort_values("YEARWEEK")

    # df_group["DATE"] = pd.to_datetime(
    #     df_group["YEARWEEK"].astype(str) + "-4", format="%G%V-%w"
    # )
    # df_group["YEAR"] = df_group["DATE"].dt.isocalendar().year
    # df_group["MONTH"] = df_group["DATE"].dt.month
    # df_group["WEEK"] = df_group["DATE"].dt.isocalendar().week
    # df_group["QUARTER"] = df_group["DATE"].dt.quarter
    df_group["QUARTER"] = ((df_group["MONTH"] - 1) / 3).astype(int) + 1
    df_group["QUARTER_GROUP"] = np.where(df_group["QUARTER"].isin([1, 2]), 1, 2)

    df_group["COUNT WEEKS"] = (
        df_group.groupby(["YEAR", "MONTH"])["YEARWEEK"].transform("count").astype(int)
    )
    df_group["WEEK ORDER"] = (
        df_group.groupby(["YEAR", "MONTH"])["YEARWEEK"].transform("rank").astype(int)
    )

    df_group["YEARWEEK ORDER"] = df_group["YEARWEEK"].transform("rank").astype("int")

    df_history = df_group.query("FUTURE == 'N' ").sort_values("YEARWEEK")
    df_future = df_group.query("FUTURE == 'Y' ")

    fulltime_slope, fulltime_intercept, r_value, p_value, std_err = linregress(
        x=df_history["YEARWEEK ORDER"].values, y=df_history["DAILY"].values
    )

    last_26weeks_slope, last_26weeks_intercept, r_value, p_value, std_err = linregress(
        x=df_history["YEARWEEK ORDER"][-26:].values, y=df_history["DAILY"][-26:].values
    )

    weighted_arr = [1, 0.5]
    intercept_arr = [fulltime_intercept, last_26weeks_intercept]
    slope_arr = [fulltime_slope, last_26weeks_slope]

    forecast_intercept = sum(
        [intercept_arr[idx] * weight for idx, weight in enumerate(weighted_arr)]
    ) / sum(weighted_arr)
    forecast_slope = sum(
        [slope_arr[idx] * weight for idx, weight in enumerate(weighted_arr)]
    ) / sum(weighted_arr)

    # forecast_intercept = (1.5*fulltime_intercept + 0.5*last_26weeks_intercept)/2
    # forecast_slope  = (1.5*fulltime_slope + 0.5*last_26weeks_slope)/2

    # df_future = df_future.merge(
    #     df_ratio_month, on=["MONTH", "COUNT WEEKS", "WEEK ORDER"], how="left"
    # )
    # df_future = df_future.merge(
    #     df_ratio_generic, on=["COUNT WEEKS", "WEEK ORDER"], how="left"
    # )
    # df_future["FINAL_RATIO"] = df_future["RATIO_MONTH"]
    # df_future["FINAL_RATIO"] = df_future["RATIO_GENERIC"]
    # df_future['FINAL_RATIO'] = df_future['FINAL_RATIO'].fillna(df_future['RATIO_GENERIC'])
    # df_future['FINAL_RATIO'] = (df_future['FINAL_RATIO'] + df_future['RATIO_GENERIC'])/2

    df_future["TRENDLINE_DAILY"] = (
        forecast_intercept + forecast_slope * df_future["YEARWEEK ORDER"]
    )
    # df_future["TRENDLINE_DAILY"] = (
    #     df_future.groupby(["YEAR", "MONTH"])["TRENDLINE"].transform("sum")
    #     * df_future["FINAL_RATIO"]
    # )

    # df_future['SLOPE_2_YEAR'] = fulltime_slope
    # df_future['INTERCEPT_2_YEAR'] = fulltime_intercept
    # df_future['SLOPE_26_WEEK'] = last_26weeks_slope
    # df_future['INTERCEPT_26_WEEK'] = last_26weeks_intercept
    # df_future['SLOPE_FUTURE'] =  forecast_slope
    # df_future['INTERCEPT_FUTURE'] = forecast_intercept

    # df_future = df_future[['KEY', 'YEARWEEK', f'FC_{target_var}', *debug_columns]]
    # df_future = df_future[['KEY', 'YEARWEEK', 'MONTH', 'COUNT WEEKS', 'WEEK ORDER', 'FINAL_RATIO', 'TRENDLINE', f'FC_{target_var}']]

    df_result = df_future.append(df_history)
    df_result = df_result[["KEY", "YEARWEEK", "TRENDLINE_DAILY"]]
    return df_result


@ray.remote
def REMOTE_calculate_future_trendline(
    key, df_group, target_var, exo_vars, categorical_vars
):
    try:
        result = calculate_future_trendline(
            key, df_group, target_var, exo_vars, categorical_vars
        )
        result = {"KEY": key, "ERROR": "NO ERROR", "OUTPUT": result}
    except Exception as ex:
        result = {"KEY": key, "ERROR": str(ex), "OUTPUT": pd.DataFrame()}

    return result

# COMMAND ----------

# MAGIC %md
# MAGIC # Facebook Prophet method

# COMMAND ----------

from prophet import Prophet


def prophet_cumsum_forecast(key, df_group, target_var, exo_vars, categorical_vars):
    # This is temporary, need to write log to a file, not throw it to NULL area.
    # sys.stdout, sys.stderr = open(os.devnull, "w"), open(os.devnull, "w")
    warnings.filterwarnings("ignore")
    ################################################################################

    df_group = df_group[["KEY", "YEARWEEK", "WORKINGDAY", "FUTURE", target_var]]
    df_group["DAILY"] = np.where(
        df_group["WORKINGDAY"] == 0, 0, df_group[target_var] / df_group["WORKINGDAY"]
    )

    df_group = df_group.sort_values(by="YEARWEEK")

    df_group["CUMSUM_DAILY"] = df_group["DAILY"].transform("cumsum")
    df_group["ds"] = pd.to_datetime(
        df_group["YEARWEEK"].astype(str) + "-4", format="%G%V-%w"
    )
    df_group["y"] = df_group["CUMSUM_DAILY"]

    df_train = df_group.query("FUTURE == 'N' ")[["ds", "y", *exo_vars]]

    model = Prophet(growth="linear")

    for col in exo_vars:
        model.add_regressor(col)
    model.fit(df_train, iter=5000)

    df_forecast = model.predict(df_group[["ds", *exo_vars]])
    df_forecast = df_forecast[["ds", "yhat", "trend", "additive_terms"]].sort_values(
        "ds"
    )
    df_forecast["FBPROPHET_DAILY"] = df_forecast["yhat"].diff()
    df_forecast["trend"] = df_forecast["trend"].diff()
    df_forecast["additive_terms"] = df_forecast["additive_terms"].diff()
    # df_forecast = df_forecast[["ds", "YHAT_DAILY", 'trend', 'additive_terms']]

    df_forecast = df_forecast[["ds", "FBPROPHET_DAILY"]]
    df_forecast["KEY"] = key
    df_forecast["YEARWEEK"] = (
        df_forecast["ds"].dt.isocalendar().year * 100
        + df_forecast["ds"].dt.isocalendar().week
    )

    df_forecast = df_forecast[["KEY", "YEARWEEK", "FBPROPHET_DAILY"]]
    # df_group["DATE"] = df_group["YEARWEEK"].astype(str)
    # df_group["DATE"] = pd.to_datetime(df_group["DATE"] + "-4", format="%G%V-%w")
    # df_group["YEAR"] = df_group["DATE"].dt.isocalendar().year
    # df_group["MONTH"] = df_group["DATE"].dt.month
    # df_group["WEEK"] = df_group["DATE"].dt.isocalendar().week

    # df_group = df_group.merge(df_forecast, on="ds", how="inner")
    # # df_ratio = calculate_phasing_ratio(key, df_group)
    # # df_group = df_group.merge(df_ratio, on=["KEY", "YEARWEEK"], how="left")

    # # df_group["YHAT_DAILY_PHASING"] = (
    # #     df_group.groupby(["YEAR", "MONTH"])["YHAT_DAILY"].transform("sum")
    # #     * df_group["RATIO_GENERIC"]
    # # )
    # # df_group["YHAT_DAILY_PHASING"] = df_group["YHAT_DAILY_PHASING"].fillna(0)

    return df_forecast


@ray.remote
def REMOTE_prophet_cumsum_forecast(
    key, df_group, target_var, exo_vars, categorical_vars
):
    # f = open(os.devnull, "w")
    # sys.stdout = f

    try:
        result = prophet_cumsum_forecast(
            key, df_group, target_var, exo_vars, categorical_vars
        )
        result = {"KEY": key, "ERROR": "NO ERROR", "OUTPUT": result}
    except Exception as ex:
        result = {"KEY": key, "ERROR": str(ex), "OUTPUT": pd.DataFrame()}

    return result

# COMMAND ----------

# MAGIC %md
# MAGIC # LINKEDIN GreyKite

# COMMAND ----------

from greykite.common.data_loader import DataLoader
from greykite.framework.templates.autogen.forecast_config import ForecastConfig
from greykite.framework.templates.autogen.forecast_config import MetadataParam
from greykite.framework.templates.forecaster import Forecaster
from greykite.framework.templates.model_templates import ModelTemplateEnum
from greykite.framework.utils.result_summary import summarize_grid_search_results

import plotly

# COMMAND ----------

def linkedin_forecast(key, df_group, target_var, exo_vars, categorical_vars):
    # This is temporary, need to write log to a file, not throw it to NULL area.
    # sys.stdout, sys.stderr = open(os.devnull, "w"), open(os.devnull, "w")
    warnings.filterwarnings("ignore")
    ############################################################################

    df_group = df_group[["KEY", "YEARWEEK", "WORKINGDAY", "FUTURE", target_var]]
    df_group["DAILY"] = np.where(
        df_group["WORKINGDAY"] == 0, 0, df_group[target_var] / df_group["WORKINGDAY"]
    )

    df_group["DATE"] = pd.to_datetime(
        df_group["YEARWEEK"].astype(str) + "-4", format="%G%V-%w"
    )

    df_train = df_group.query("FUTURE == 'N' ")
    df_train = df_train[["DATE", "DAILY"]]
    metadata = MetadataParam(
        time_col="DATE",
        value_col="DAILY",
        freq="W-THU",
    )

    forecaster = Forecaster()  # Creates forecasts and stores the result
    result = forecaster.run_forecast_config(  # result is also stored as `forecaster.forecast_result`.
        df=df_train,
        config=ForecastConfig(
            model_template="SILVERKITE",
            forecast_horizon=52,  # forecast 1 year later from train
            coverage=0.95,  #
            metadata_param=metadata,
        ),
    )

    df_forecast = result.forecast.df.round(2) # take result to dataframe and round 2
    df_forecast["KEY"] = key
    df_forecast["YEARWEEK"] = (
        df_forecast["DATE"].dt.isocalendar().year * 100
        + df_forecast["DATE"].dt.isocalendar().week
    )

    df_forecast["LINKEDIN_DAILY"] = df_forecast["forecast"]
    df_forecast = df_forecast[["KEY", "YEARWEEK", "LINKEDIN_DAILY"]]

    return df_forecast


@ray.remote
def REMOTE_linkedin_forecast(key, df_group, target_var, exo_vars, categorical_vars):
    try:
        result = linkedin_forecast(
            key, df_group, target_var, exo_vars, categorical_vars
        )
        result = {"KEY": key, "ERROR": "NO ERROR", "OUTPUT": result}
    except Exception as ex:
        result = {"KEY": key, "ERROR": str(ex), "OUTPUT": pd.DataFrame()}

    return result

# COMMAND ----------

# MAGIC %md
# MAGIC # Main Run for Accuracy Check

# COMMAND ----------

def baseline_snop_forecast(df_dataset, target_var, exo_vars, categorical_vars):
    warnings.filterwarnings("ignore")

    # Run FBPROPHET
    fbprophet_output = ray.get(
        [
            REMOTE_prophet_cumsum_forecast.remote(
                key, df_group, target_var, exo_vars, categorical_vars
            )
            for key, df_group in df_dataset.groupby("KEY")
        ]
    )
    
    # Run LINKEDIN
    linkedin_output = ray.get(
        [
            REMOTE_linkedin_forecast.remote(
                key, df_group, target_var, exo_vars, categorical_vars
            )
            for key, df_group in df_dataset.groupby("KEY")
        ]
    )
    
    # Run TRENDLINE
    future_trendline_output = ray.get(
        [
            REMOTE_calculate_future_trendline.remote(
                key, df_group, target_var, exo_vars, categorical_vars
            )
            for key, df_group in df_dataset.groupby("KEY")
        ]
    )
    
    # Run PHASING
    phasing_output = ray.get(
        [
            REMOTE_calculate_phasing_ratio.remote(key, df_group, target_var)
            for key, df_group in df_dataset.groupby("KEY")
        ]
    )

    df_prophet = pd.concat([item["OUTPUT"] for item in fbprophet_output])
    df_linkedin = pd.concat([item["OUTPUT"] for item in linkedin_output])
    df_future_trendline = pd.concat(
        [item["OUTPUT"] for item in future_trendline_output]
    )
    df_phasing = pd.concat([item["OUTPUT"] for item in phasing_output])
    
    ############ ERROR STACKTRACE #############
    df_prophet_error = pd.DataFrame(
        [
            {"KEY": item["KEY"], "PROPHET_ERROR": item["ERROR"]}
            for item in fbprophet_output
            if item["ERROR"] != "NO ERROR"
        ]
    )

    df_linkedin_error = pd.DataFrame(
        [
            {"KEY": item["KEY"], "LINKEDIN_ERROR": item["ERROR"]}
            for item in linkedin_output
            if item["ERROR"] != "NO ERROR"
        ]
    )
    df_error = df_prophet_error.merge(df_linkedin_error, on="KEY", how="outer").fillna(
        "-"
    )

    # for output in [fbprophet_output, linkedin_output]:
    #     for item in output:
    #         if item["ERROR"] != "NO ERROR":
    #             {"KEY": item["KEY"], "ERROR": item["ERROR"]}

    df_dataset = df_dataset.merge(df_prophet, on=["KEY", "YEARWEEK"], how ='left')
    df_dataset = df_dataset.merge(df_linkedin, on=["KEY", "YEARWEEK"], how ='left')
    df_dataset = df_dataset.merge(df_future_trendline, on=["KEY", "YEARWEEK"], how ='left') 
    df_dataset = df_dataset.merge(df_phasing, on=["KEY", "YEARWEEK"], how ='left')
   

    df_dataset["DAILY"] = np.where(
        df_dataset["WORKINGDAY"] == 0,
        0,
        df_dataset[target_var] / df_dataset["WORKINGDAY"],
    )
    # df_dataset["DATE"] = pd.to_datetime(
    #     df_dataset["YEARWEEK"].astype(str) + "-4", format="%G%V-%w"
    # )

    # df_dataset["YEAR"] = df_dataset["DATE"].dt.year
    # df_dataset["MONTH"] = df_dataset["DATE"].dt.month

    df_forecast = df_dataset.fillna(0)

    #####
    for col in ["FBPROPHET_DAILY", "LINKEDIN_DAILY", "TRENDLINE_DAILY"]:
        df_forecast[col].loc[(df_forecast[col] < 0)] = 0
        df_forecast[f"{col}_PHASING"] = (
            df_forecast.groupby(["KEY", "YEAR", "MONTH"])[col].transform("sum")
            * df_forecast["RATIO_GENERIC"]
        )

    df_forecast["LINKEDIN_DAILY_TREND_PHASING"] = (
        df_forecast["LINKEDIN_DAILY_PHASING"] + df_forecast["TRENDLINE_DAILY"]
    ) / 2

    df_forecast["MEDIAN_METHOD"] = df_forecast[
        [
            "FBPROPHET_DAILY",
            "LINKEDIN_DAILY",
            "FBPROPHET_DAILY_PHASING",
            "LINKEDIN_DAILY_PHASING",
            "TRENDLINE_DAILY_PHASING",
            "LINKEDIN_DAILY_TREND_PHASING",
        ]
    ].median(axis=1)

    return df_forecast, df_error

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### test model

# COMMAND ----------

df_group = DF[DF['KEY'] == 'NATIONWIDE|FABSOL|COMFORT ELEGANT POUCH 3.6KG']
df_group["FUTURE"] = "N"
df_group = df_group[df_group['YEARWEEK'] <= 202335]
df_group["FUTURE"].loc[(df_group["YEARWEEK"] >= 202301)] = "Y"
df_group.head(2)

# COMMAND ----------

df_group = df_group[["KEY", "YEARWEEK", "WORKINGDAY", "FUTURE", 'BASELINE']]
df_group["DAILY"] = np.where(
    df_group["WORKINGDAY"] == 0, 0, df_group['BASELINE'] / df_group["WORKINGDAY"]
)

df_group = df_group.sort_values(by="YEARWEEK")

df_group["CUMSUM_DAILY"] = df_group["DAILY"].transform("cumsum")
df_group["ds"] = pd.to_datetime(
    df_group["YEARWEEK"].astype(str) + "-4", format="%G%V-%w"
)
df_group["y"] = df_group["CUMSUM_DAILY"]
df_group

# COMMAND ----------

df_train = df_group.query("FUTURE == 'N' ")[["ds", "y"]]
df_test = df_group.query("FUTURE == 'Y' ")[["ds", "y"]]

# COMMAND ----------

model = Prophet(growth="linear")

model.fit(df_train, iter=5000)

df_forecast = model.predict(df_group[["ds"]])
print(df_forecast.shape)
df_forecast.tail(2)

# COMMAND ----------

df_forecast = df_forecast[["ds", "yhat", "trend", "additive_terms"]].sort_values(
    "ds"
)
df_forecast["FBPROPHET_DAILY"] = df_forecast["yhat"].diff()
df_forecast["trend"] = df_forecast["trend"].diff()
df_forecast["additive_terms"] = df_forecast["additive_terms"].diff()


# COMMAND ----------

df_forecast = df_forecast.merge(df_group[['ds','DAILY']], on = 'ds')
df_forecast["YEARWEEK"] = (
    df_forecast["ds"].dt.isocalendar().year * 100
    + df_forecast["ds"].dt.isocalendar().week
)
print(df_forecast.shape)
df_forecast.head(2)

# COMMAND ----------

fig = px.line(data_frame = df_forecast, x = 'ds', y = ['FBPROPHET_DAILY','DAILY'])
fig.show()

# COMMAND ----------

#greykite
df_train = df_group.query("FUTURE == 'N' ")[["ds", "DAILY"]]
df_test = df_group.query("FUTURE == 'Y' ")[["ds", "DAILY"]]


# COMMAND ----------

metadata = MetadataParam(
    time_col="ds",
    value_col="DAILY",
    freq="W-THU",
)
metadata

# COMMAND ----------

forecaster = Forecaster()  # Creates forecasts and stores the result
result = forecaster.run_forecast_config(  # result is also stored as `forecaster.forecast_result`.
    df=df_train,
    config=ForecastConfig(
        model_template="SILVERKITE",
        forecast_horizon=35,  # forecast 8 month
        coverage=0.95,  #
        metadata_param=metadata,
    ),
)
result

# COMMAND ----------

df_forecast = result.forecast.df.round(2)

df_forecast = df_forecast.merge(df_group[['ds','DAILY']], on = 'ds')
df_forecast["YEARWEEK"] = (
    df_forecast["ds"].dt.isocalendar().year * 100
    + df_forecast["ds"].dt.isocalendar().week
)
print(df_forecast.shape)
df_forecast.tail(2)

# COMMAND ----------

fig = go.Figure(
  data = [go.Scatter(x = df_forecast['ds'], y = df_forecast['DAILY'], name = 'ACTUAL'),
          go.Scatter(x = df_forecast['ds'], y = df_forecast['forecast'], name = 'FORECAST'),
          go.Scatter(x = df_forecast['ds'], y = df_forecast['forecast_lower'],name='FC_lower'),
          go.Scatter(x = df_forecast['ds'], y = df_forecast['forecast_upper'], fill ='tonexty',name='FC_upper')
]
)
fig.show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Find best accurate methods in the last 52 weeks.

# COMMAND ----------

def accuracy_check(key, df_group, actual_col, predict_col_arr):
    max_accuracy = 0
    max_accuracy_col = "BLANK"
    min_error = 0
    sum_predictsale = 0
    sum_actualsale = df_group[actual_col].sum()
    for col in predict_col_arr:
        error = sum((df_group[actual_col] - df_group[col]).abs())
        accuracy = 1 - error / df_group[actual_col].sum()
        if accuracy > max_accuracy:
            max_accuracy = accuracy
            max_accuracy_col = col
            min_error = error
            sum_predictsale = df_group[col].sum()
    return {
        "KEY": key,
        "MAX_ACCURACY": max_accuracy,
        "COL": max_accuracy_col,
        "SUM_ACTUAL_SALE": sum_actualsale,
        "SUM_PREDICT_SALE": sum_predictsale,
        "ERROR": min_error,
    }

# COMMAND ----------

### Choose the best accuracy methods ###
DF_ACC_CHECK = DF.copy()
DF_ACC_CHECK["FUTURE"] = "N"

# 8/2023
DF_ACC_CHECK = DF_ACC_CHECK[DF_ACC_CHECK['YEARWEEK'] <= 202335]
DF_ACC_CHECK["FUTURE"].loc[(DF_ACC_CHECK["YEARWEEK"] >= 202301)] = "Y"

###################################
# DF_ACC_CHECK["FUTURE"].loc[
#     (DF_ACC_CHECK["YEARWEEK"] > DF_ACC_CHECK["YEARWEEK"].max() - 100)
# ] = "Y"
# DF_ACC_CHECK = DF_ACC_CHECK.merge(df_calendar, on="YEARWEEK")
# DF_ACC_CHECK["WORKINGDAY"] = DF_ACC_CHECK["DTWORKINGDAY"]

DF_ACC_CHECK, DF_ERROR = baseline_snop_forecast(
    DF_ACC_CHECK, target_var="BASELINE", exo_vars=[], categorical_vars=[]
)

# COMMAND ----------

display(DF_ERROR)

# COMMAND ----------

actual_col = "DAILY"
predict_col_arr = [
    "FBPROPHET_DAILY",
    "LINKEDIN_DAILY",
    "FBPROPHET_DAILY_PHASING",
    "LINKEDIN_DAILY_PHASING",
    "TRENDLINE_DAILY_PHASING",
    "LINKEDIN_DAILY_TREND_PHASING",
    "MEDIAN_METHOD",
]
df_accuracy_method = pd.DataFrame(
    [
        accuracy_check(key, df_group, actual_col, predict_col_arr)
        for key, df_group in DF_ACC_CHECK.groupby("KEY")
    ]
)

display(df_accuracy_method)

# COMMAND ----------

print(DF_ACC_CHECK.shape)
DF_ACC_CHECK.head(2)

# COMMAND ----------

# basic stats 
stats_input = DF_ACC_CHECK.copy()
stats_input["YEARWEEK_MIN"] = stats_input["YEARWEEK"]
stats_input["YEARWEEK_MAX"] = stats_input["YEARWEEK"]
stats_input["DAILY_MIN"] = stats_input["DAILY"]
stats_input["DAILY_MAX"] = stats_input["DAILY"]
stats_input["DAILY_AVG"] = stats_input["DAILY"]

stats_input = stats_input.sort_values(["KEY", "YEARWEEK"])

stats_input['lagged_value'] = stats_input.groupby(['KEY'])['DAILY'].shift(1)
stats_input["trend"] = 100 * np.minimum(1, np.maximum(-1,
                        (stats_input["DAILY"] - stats_input["lagged_value"])/stats_input["lagged_value"]))

# COMMAND ----------

stats_df = stats_input.groupby("KEY").agg({
    "YEARWEEK" : "count",
    "YEARWEEK_MIN" : "min",
    "YEARWEEK_MAX" : "max",
    "DAILY_MIN" : "min",
    "DAILY_AVG" : "mean",
    "DAILY_MAX" : "max",
    "trend": "mean"
}).reset_index()

# COMMAND ----------

# last_8months = DF_ACC_CHECK["YEARWEEK"].max() - 35
last_8months = DF_ACC_CHECK["YEARWEEK"][DF_ACC_CHECK["FUTURE"] == "Y"].min()

stats_df_8_month = (
    DF_ACC_CHECK[DF_ACC_CHECK["YEARWEEK"] >= last_8months]
    .groupby("KEY")
    .agg(
        {
            "YEARWEEK": "count",
            "DAILY": ["mean", "sum"],
        }
    )
    .reset_index()
)
stats_df_8_month.columns = ["_".join(col) for col in stats_df_8_month.columns]
stats_df_8_month = stats_df_8_month.rename(
    columns={
        "KEY_": "KEY",
        "YEARWEEK_count": "WEEK_COUNT_LAST_8MONTH",
        "DAILY_mean": "AVG_SALE_LAST_8MONTH",
        "DAILY_sum": "SUM_SALE_LAST_8MONTH",
    }
)
stats_df = stats_df.merge(stats_df_8_month, how="left", on="KEY")
stats_df

# COMMAND ----------

def accuracy_check_full_key(key, df_group, actual_col, predict_col_arr):
    acc_dic = {"KEY": key, "SUM_ACTUAL_SALE": df_group[actual_col].sum()}
    max_accuracy = 0
    max_accuracy_col = "BLANK"
    min_error = 0
    sum_predictsale = 0
    for col in predict_col_arr:
        error = sum((df_group[actual_col] - df_group[col]).abs())
        accuracy = 1 - error / df_group[actual_col].sum()
        acc_dic[f"{col}_ACCURACY"] = accuracy
        acc_dic[f"{col}_ERROR"] = error
        acc_dic[f"{col}_SUM_PREDICT_SALE"] = df_group[col].sum()

        if accuracy > max_accuracy:
            max_accuracy = accuracy
            max_accuracy_col = col
            min_error = error
            sum_predictsale = df_group[col].sum()

    acc_dic["MAX_ACCURACY"] = max_accuracy
    acc_dic["METHOD_FORECAST"] = max_accuracy_col
    acc_dic["ERROR"] = min_error
    acc_dic["SUM_PREDICT_SALE"] = sum_predictsale

    return acc_dic

# accuracy all 
actual_col = "DAILY"
predict_col_arr = [
    "FBPROPHET_DAILY",
    "LINKEDIN_DAILY",
    "FBPROPHET_DAILY_PHASING",
    "LINKEDIN_DAILY_PHASING",
    "TRENDLINE_DAILY_PHASING",
    "LINKEDIN_DAILY_TREND_PHASING",
    "MEDIAN_METHOD",
]

# COMMAND ----------

df_accuracy_method = pd.DataFrame(
    [
        accuracy_check_full_key(key, df_group, actual_col, predict_col_arr)
        for key, df_group in DF_ACC_CHECK[DF_ACC_CHECK["FUTURE"] == 'Y'].groupby("KEY")
    ]
)
stats_df =stats_df.merge(df_accuracy_method, how = "left", on = 'KEY')

# COMMAND ----------

stats_df['METHOD_FORECAST'] = stats_df['METHOD_FORECAST'].fillna('BLANK')
stats_df = stats_df.fillna(0)
stats_df['CATEGORY'] = stats_df['KEY'].str.split('|', expand = True)[1]
cols = stats_df.columns.tolist()
cols = cols[-1:] + cols[:-1]
stats_df = stats_df[cols]

stats_df

# COMMAND ----------

# stats_df.to_excel("/Workspace/Users/ng-minh-hoang.dat@unilever.com/Forecast Primary Sales/Accuracy_Forecast_Baseline_KEY.xlsx")

# COMMAND ----------

def accuracy_check_full_cate(cate, df_group, actual_col, predict_col_arr, stats_df):
    acc_dic = {"CATEGORY": cate, "SUM_ACTUAL_SALE": df_group[actual_col].sum()}
    max_accuracy = 0
    max_accuracy_col = "BLANK"
    min_error = 0
    sum_predictsale = 0
    for col in predict_col_arr:
        error = sum((df_group[actual_col] - df_group[col]).abs())
        accuracy = 1 - error / df_group[actual_col].sum()
        acc_dic[f"{col}_ACCURACY"] = accuracy
        acc_dic[f"{col}_ERROR"] = error
        acc_dic[f"{col}_SUM_PREDICT_SALE"] = df_group[col].sum()

        if accuracy > max_accuracy:
            max_accuracy = accuracy
            max_accuracy_col = col
            min_error = error
            sum_predictsale = df_group[col].sum()
    # Max Accuracy of CATEGORY follow FA formula with whole data
    acc_dic["MAX_ACCURACY_BY_CATEGORY"] = max_accuracy
    acc_dic["METHOD_FORECAST"] = max_accuracy_col
    acc_dic["ERROR_SUM_BY_CATEGORY"] = min_error
    acc_dic["SUM_PREDICT_SALE_BY_CATEGORY"] = sum_predictsale

    # Max Accuracy follow Max Accuracy of each DP with its own optimized method
    # df_key_max_accuracy = pd.DataFrame(
    #     [
    #         accuracy_check_full_key(key, df_group_key, actual_col, predict_col_arr)
    #         for key, df_group_key in df_group.groupby("KEY")
    #     ]
    # )
    df_key_max_accuracy = stats_df[
        stats_df["KEY"].str.split("|", expand=True)[1] == cate
    ]
    df_key_max_accuracy = df_key_max_accuracy[
        ["MAX_ACCURACY", "ERROR", "SUM_PREDICT_SALE", "SUM_SALE_LAST_8MONTH"]
    ]
    median_max_accuracy = df_key_max_accuracy["MAX_ACCURACY"].median()
    
    weighted_mean_max_accuracy = (
        df_key_max_accuracy["SUM_SALE_LAST_8MONTH"]
        * df_key_max_accuracy["MAX_ACCURACY"]
    ).sum() / df_key_max_accuracy["SUM_SALE_LAST_8MONTH"].sum()

    acc_dic["MEDIAN_MAX_ACCURACY_BY_KEY"] = median_max_accuracy
    acc_dic["WEIGHTED_MEAN_MAX_ACCURACY_BY_KEY"] = weighted_mean_max_accuracy
    acc_dic["ERROR_SUM_BY_KEY"] = df_key_max_accuracy["ERROR"].sum()
    acc_dic["SUM_PREDICT_SALE_BY_KEY"] = df_key_max_accuracy["SUM_PREDICT_SALE"].sum()

    return acc_dic


# accuracy all
actual_col = "DAILY"
predict_col_arr = [
    "FBPROPHET_DAILY",
    "LINKEDIN_DAILY",
    "FBPROPHET_DAILY_PHASING",
    "LINKEDIN_DAILY_PHASING",
    "TRENDLINE_DAILY_PHASING",
    "LINKEDIN_DAILY_TREND_PHASING",
    "MEDIAN_METHOD",
]

# COMMAND ----------

df_accuracy_cate = pd.DataFrame(
    [
        accuracy_check_full_cate(cate, df_group, actual_col, predict_col_arr, stats_df)
        for cate, df_group in DF_ACC_CHECK[DF_ACC_CHECK["FUTURE"] == 'Y'].groupby("CATEGORY")
    ]
)
df_accuracy_cate

# COMMAND ----------

# df_accuracy_cate.to_excel("/Workspace/Users/ng-minh-hoang.dat@unilever.com/Forecast Primary Sales/Accuracy_Forecast_Baseline_Category.xlsx")

# COMMAND ----------

df_final = DF_ACC_CHECK.copy()

df_final = df_final.merge(stats_df[['KEY','MAX_ACCURACY','METHOD_FORECAST']], on ='KEY', how ='left')
df_final = df_final.rename(columns = {"MAX_ACCURACY": "MAX_ACCURACY_BY_KEY"})

# COMMAND ----------

predict_col_arr = [
    "FBPROPHET_DAILY",
    "LINKEDIN_DAILY",
    "FBPROPHET_DAILY_PHASING",
    "LINKEDIN_DAILY_PHASING",
    "TRENDLINE_DAILY_PHASING",
    "LINKEDIN_DAILY_TREND_PHASING",
    "MEDIAN_METHOD",
]
df_final["BASELINE_FORECAST"] = 0
for col in predict_col_arr:
    df_final["BASELINE_FORECAST"][df_final["METHOD_FORECAST"] == col] = df_final[col]

df_final["BASELINE_FORECAST"] = df_final["BASELINE_FORECAST"] * df_final["WORKINGDAY"]

df_final = df_final.sort_values(["KEY","YEARWEEK"])

# COMMAND ----------



fig = px.line(
    data_frame=df_final[df_final["DPNAME"] == "OMO GOLD 5500 GR"],
    x="DATE",
    y=["BASELINE","BASELINE_FORECAST"],
)
fig.show()

# COMMAND ----------

df_final

# COMMAND ----------

df_final = df_final[
    [
        "KEY",
        "YEARWEEK",
        "BANNER",
        "REGION",
        "CATEGORY",
        "DPNAME",
        "ACTUALSALE",
        "WORKINGDAY",
        "DATE",
        "YEAR",
        "MONTH",
        "DAILY_AVG_SALES",
        "DAILY_AVG_SALES_ORIGINAL",
        "GREEN",
        "MA",
        "MSTD",
        "BASELINE",
        "BASELINE_DAILY",
        "FBPROPHET_DAILY",
        "LINKEDIN_DAILY",
        "TRENDLINE_DAILY",
        "RATIO_GENERIC",
        "FBPROPHET_DAILY_PHASING",
        "LINKEDIN_DAILY_PHASING",
        "TRENDLINE_DAILY_PHASING",
        "LINKEDIN_DAILY_TREND_PHASING",
        "MEDIAN_METHOD",
        "MAX_ACCURACY_BY_KEY",
        "METHOD_FORECAST",
        "BASELINE_FORECAST",
    ]
]
df_final

# COMMAND ----------

# df_final.to_excel("/Workspace/Users/ng-minh-hoang.dat@unilever.com/Forecast Primary Sales/SNOP_Forecast_Baseline_Primary.xlsx", index= False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Forecast for this year + next year (with Stats-related methods)

# COMMAND ----------

DF_PERIOD_FORECAST_DATA = DF[(DF["YEARWEEK"] <= 202335) & (DF["CATEGORY"] == "FABSOL")].copy()
DF_PERIOD_FORECAST_DATA["FUTURE"] = "N"

df_future = pd.DataFrame({"KEY": DF[DF["CATEGORY"] == "FABSOL"]["KEY"].unique()})
df_future["CHECK"] = 0
df_yearweek_future = pd.DataFrame(
    {"YEARWEEK": [*range(DF_PERIOD_FORECAST_DATA["YEARWEEK"].max() + 1, 202353)]}
)
df_yearweek_future["CHECK"] = 0
df_future = df_future.merge(df_yearweek_future, on="CHECK", how="outer")
df_future["FUTURE"] = "Y"

# COMMAND ----------

df_calendar = pd.read_excel(
    "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/LANDING-MASTER/Master ML Calendar.xlsx",
    sheet_name="BASEWEEK-CALENDAR",
    engine="openpyxl",
)
df_calendar = df_calendar[["YEARWEEK", "DTWORKINGDAY"]]

# COMMAND ----------

DF_PERIOD_FORECAST_DATA = pd.concat([DF_PERIOD_FORECAST_DATA, df_future])
DF_PERIOD_FORECAST_DATA = DF_PERIOD_FORECAST_DATA.fillna(0)

DF_PERIOD_FORECAST_DATA = DF_PERIOD_FORECAST_DATA.merge(df_calendar, on="YEARWEEK")
DF_PERIOD_FORECAST_DATA["WORKINGDAY"] = DF_PERIOD_FORECAST_DATA["DTWORKINGDAY"]

# COMMAND ----------

DF_PERIOD_FORECAST_DATA

# COMMAND ----------

DF_PERIOD_FORECAST_DATA_RESULT, DF_ERROR = baseline_snop_forecast(
    DF_PERIOD_FORECAST_DATA, target_var="BASELINE", exo_vars=[], categorical_vars=[]
)

# COMMAND ----------

actual_col = "DAILY"
predict_col_arr = [
    "FBPROPHET_DAILY",
    "LINKEDIN_DAILY",
    "FBPROPHET_DAILY_PHASING",
    "LINKEDIN_DAILY_PHASING",
    "TRENDLINE_DAILY_PHASING",
    "LINKEDIN_DAILY_TREND_PHASING",
    "MEDIAN_METHOD",
]
df_accuracy_method = pd.DataFrame(
    [
        accuracy_check(key, df_group, actual_col, predict_col_arr)
        for key, df_group in DF_PERIOD_FORECAST_DATA_RESULT.groupby("KEY")
    ]
)

display(df_accuracy_method)

# COMMAND ----------

display(DF_ERROR)

# COMMAND ----------

DF_PERIOD_FORECAST_DATA_RESULT

# COMMAND ----------

# DF_PERIOD_FORECAST_DATA = DF.append(DF_PERIOD_FORECAST_DATA_RESULT)
DF_PERIOD_FORECAST_DATA = DF[DF["CATEGORY"] == "FABSOL"].append(DF_PERIOD_FORECAST_DATA_RESULT[DF_PERIOD_FORECAST_DATA_RESULT["FUTURE"] == 'Y'])

DF_PERIOD_FORECAST_DATA = DF_PERIOD_FORECAST_DATA.fillna(0)

# COMMAND ----------

DF_PERIOD_FORECAST_DATA

# COMMAND ----------

# MERGE TO GET MAXIMUM ACCURACY MODEL 
DF_PERIOD_FORECAST_DATA = DF_PERIOD_FORECAST_DATA.merge(df_accuracy_method[["KEY","MAX_ACCURACY", "COL"]], on="KEY")
DF_PERIOD_FORECAST_DATA["PROPOSED_FC_DAILY"] = 0
for col in df_accuracy_method["COL"].unique():
    if col in DF_PERIOD_FORECAST_DATA.columns:
        DF_PERIOD_FORECAST_DATA["PROPOSED_FC_DAILY"].loc[
            (DF_PERIOD_FORECAST_DATA["COL"] == col)
        ] = DF_PERIOD_FORECAST_DATA[col]
DF_PERIOD_FORECAST_DATA["PROPOSED_FC_WEEKLY"] = (
    DF_PERIOD_FORECAST_DATA["PROPOSED_FC_DAILY"] * DF_PERIOD_FORECAST_DATA["WORKINGDAY"]
)

# COMMAND ----------

DF_PERIOD_FORECAST_DATA["FUTURE"].loc[
    (DF_PERIOD_FORECAST_DATA["YEARWEEK"] <= DF['YEARWEEK'].max())
] = "N"
DF_PERIOD_FORECAST_DATA.drop(
    columns=["DATE", "YEAR", "MONTH", "CHECK", "MAX_ACCURACY"], inplace=True
)
DF_PERIOD_FORECAST_DATA["FUTURE"] = DF_PERIOD_FORECAST_DATA["FUTURE"].astype(str)

# COMMAND ----------

# add stats 
import re
from datetime import timedelta, date

def trend_eda(df, predicted_col):
    df = df.sort_values(["KEY", "YEARWEEK"])
    df['lagged_value'] = df.groupby(['KEY'])[predicted_col].shift(1)
    df["trend"] = 100 * np.minimum(1, np.maximum(-1,
                        (df[predicted_col] - df["lagged_value"])/df["lagged_value"]))
    
    stats_df= df.groupby("KEY").agg({
    predicted_col : "mean",
    "trend": "mean"
    }).reset_index().rename({predicted_col: predicted_col + "MEAN_3M","trend": predicted_col + "TREND_3M"}, axis = 1)

    return stats_df

# basic stats 
stats_input = DF_PERIOD_FORECAST_DATA[DF_PERIOD_FORECAST_DATA["FUTURE"] == 'Y'].copy()
stats_input["DATE"] = pd.to_datetime(
        stats_input["YEARWEEK"].astype(str) + "-4", format="%G%V-%w"
    )
#get 3 month data
next_3months = stats_input["DATE"].min() +  timedelta(days=90)
df_stats_3month = stats_input[stats_input["DATE"] <= next_3months]

# stats PROPOSED_FC_DAILY
df_stats_3month = df_stats_3month.sort_values(["KEY", "YEARWEEK"])
df_stats_3month['lagged_value'] = df_stats_3month.groupby(['KEY'])['PROPOSED_FC_DAILY'].shift(1)
df_stats_3month["trend"] = 100 * np.minimum(1, np.maximum(-1,
                    (df_stats_3month["PROPOSED_FC_DAILY"] - df_stats_3month["lagged_value"])/df_stats_3month["lagged_value"]))
stats_future= df_stats_3month.groupby("KEY").agg({
"PROPOSED_FC_DAILY" : "mean",
"trend": "mean"
}).reset_index().rename({"PROPOSED_FC_DAILY": "PROPOSED_FC_DAILY_MEAN_3M","trend": "PROPOSED_FC_DAILY_TREND_3M"}, axis = 1)

# trend for all model 
predict_col_arr = [
    "FBPROPHET_DAILY",
    "LINKEDIN_DAILY",
    "FBPROPHET_DAILY_PHASING",
    "LINKEDIN_DAILY_PHASING",
    "TRENDLINE_DAILY_PHASING",
    "LINKEDIN_DAILY_TREND_PHASING",
    "MEDIAN_METHOD",
]
for col in predict_col_arr:
    df_trend = trend_eda(df_stats_3month,col)
    stats_future = stats_future.merge(df_trend, on = "KEY", how = "left")

final_stat = stats_df.merge(stats_future, on = 'KEY', how = 'left')

# COMMAND ----------

# DF_PERIOD_FORECAST_DATA.to_parquet(
#     "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/TEMP_DATA/BASELINE_SEC_FORECAST.parquet",
#     index=False,
# )

# COMMAND ----------

# write_excel_file(final_stat,"DF_STATS", "data", OUTPUT_PATH)
# write_excel_file(DF_ERROR,"PREDICT_ERROR", "data", OUTPUT_PATH)

# DF_PERIOD_FORECAST_DATA.to_csv(OUTPUT_PATH + "/PREDICTED_OUTPUT.csv", index=False)

# COMMAND ----------


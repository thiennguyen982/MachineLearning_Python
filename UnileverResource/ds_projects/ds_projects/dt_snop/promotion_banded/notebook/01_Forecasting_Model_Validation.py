# Databricks notebook source
# MAGIC %run "/Repos/lai-trung-minh.duc@unilever.com/SC_DT_Forecast_Project/EnvironmentSetup"

# COMMAND ----------

import pandas as pd
import numpy as np
from scipy.stats import linregress
import warnings
import pickle as pkl

warnings.filterwarnings("ignore")

# COMMAND ----------

import ray
import os
import sys

# COMMAND ----------

# MAGIC %md
# MAGIC # Load Data into the Notebook

# COMMAND ----------

DF = pd.read_parquet(
    "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/TEMP_DATA/BASELINE.parquet"
)
############# DEBUG #########################
# DF = DF.query("CATEGORY == 'FABSOL'")
############# DEBUG #########################

DF_NATIONWIDE = (
    DF.groupby(["CATEGORY", "DPNAME", "YEARWEEK"])[["ACTUALSALE", "BASELINE"]]
    .sum()
    .reset_index()
)
DF_NATIONWIDE["KEY"] = (
    "NATIONWIDE|" + DF_NATIONWIDE["CATEGORY"] + "|" + DF_NATIONWIDE["DPNAME"]
)
# DF = DF.append(DF_NATIONWIDE)
DF = DF_NATIONWIDE  # Only run for NATIONWIDE
DF = DF[["KEY", "YEARWEEK", "ACTUALSALE", "BASELINE"]]

#######################################

df_calendar = pd.read_excel(
    "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/LANDING-MASTER/Master ML Calendar.xlsx",
    sheet_name="BASEWEEK-CALENDAR",
    engine="openpyxl",
)
df_calendar = df_calendar[["YEARWEEK", "DTWORKINGDAY"]]

df_calendar_custom = pd.read_excel(
    "/dbfs/FileStore/tables/CUSTOM_CALENDAR_FEATURE.xlsx"
)
# CALENDAR_FEATURES = list(df_calendar_custom.columns)
# CALENDAR_FEATURES.remove("YEARWEEK")
CALENDAR_FEATURES = [
    "MONTH",
    "YEAR",
    "RANK",
    "WEEK",
    "QUARTER",
    "END_PREVMONTH_DAYCOUNT",
]

# COMMAND ----------

# DF["FUTURE"] = "N"

# DF_FUTURE = pd.DataFrame({"KEY": DF["KEY"].unique()})
# DF_FUTURE["CHECK"] = 0
# DF_YEARWEEK = pd.DataFrame(
#     {"YEARWEEK": [*range(DF["YEARWEEK"].max() + 1, 202353), *range(202401, 202453)]}
# )
# DF_YEARWEEK["CHECK"] = 0
# DF_FUTURE = DF_FUTURE.merge(DF_YEARWEEK, on="CHECK", how="outer")
# DF_FUTURE["FUTURE"] = "Y"

# DF = pd.concat([DF, DF_FUTURE])
# DF = DF.fillna(0)

# df_calendar = pd.read_excel(
#     "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/LANDING-MASTER/Master ML Calendar.xlsx",
#     sheet_name="BASEWEEK-CALENDAR",
# )
# df_calendar = df_calendar[["YEARWEEK", "DTWORKINGDAY"]]
# DF = DF.merge(df_calendar, on="YEARWEEK")
# DF["WORKINGDAY"] = DF["DTWORKINGDAY"]
# DF["DAILY"] = np.where(DF["WORKINGDAY"] == 0, 0, DF["BASELINE"] / DF["WORKINGDAY"])

# COMMAND ----------

# MAGIC %md
# MAGIC # Utils Calculation
# MAGIC - Phasing Ratio
# MAGIC - General Regression Trend

# COMMAND ----------

def calculate_phasing_ratio(key, df_group, target_var):
    warnings.filterwarnings("ignore")

    df_group = df_group[["KEY", "YEARWEEK", "WORKINGDAY", "FUTURE", target_var]]
    df_group["DAILY"] = np.where(
        df_group["WORKINGDAY"] == 0, 0, df_group[target_var] / df_group["WORKINGDAY"]
    )
    df_group = df_group.sort_values("YEARWEEK")
    df_group["DATE"] = pd.to_datetime(
        df_group["YEARWEEK"].astype(str) + "-4", format="%G%V-%w"
    )
    df_group["YEAR"] = df_group["DATE"].dt.isocalendar().year
    df_group["MONTH"] = df_group["DATE"].dt.month
    df_group["WEEK"] = df_group["DATE"].dt.isocalendar().week

    df_group["COUNT WEEKS"] = (
        df_group.groupby(["YEAR", "MONTH"])["WEEK"].transform("count").astype(int)
    )
    df_group["WEEK ORDER"] = (
        df_group.groupby(["YEAR", "MONTH"])["WEEK"].transform("rank").astype(int)
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

    df_group = df_group[["KEY", "YEARWEEK", "WORKINGDAY", "FUTURE", target_var]]
    df_group["DAILY"] = np.where(
        df_group["WORKINGDAY"] == 0, 0, df_group[target_var] / df_group["WORKINGDAY"]
    )
    df_group = df_group.sort_values("YEARWEEK")

    df_history = df_group.query("FUTURE == 'N' ")
    df_group["DATE"] = pd.to_datetime(
        df_group["YEARWEEK"].astype(str) + "-4", format="%G%V-%w"
    )
    df_group["YEAR"] = df_group["DATE"].dt.isocalendar().year
    df_group["MONTH"] = df_group["DATE"].dt.month
    df_group["WEEK"] = df_group["DATE"].dt.isocalendar().week
    df_group["QUARTER"] = df_group["DATE"].dt.quarter
    df_group["QUARTER_GROUP"] = np.where(df_group["QUARTER"].isin([1, 2]), 1, 2)

    df_group["COUNT WEEKS"] = (
        df_group.groupby(["YEAR", "MONTH"])["WEEK"].transform("count").astype(int)
    )
    df_group["WEEK ORDER"] = (
        df_group.groupby(["YEAR", "MONTH"])["WEEK"].transform("rank").astype(int)
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



# COMMAND ----------

# MAGIC %md
# MAGIC # Facebook Prophet method

# COMMAND ----------

from prophet import Prophet


def prophet_cumsum_forecast(key, df_group, target_var, exo_vars, categorical_vars):
    # This is temporary, need to write log to a file, not throw it to NULL area.
    sys.stdout, sys.stderr = open(os.devnull, "w"), open(os.devnull, "w")
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
    f = open(os.devnull, "w")
    sys.stdout = f

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
    sys.stdout, sys.stderr = open(os.devnull, "w"), open(os.devnull, "w")
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
    # print(df_train)
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
            forecast_horizon=52 + 26,  #
            coverage=0.95,  #
            metadata_param=metadata,
        ),
    )

    df_forecast = result.forecast.df.round(2)
    df_forecast["KEY"] = key
    # df_forecast = df_forecast[(df_forecast["DATE"] <= "2025-01-01")]
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
    # print(item)
    print(df_prophet_error)
    print(df_linkedin_error)
    df_error = df_prophet_error.merge(df_linkedin_error, on="KEY", how="outer").fillna(
        "-"
    )

    for output in [fbprophet_output, linkedin_output]:
        for item in output:
            if item["ERROR"] != "NO ERROR":
                {"KEY": item["KEY"], "ERROR": item["ERROR"]}

    df_dataset = df_dataset.merge(df_prophet, on=["KEY", "YEARWEEK"])
    df_dataset = df_dataset.merge(df_linkedin, on=["KEY", "YEARWEEK"])
    df_dataset = df_dataset.merge(df_future_trendline, on=["KEY", "YEARWEEK"])
    df_dataset = df_dataset.merge(df_phasing, on=["KEY", "YEARWEEK"])

    df_dataset["DAILY"] = np.where(
        df_dataset["WORKINGDAY"] == 0,
        0,
        df_dataset[target_var] / df_dataset["WORKINGDAY"],
    )
    df_dataset["DATE"] = pd.to_datetime(
        df_dataset["YEARWEEK"].astype(str) + "-4", format="%G%V-%w"
    )

    df_dataset["YEAR"] = df_dataset["DATE"].dt.year
    df_dataset["MONTH"] = df_dataset["DATE"].dt.month

    df_forecast = df_dataset.query("FUTURE == 'Y' ").fillna(0)
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
# MAGIC # Adhoc

# COMMAND ----------

### Choose the best accuracy methods ###
DF_ACC_CHECK = DF.copy()

###################################
DF_ACC_CHECK = DF_ACC_CHECK[DF_ACC_CHECK["YEARWEEK"] <= 202347]

DF_ACC_CHECK["FUTURE"] = "N"
DF_ACC_CHECK["FUTURE"].loc[
    (DF_ACC_CHECK["YEARWEEK"] >= 202301)
] = "Y"
###################################

DF_ACC_CHECK = DF_ACC_CHECK.merge(df_calendar, on="YEARWEEK")
DF_ACC_CHECK["WORKINGDAY"] = DF_ACC_CHECK["DTWORKINGDAY"]

DF_ACC_CHECK, DF_ERROR = baseline_snop_forecast(
    DF_ACC_CHECK, target_var="BASELINE", exo_vars=[], categorical_vars=[]
)

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
display(DF_ERROR)

# COMMAND ----------

df_accuracy_method = pd.read_csv("/dbfs/mnt/adls/NMHDAT_SNOP/DT/FC_BASELINE_SECONDARY/EVALUATE_2023/ACC_BEST_2022.csv")

DF_ERROR = pd.read_csv("/dbfs/mnt/adls/NMHDAT_SNOP/DT/FC_BASELINE_SECONDARY/EVALUATE_2023/ACC_ERROR_2022.csv")
DF_ERROR.shape, df_accuracy_method.shape

# COMMAND ----------

DF_ACC_CHECK = DF_ACC_CHECK.merge(df_accuracy_method, on = "KEY")
DF_ACC_CHECK["FC_lag4_BL_Sec_daily"] = 0
for col in df_accuracy_method["COL"].unique():
    if col in DF_ACC_CHECK.columns:
        DF_ACC_CHECK["FC_lag4_BL_Sec_daily"].loc[(DF_ACC_CHECK["COL"] == col)] = DF_ACC_CHECK[col]

DF_ACC_CHECK["FC_lag4_BL_Sec_weekly"] = DF_ACC_CHECK["FC_lag4_BL_Sec_daily"] * DF_ACC_CHECK["WORKINGDAY"]

# COMMAND ----------

df_check_missing = pd.pivot_table(DF_ACC_CHECK, columns = "YEARWEEK", index = "KEY", values = "FC_lag4_BL_Sec_weekly")
df_check_missing.shape, df_check_missing.isnull().sum().sum(), df_check_missing.isnull().sum().sum() / df_check_missing.size

# COMMAND ----------

display(df_check_missing.reset_index())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Primary Sale

# COMMAND ----------

df_pri_sales = pd.read_parquet("/dbfs/mnt/adls/SAP_HANA_DATASET/RAW_DATA/PRI_SALES_BANNER_WEEKLY_PARQUET")
df_pri_sales = df_pri_sales.dropna()
df_pri_sales["MATERIAL"] = df_pri_sales["MATERIAL"].astype(int)

df_pri_sales = df_pri_sales[
    df_pri_sales["BANNER"].isin(
        ["DT HCME", "DT MEKONG DELTA", "DT North", "DT CENTRAL"]
    )
]
df_pri_sales = df_pri_sales.groupby(["YEARWEEK", "MATERIAL"])["PCS"].sum().reset_index()
df_pri_sales["REGION"] = "NATIONWIDE"
df_pri_sales["BANNER"] = "NATIONWIDE"

# COMMAND ----------

df_master_product = pd.read_excel(
    "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/LANDING-MASTER/Master Data Total Cat.xlsx",
    sheet_name=None,
)

df_master_dpname = df_master_product["DP Name Master"]
df_master_dpname = df_master_dpname[["DP Name", "DP Name Current"]]
df_master_dpname.columns = ["DPNAME", "DPNAME CURRENT"]
df_master_dpname = df_master_dpname.astype(str)
df_master_dpname["DPNAME"] = df_master_dpname["DPNAME"].str.strip().str.upper()
df_master_dpname["DPNAME CURRENT"] = (
    df_master_dpname["DPNAME CURRENT"].str.strip().str.upper()
)

df_master_product = df_master_product["Code Master"]
df_master_product = df_master_product[
    ["Category", "SAP Code", "DP name", "SKU type", "Pcs/CS", "NW per CS (selling-kg)"]
]
df_master_product.columns = ["CATEGORY", "MATERIAL", "DPNAME" , "SKU_TYPE", "PCS/CS", "KG/CS"]
df_master_product = df_master_product.dropna()
df_master_product["MATERIAL"] = df_master_product["MATERIAL"].astype(int)
df_master_product["KG/PCS"] = df_master_product["KG/CS"] / df_master_product["PCS/CS"]
df_master_product["CATEGORY"] = (
    df_master_product["CATEGORY"].astype(str).str.strip().str.upper()
)
df_master_product["DPNAME"] = (
    df_master_product["DPNAME"].astype(str).str.strip().str.upper()
)
df_master_product["CATEGORY"].loc[
    (df_master_product["DPNAME"].str.contains("BRUSH"))
] = "TBRUSH"

df_master_product = df_master_product.merge(df_master_dpname, on="DPNAME")
df_master_product = df_master_product.drop(columns=["DPNAME"])
df_master_product = df_master_product.rename(columns={"DPNAME CURRENT": "DPNAME"})

print (df_master_product.shape)
df_master_product = df_master_product.drop_duplicates(subset='MATERIAL')
print (df_master_product.shape)

# COMMAND ----------


df_master_product.head(3)

# COMMAND ----------

df_calendar_workingday = pd.read_excel(
    "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/LANDING-MASTER/Master Data Total Cat.xlsx",
    sheet_name="Week Master",
    dtype=object,
)

df_calendar_workingday = df_calendar_workingday[["Week.Year", "CD working day"]]
df_calendar_workingday["Week.Year"] = df_calendar_workingday["Week.Year"].astype(str)
yearweek = df_calendar_workingday["Week.Year"].str.split(".", expand=True)
df_calendar_workingday["YEARWEEK"] = yearweek[1].astype(int) * 100 + yearweek[0].astype(
    int
)
df_calendar_workingday = df_calendar_workingday[["YEARWEEK", "CD working day"]]
df_calendar_workingday.columns = ["YEARWEEK", "DTWORKINGDAY"]
df_calendar_workingday['DTWORKINGDAY'] = df_calendar_workingday['DTWORKINGDAY'].astype(int)

# COMMAND ----------

df_week_master = pd.read_excel(
    "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/LANDING-MASTER/Master Data Total Cat.xlsx",
    sheet_name="GWeek Master",
)
df_week_master = df_week_master[
    ["Weeknum", "Year", "Month Num", "Week Dolphin", "MONTH PHASING"]
]
df_week_master = (
    df_week_master.groupby(by=["Year", "Weeknum"])["MONTH PHASING"]
    .max()
    .to_frame()
    .reset_index()
    .merge(df_week_master, on=["Weeknum", "Year", "MONTH PHASING"], how="inner")
)
df_week_master = df_week_master.drop_duplicates(
    subset=["Year", "Weeknum"], keep="first"
)
df_week_master = df_week_master.drop(["Weeknum", "MONTH PHASING"], axis=1)
df_week_master = df_week_master.rename(
    columns={"Week Dolphin": "YEARWEEK", "Year": "YEAR", "Month Num": "MONTH"}
)

# COMMAND ----------

df_pri_sales = df_pri_sales.merge(df_master_product, on="MATERIAL")

df_pri_sales["TON"] = df_pri_sales["PCS"] * df_pri_sales["KG/PCS"] / 1000
df_pri_sales["CS"] = df_pri_sales["PCS"] / df_pri_sales["PCS/CS"]

# COMMAND ----------

df_pri_sales = (
    df_pri_sales.groupby(["YEARWEEK", "BANNER", "REGION", "CATEGORY", "DPNAME"])[
        ["PCS", "TON", "CS"]
    ]
    .sum()
    .reset_index()
)

df_pri_sales["ACTUALSALE"] = df_pri_sales["TON"]
df_pri_sales["ACTUALSALE"].loc[
    (df_pri_sales["CATEGORY"].isin(["SKINCARE", "IC", "DEO"]))
] = df_pri_sales["CS"]
df_pri_sales["ACTUALSALE"].loc[(df_pri_sales["CATEGORY"].isin(["TBRUSH"]))] = (
    df_pri_sales["PCS"] / 1000
)

# COMMAND ----------

df_pri_sales["DATE"] = pd.to_datetime(df_pri_sales["YEARWEEK"] + "-1", format="%G%V-%w")

df_pri_sales["YEARWEEK"] = df_pri_sales["YEARWEEK"].astype(int)

df_pri_sales = df_pri_sales.merge(df_calendar_workingday, on="YEARWEEK")

df_pri_sales = df_pri_sales.merge(df_week_master, on = "YEARWEEK")

df_pri_sales["QUARTER"] = ((df_pri_sales["MONTH"] - 1) / 3).astype(int) + 1

# COMMAND ----------

df_pri_sales = df_pri_sales.fillna(0)
df_pri_sales = df_pri_sales.sort_values(["DPNAME","YEARWEEK"]).reset_index(drop = True)
print(df_pri_sales.shape)
df_pri_sales.head(3)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Secondary Sale

# COMMAND ----------

df_sec_sales = pd.read_parquet("/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/TEMP_DATA/BASELINE_SEC_FORECAST.parquet")

df_sec_sales[['BANNER', 'CATEGORY', 'DPNAME']] = df_sec_sales['KEY'].str.split('|', expand=True)

df_sec_sales["KEY_NATIONWIDE"] = (
    df_sec_sales["CATEGORY"] + "|" + df_sec_sales["DPNAME"]
)

# COMMAND ----------

df_sec_sales = df_sec_sales.drop(["DTWORKINGDAY", "WORKINGDAY"], axis=1)
df_sec_sales = df_sec_sales.merge(df_calendar_workingday, on="YEARWEEK")
df_sec_sales = df_sec_sales.query("BANNER == 'NATIONWIDE' ")

# COMMAND ----------

df_sec_sales["DATE"] = pd.to_datetime(
    (df_sec_sales["YEARWEEK"]).astype(str) + "-1", format="%G%V-%w"
)

df_sec_sales = df_sec_sales.merge(df_week_master, on="YEARWEEK")

df_sec_sales["QUARTER"] = ((df_sec_sales["MONTH"] - 1) / 3).astype(int) + 1

df_sec_sales = df_sec_sales.drop_duplicates(
    subset=["KEY", "YEARWEEK"], keep="first"
)

# COMMAND ----------

df_sec_sales = df_sec_sales.sort_values(["KEY","YEARWEEK"]).reset_index(drop = True)
print(df_sec_sales.shape)
df_sec_sales.head(3)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Oneshot FC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Convert Sec2Pri Baseline with ratio_phasing Weekly/Month 2 years latest

# COMMAND ----------

def create_ratio_phasing(df_pri_sales, primary_var):
    ratio_WEEK_MONTH_arr = []

    for key, df_group in tqdm(
        df_pri_sales.groupby(["BANNER", "CATEGORY", "DPNAME"])
    ):
        df_group = df_group.sort_values("DATE")
        df_group[primary_var] = df_group[primary_var].fillna(0)

        df_group["WEEK/MONTH RATIO"] = df_group[primary_var] / df_group.groupby(
            ["YEAR", "MONTH"]
        )[primary_var].transform("sum")

        df_group["WEEK/MONTH COUNT"] = df_group.groupby(["YEAR", "MONTH"])[
            "YEARWEEK"
        ].transform("count")

        df_group["WEEK/MONTH ORDER"] = df_group.groupby(["YEAR", "MONTH"])[
            "YEARWEEK"
        ].transform("rank")

        # Contribution of selling day follow phase Week/Month
        df_group["SELLINGDAY_WEEK/MONTH RATIO"] = (
            df_group["WEEK/MONTH RATIO"] / df_group["DTWORKINGDAY"]
        )

        ratio_WEEK_MONTH = (
            df_group.groupby(["WEEK/MONTH COUNT", "WEEK/MONTH ORDER"])
            .agg({"SELLINGDAY_WEEK/MONTH RATIO": ["mean", "median", "std"]})
            .reset_index()
        )

        ratio_WEEK_MONTH.columns = ["_".join(col) for col in ratio_WEEK_MONTH.columns]
        ratio_WEEK_MONTH["KEY"] = "|".join(key)
        ratio_WEEK_MONTH_arr.append(ratio_WEEK_MONTH)

    # ********************************************************
    df_ratio_WEEK_MONTH = pd.concat(ratio_WEEK_MONTH_arr)
    df_ratio_WEEK_MONTH = df_ratio_WEEK_MONTH.query("`WEEK/MONTH COUNT_` >= 4")
    df_ratio_WEEK_MONTH = df_ratio_WEEK_MONTH.dropna(
        subset=["SELLINGDAY_WEEK/MONTH RATIO_median"]
    )
    df_ratio_WEEK_MONTH["KEY"] = df_ratio_WEEK_MONTH["KEY"].str.replace(
        "NATIONWIDE\|NATIONWIDE\|", ""
    )

    df_ratio_WEEK_MONTH["WEEK/MONTH ORDER_"] = df_ratio_WEEK_MONTH[
        "WEEK/MONTH ORDER_"
    ].astype(int)

    df_ratio_WEEK_MONTH.columns = [
        "WEEK/MONTH COUNT",
        "WEEK/MONTH ORDER",
        "SELLINGDAY_WEEK/MONTH RATIO_mean",
        "SELLINGDAY_WEEK/MONTH RATIO_median",
        "SELLINGDAY_WEEK/MONTH RATIO_std",
        "KEY",
    ]

    return df_ratio_WEEK_MONTH

# COMMAND ----------

def convert_data(df_convert, df_ratio_WEEK_MONTH, input_var, output_var):
    df_convert = df_convert.drop(
        "SELLINGDAY_WEEK/MONTH RATIO_median",
        axis=1,
    )

    df_convert = df_convert.merge(
        df_ratio_WEEK_MONTH,
        on=["WEEK/MONTH COUNT", "WEEK/MONTH ORDER", "KEY"],
        how="left",
    )
    df_convert["SELLINGDAY_WEEK/MONTH RATIO_median"] = df_convert[
        "SELLINGDAY_WEEK/MONTH RATIO_median"
    ].replace([-np.inf, np.inf], 0)

    ratio_WEEK_MONTH_category = (
        df_convert.groupby(["CATEGORY", "WEEK/MONTH COUNT", "WEEK/MONTH ORDER"])
        .agg({"SELLINGDAY_WEEK/MONTH RATIO_median": ["median"]})
        .reset_index()
    )

    ratio_WEEK_MONTH_category.columns = [
        "_".join(col) for col in ratio_WEEK_MONTH_category.columns
    ]

    ratio_WEEK_MONTH_category.columns = [
        "CATEGORY",
        "WEEK/MONTH COUNT",
        "WEEK/MONTH ORDER",
        "SELLINGDAY_WEEK/MONTH RATIO_median",
    ]
    df_convert = df_convert.drop(
        [
            "SELLINGDAY_WEEK/MONTH RATIO_mean",
            "SELLINGDAY_WEEK/MONTH RATIO_median",
            "SELLINGDAY_WEEK/MONTH RATIO_std",
        ],
        axis=1,
    )
    # replace ratio follow dpname by ratio follow category
    df_convert = df_convert.merge(
        ratio_WEEK_MONTH_category,
        on=["CATEGORY", "WEEK/MONTH COUNT", "WEEK/MONTH ORDER"],
        how="left",
    )

    df_convert["FC_PRI_BASELINE"] = (
        df_convert.groupby(["KEY", "YEAR", "MONTH"])[input_var].transform("sum")
        * df_convert["SELLINGDAY_WEEK/MONTH RATIO_median"]
    )

    df_convert[output_var] = df_convert["FC_PRI_BASELINE"] * df_convert["DTWORKINGDAY"]

    return df_convert

# COMMAND ----------

DF_ACC_CHECK[["BANNER","CATEGORY","DPNAME"]] = DF_ACC_CHECK["KEY"].str.split("|", expand = True)
DF_ACC_CHECK.head(2)

# COMMAND ----------

df_convert = DF_ACC_CHECK[["YEARWEEK","ACTUALSALE","BASELINE","CATEGORY","DPNAME","FC_lag4_BL_Sec_weekly"]].copy()
df_convert = df_convert.rename(
    columns={"ACTUALSALE": "SEC_SALES", "BASELINE": "SEC_BASELINE"}
)

df_convert = df_convert.merge(
    df_pri_sales[["CATEGORY", "DPNAME", "YEARWEEK", "ACTUALSALE"]],
    how="outer",
    on=["CATEGORY", "DPNAME", "YEARWEEK"],
)
df_convert = df_convert.rename(columns={"ACTUALSALE": "PRI_SALES"})

df_convert = df_convert[
    (df_convert["YEARWEEK"] <= 202347)
    & (df_convert["YEARWEEK"] >= 202101)
]
df_convert = df_convert.drop_duplicates()
print(df_convert.shape)
df_convert.head(2)

# COMMAND ----------

df_convert["BANNER"] = "NATIONWIDE"
df_convert["KEY"] = "NATIONWIDE|" + df_convert["CATEGORY"] + "|" + df_convert["DPNAME"]

df_convert = df_convert.merge(df_calendar_workingday, on="YEARWEEK")

df_convert["DATE"] = pd.to_datetime(
    (df_convert["YEARWEEK"]).astype(str) + "-1", format="%G%V-%w"
)

df_convert = df_convert.merge(df_week_master, on="YEARWEEK")

df_convert["QUARTER"] = ((df_convert["MONTH"] - 1) / 3).astype(int) + 1

df_convert = df_convert.drop_duplicates(
    subset=["KEY", "YEARWEEK"], keep="first"
)

df_convert["WEEK/MONTH COUNT"] = (
    df_convert.groupby(["KEY", "YEAR", "MONTH"])["YEARWEEK"]
    .transform("count")
    .astype(int)
)
df_convert["WEEK/MONTH ORDER"] = (
    df_convert.groupby(["KEY", "YEAR", "MONTH"])["YEARWEEK"]
    .transform("rank")
    .astype(int)
)
df_convert = df_convert.sort_values(["KEY", "YEARWEEK"]).reset_index(
    drop=True
)

df_convert["FC_PRI_BASELINE"] = 0
df_convert["FC_PRI_BASELINE_WEEKLY"] = 0
df_convert["SELLINGDAY_WEEK/MONTH RATIO_median"] = 0
df_convert.head(2)

# COMMAND ----------

from tqdm import tqdm

for month_idx in range(1,12):
    year_idx = 2023

    phasing_lower = df_convert["YEARWEEK"][
        (df_convert["YEAR"] >= (year_idx - 2)) & (df_convert["MONTH"] == month_idx)
    ].min()

    if month_idx == 1:
        phasing_upper = df_convert["YEARWEEK"][
            df_convert["YEAR"] <= (year_idx - 1)
        ].max()
    else:
        phasing_upper = df_convert["YEARWEEK"][
            (df_convert["YEAR"] <= year_idx) & (df_convert["MONTH"] == (month_idx - 1))
        ].max()
        
    df_ratio_phasing = df_convert[
        (df_convert["YEARWEEK"] >= phasing_lower) & (df_convert["YEARWEEK"] <= phasing_upper)
    ]

    df_ratio_WEEK_MONTH = create_ratio_phasing(df_ratio_phasing, "PRI_SALES")

    df_convert_pattern = convert_data(
        df_convert[
            (df_convert["YEAR"] == year_idx) & (df_convert["MONTH"] == month_idx)
        ],
        df_ratio_WEEK_MONTH,
        input_var = "FC_lag4_BL_Sec_weekly",
        output_var = "FC_PRI_BASELINE_WEEKLY"
    )

    df_convert_pattern = df_convert_pattern.reset_index(drop=True)  
    indices = df_convert[(df_convert["YEAR"] == year_idx) & (df_convert["MONTH"] == month_idx)].index 
    positions = df_convert.index.get_indexer(indices) 
    df_convert.iloc[positions] = df_convert_pattern

# COMMAND ----------

df_convert = df_convert.rename(columns = {"SELLINGDAY_WEEK/MONTH RATIO_median": "RATIO_median"})
df_convert = df_convert.sort_values(["KEY","YEARWEEK"]).reset_index(drop = True)
df_convert

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Evaluate Baseline 

# COMMAND ----------

def accuracy_check(key, df_group, actual_col, predict_col_arr, Ratio=1):
    df_group[actual_col] = df_group[actual_col].fillna(0)

    performance = dict()
    sum_actualsale = df_group[actual_col].sum()

    performance = {"CATEGORY": key, "Sum_actualsale": sum_actualsale}

    for predict_col in predict_col_arr:
        df_group[predict_col] = df_group[predict_col].fillna(0)
        df_group[predict_col] = df_group[predict_col].replace([-np.inf, np.inf], 0)
        df_group[predict_col] = df_group[predict_col] * Ratio

        error = sum((df_group[actual_col] - df_group[predict_col]).abs())
        accuracy = 1 - error / df_group[actual_col].sum()
        sum_predictsale = df_group[predict_col].sum()

        performance["Sum_predictsale_" + predict_col] = sum_predictsale
        performance["Accuracy_" + predict_col] = accuracy
        performance["Error_" + predict_col] = error

    return performance

# COMMAND ----------

actual_col = "PRI_SALES"
predict_col_arr = [
    "FC_PRI_BASELINE_WEEKLY"
]
df_accuracy_baseline_monthly2023 = pd.DataFrame(columns=["CATEGORY"])

for month_idx in range(1, 12):
    df_convert_pattern = df_convert[
        (df_convert["YEAR"] == 2023)
        & (df_convert["MONTH"] == month_idx)
    ]
    df_accuracy_phase_2Y = pd.DataFrame.from_dict(
        [
            accuracy_check(key, df_group, actual_col, predict_col_arr)
            for key, df_group in  df_convert_pattern.groupby("CATEGORY")
        ]
    )

    for col in df_accuracy_phase_2Y.columns:
        if col != "CATEGORY":
            df_accuracy_phase_2Y = df_accuracy_phase_2Y.rename(columns = {
                col: col + "_month_" + str(month_idx)
            })

    df_accuracy_baseline_monthly2023 = df_accuracy_baseline_monthly2023.merge(
        df_accuracy_phase_2Y, on=["CATEGORY"], how="outer"
    )

# COMMAND ----------

df_accuracy_baseline_monthly2023 = df_accuracy_baseline_monthly2023.fillna(0)
df_accuracy_baseline_monthly2023 = df_accuracy_baseline_monthly2023.replace([-np.inf, np.inf], 0)

df_accuracy_baseline_monthly2023

# COMMAND ----------

accuracy_check("total", df_convert[df_convert["YEARWEEK"].between(202301, 202347)], "PRI_SALES", ["FC_PRI_BASELINE_WEEKLY"])

# COMMAND ----------

accuracy_check("total", df_convert[df_convert["YEARWEEK"].between(202301, 202347)], "SEC_SALES", ["FC_lag4_BL_Sec_weekly"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Lag4 FC

# COMMAND ----------

from tqdm import tqdm
df_evaluate = pd.DataFrame()

for yearweek_cutoff in tqdm([*range(202301, 202348)]):
    if (yearweek_cutoff - 4 < 202301):
        time_training = yearweek_cutoff - 52
    else:
        time_training = yearweek_cutoff - 4

    df_cutoff = pd.read_csv(f"/dbfs/mnt/adls/NMHDAT_SNOP/DT/FC_BASELINE_SECONDARY/EVALUATE_2023/TRAINING_TO_{time_training}.csv")
    #lag4
    df_cutoff = df_cutoff[df_cutoff["YEARWEEK"] == yearweek_cutoff]

    df_evaluate = pd.concat([df_evaluate, df_cutoff])

df_evaluate = df_evaluate.sort_values(["KEY","YEARWEEK"])

# COMMAND ----------

df_accuracy_method = pd.read_csv("/dbfs/mnt/adls/NMHDAT_SNOP/DT/FC_BASELINE_SECONDARY/EVALUATE_2023/ACC_BEST_2022.csv")

DF_ERROR = pd.read_csv("/dbfs/mnt/adls/NMHDAT_SNOP/DT/FC_BASELINE_SECONDARY/EVALUATE_2023/ACC_ERROR_2022.csv")
DF_ERROR.shape, df_accuracy_method.shape

# COMMAND ----------

df_evaluate[["BANNER","CATEGORY","DPNAME"]] = df_evaluate["KEY"].str.split("|", expand = True)

# COMMAND ----------

df_evaluate = df_evaluate.merge(df_accuracy_method, on = "KEY")
df_evaluate["FC_lag4_BL_Sec_daily"] = 0
for col in df_accuracy_method["COL"].unique():
    if col in df_evaluate.columns:
        df_evaluate["FC_lag4_BL_Sec_daily"].loc[(df_evaluate["COL"] == col)] = df_evaluate[col]

df_evaluate["FC_lag4_BL_Sec_weekly"] = df_evaluate["FC_lag4_BL_Sec_daily"] * df_evaluate["WORKINGDAY"]

# COMMAND ----------

df_convert_lag4 = df_sec_sales[["YEARWEEK","ACTUALSALE","BASELINE","CATEGORY","DPNAME"]].copy()
df_convert_lag4 = df_convert_lag4.rename(
    columns={"ACTUALSALE": "SEC_SALES", "BASELINE": "SEC_BASELINE"}
)

df_convert_lag4 = df_convert_lag4.merge(
    df_pri_sales[["CATEGORY", "DPNAME", "YEARWEEK", "ACTUALSALE"]],
    how="outer",
    on=["CATEGORY", "DPNAME", "YEARWEEK"],
)
df_convert_lag4 = df_convert_lag4.rename(columns={"ACTUALSALE": "PRI_SALES"})

df_convert_lag4 = df_convert_lag4[
    (df_convert_lag4["YEARWEEK"] <= df_evaluate["YEARWEEK"].max())
    & (df_convert_lag4["YEARWEEK"] >= 202101)
]

df_convert_lag4 = df_convert_lag4.merge(
    df_evaluate[["CATEGORY","DPNAME","YEARWEEK","FC_lag4_BL_Sec_weekly"]], on=["CATEGORY", "DPNAME", "YEARWEEK"], 
    how="outer",
)
print(df_convert_lag4.shape)

df_convert_lag4 = df_convert_lag4[(df_convert_lag4["YEARWEEK"] < 202301) | (df_convert_lag4["FC_lag4_BL_Sec_weekly"].notnull())]
print(df_convert_lag4.shape)
df_convert_lag4.head(2)

# COMMAND ----------

df_convert_lag4["BANNER"] = "NATIONWIDE"
df_convert_lag4["KEY"] = "NATIONWIDE|" + df_convert_lag4["CATEGORY"] + "|" + df_convert_lag4["DPNAME"]

df_convert_lag4 = df_convert_lag4.merge(df_calendar_workingday, on="YEARWEEK")

df_convert_lag4["DATE"] = pd.to_datetime(
    (df_convert_lag4["YEARWEEK"]).astype(str) + "-1", format="%G%V-%w"
)

df_convert_lag4 = df_convert_lag4.merge(df_week_master, on="YEARWEEK")

df_convert_lag4["QUARTER"] = ((df_convert_lag4["MONTH"] - 1) / 3).astype(int) + 1

df_convert_lag4 = df_convert_lag4.drop_duplicates(
    subset=["KEY", "YEARWEEK"], keep="first"
)

df_convert_lag4["WEEK/MONTH COUNT"] = (
    df_convert_lag4.groupby(["KEY", "YEAR", "MONTH"])["YEARWEEK"]
    .transform("count")
    .astype(int)
)
df_convert_lag4["WEEK/MONTH ORDER"] = (
    df_convert_lag4.groupby(["KEY", "YEAR", "MONTH"])["YEARWEEK"]
    .transform("rank")
    .astype(int)
)
df_convert_lag4 = df_convert_lag4.sort_values(["KEY", "YEARWEEK"]).reset_index(
    drop=True
)

df_convert_lag4["FC_PRI_BASELINE"] = 0
df_convert_lag4["FC_PRI_BASELINE_WEEKLY"] = 0
df_convert_lag4["SELLINGDAY_WEEK/MONTH RATIO_median"] = 0
df_convert_lag4.head(2)

# COMMAND ----------

df_convert_lag4[df_convert_lag4["YEARWEEK"] >= 202301].describe(include = "all")

# COMMAND ----------

for month_idx in range(1,12):
    year_idx = 2023
    
    phasing_lower = df_convert_lag4["YEARWEEK"][
        (df_convert_lag4["YEAR"] >= (year_idx - 2)) & (df_convert_lag4["MONTH"] == month_idx)
    ].min()
    # phasing_lower = 202101

    if month_idx == 1:
        phasing_upper = df_convert_lag4["YEARWEEK"][
            df_convert_lag4["YEAR"] <= (year_idx - 1)
        ].max()
    else:
        phasing_upper = df_convert_lag4["YEARWEEK"][
            (df_convert_lag4["YEAR"] <= year_idx) & (df_convert_lag4["MONTH"] == (month_idx - 1))
        ].max()
        
    df_ratio_phasing = df_convert_lag4[
        (df_convert_lag4["YEARWEEK"] >= phasing_lower) & (df_convert_lag4["YEARWEEK"] <= phasing_upper)
    ]

    df_ratio_WEEK_MONTH = create_ratio_phasing(df_ratio_phasing, "PRI_SALES")

    df_convert_pattern = convert_data(
        df_convert_lag4[
            (df_convert_lag4["YEAR"] == year_idx) & (df_convert_lag4["MONTH"] == month_idx)
        ],
        df_ratio_WEEK_MONTH,
        input_var = "FC_lag4_BL_Sec_weekly",
        output_var = "FC_PRI_BASELINE_WEEKLY"
    )
    df_convert_pattern = df_convert_pattern.reset_index(drop=True)  
    indices = df_convert_lag4[(df_convert_lag4["YEAR"] == year_idx) & (df_convert_lag4["MONTH"] == month_idx)].index 
    positions = df_convert_lag4.index.get_indexer(indices) 
    df_convert_lag4.iloc[positions] = df_convert_pattern

# COMMAND ----------

def accuracy_check(key, df_group, actual_col, predict_col_arr, Ratio=1):
    df_group[actual_col] = df_group[actual_col].fillna(0)

    performance = dict()
    sum_actualsale = df_group[actual_col].sum()

    performance = {"CATEGORY": key, "Sum_actualsale": sum_actualsale}

    for predict_col in predict_col_arr:
        df_group[predict_col] = df_group[predict_col].fillna(0)
        df_group[predict_col] = df_group[predict_col].replace([-np.inf, np.inf], 0)
        df_group[predict_col] = df_group[predict_col] * Ratio

        error = sum((df_group[actual_col] - df_group[predict_col]).abs())
        accuracy = 1 - error / df_group[actual_col].sum()
        sum_predictsale = df_group[predict_col].sum()

        performance["Sum_predictsale_" + predict_col] = sum_predictsale
        performance["Accuracy_" + predict_col] = accuracy
        performance["Error_" + predict_col] = error

    return performance

# COMMAND ----------

accuracy_check("Baseline", df_convert_lag4[df_convert_lag4["YEARWEEK"].between(202301, 202347)], "PRI_SALES", ["FC_PRI_BASELINE_WEEKLY"])

# COMMAND ----------

df_export = df_convert_lag4[df_convert_lag4["YEARWEEK"] >= 202301]
df_export["BANNER"] == "NATIONWIDE"
df_export = df_export[["YEARWEEK","BANNER","CATEGORY","DPNAME","DATE","YEAR","MONTH","WEEK/MONTH COUNT","WEEK/MONTH ORDER","SEC_SALES","PRI_SALES","FC_lag4_BL_Sec_weekly","FC_PRI_BASELINE_WEEKLY","SELLINGDAY_WEEK/MONTH RATIO_median"]]

df_export.columns = ["YEARWEEK","BANNER","CATEGORY","DPNAME","DATE","YEAR","MONTH","WEEK_COUNT","WEEK_ORDER","ACTUAL_SEC_SALES","ACTUAL_PRI_SALES","FC_Lag4_BL_SEC","FC_Lag4_BL_PRIM","RATIO_median"]
df_export.shape

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Compare accuracy weekly

# COMMAND ----------

def accuracy_check(key, df_group, actual_col, predict_col_arr, Ratio=1):
    df_group[actual_col] = df_group[actual_col].fillna(0)

    performance = dict()
    sum_actualsale = df_group[actual_col].sum()

    performance = {"Total": key,}

    for predict_col in predict_col_arr:
        df_group[predict_col] = df_group[predict_col].fillna(0)
        df_group[predict_col] = df_group[predict_col].replace([-np.inf, np.inf], 0)
        df_group[predict_col] = df_group[predict_col] * Ratio

        error = sum((df_group[actual_col] - df_group[predict_col]).abs())
        accuracy = 1 - error / df_group[actual_col].sum()
        performance["Accuracy"] = accuracy

    return performance

# COMMAND ----------

actual_col = "PRI_SALES"
predict_col_arr = [
    "FC_PRI_BASELINE_WEEKLY"
]
df_acc_oneshot_pri = pd.DataFrame(columns=["Total"])

for yw in range(202301, 202348):
    df_convert_pattern = df_convert[df_convert["YEARWEEK"] == yw]
    df_acc = pd.DataFrame.from_dict(
        [
            accuracy_check("Total", df_convert_pattern, actual_col, predict_col_arr)
        ]
    )
    df_acc = df_acc.rename(columns = {"Accuracy":"Accuracy_" + str(yw)})
    df_acc_oneshot_pri = df_acc_oneshot_pri.merge(
        df_acc, on=["Total"], how="outer"
    )

# COMMAND ----------

df_acc_oneshot_pri

# COMMAND ----------

actual_col = "ACTUAL_PRI_SALES"
predict_col_arr = [
    "FC_Lag4_BL_PRIM"
]
df_acc_lag4_pri = pd.DataFrame(columns=["Total"])

for yw in range(202301, 202348):
    df_convert_pattern = df_export[df_export["YEARWEEK"] == yw]
    df_acc = pd.DataFrame.from_dict(
        [
            accuracy_check("Total", df_convert_pattern, actual_col, predict_col_arr)
        ]
    )
    df_acc = df_acc.rename(columns = {"Accuracy":"Accuracy_" + str(yw)})
    df_acc_lag4_pri = df_acc_lag4_pri.merge(
        df_acc, on=["Total"], how="outer"
    )

# COMMAND ----------

df_acc_lag4_pri

# COMMAND ----------

df_convert.head(1)

# COMMAND ----------

actual_col = "SEC_SALES"
predict_col_arr = [
    "FC_lag4_BL_Sec_weekly"
]
df_acc_oneshot_sec = pd.DataFrame(columns=["Total"])

for yw in range(202301, 202348):
    df_convert_pattern = df_convert[df_convert["YEARWEEK"] == yw]
    df_acc = pd.DataFrame.from_dict(
        [
            accuracy_check("Total", df_convert_pattern, actual_col, predict_col_arr)
        ]
    )
    df_acc = df_acc.rename(columns = {"Accuracy":"Accuracy_" + str(yw)})
    df_acc_oneshot_sec = df_acc_oneshot_sec.merge(
        df_acc, on=["Total"], how="outer"
    )

# COMMAND ----------

df_acc_oneshot_sec

# COMMAND ----------

actual_col = "ACTUAL_SEC_SALES"
predict_col_arr = [
    "FC_Lag4_BL_SEC"
]
df_acc_lag4_sec = pd.DataFrame(columns=["Total"])

for yw in range(202301, 202348):
    df_convert_pattern = df_export[df_export["YEARWEEK"] == yw]
    df_acc = pd.DataFrame.from_dict(
        [
            accuracy_check("Total", df_convert_pattern, actual_col, predict_col_arr)
        ]
    )
    df_acc = df_acc.rename(columns = {"Accuracy":"Accuracy_" + str(yw)})
    df_acc_lag4_sec = df_acc_lag4_sec.merge(
        df_acc, on=["Total"], how="outer"
    )

# COMMAND ----------

temp = pd.concat([df_acc_oneshot_pri, df_acc_oneshot_sec, df_acc_lag4_pri, df_acc_lag4_sec])
temp["Total"] = ["one shot pri","one shot sec","lag 4 pri","lag 4 sec"]
temp

# COMMAND ----------

temp = pd.melt(temp, id_vars="Total", value_vars=temp.drop("Total", axis = 1).columns)
temp["YEARWEEK"] = temp["variable"].str.split("_", expand = True)[1]
temp

# COMMAND ----------

import plotly.graph_objects as go
fig = go.Figure(data = [
    go.Scatter(x = temp[temp["Total"] == "one shot pri"]["YEARWEEK"], y = temp[temp["Total"] == "one shot pri"]["value"], name = "One shot primary"),
    go.Scatter(x = temp[temp["Total"] == "one shot sec"]["YEARWEEK"], y = temp[temp["Total"] == "one shot sec"]["value"], name = "one shot secondary"),
    go.Scatter(x = temp[temp["Total"] == "lag 4 pri"]["YEARWEEK"], y = temp[temp["Total"] == "lag 4 pri"]["value"], name = "lag 4 primary"),
    go.Scatter(x = temp[temp["Total"] == "lag 4 sec"]["YEARWEEK"], y = temp[temp["Total"] == "lag 4 sec"]["value"], name = "lag 4 secondary"),
])
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # main 

# COMMAND ----------

df_for_validation = DF.copy()
df_for_validation["FUTURE"] = "N"
df_for_validation = df_for_validation.merge(df_calendar, on="YEARWEEK")
df_for_validation["WORKINGDAY"] = df_for_validation["DTWORKINGDAY"]

# COMMAND ----------

df_for_validation.describe()

# COMMAND ----------

from tqdm import tqdm
import traceback
yearweek_max = df_for_validation["YEARWEEK"].max()

def baseline_validation_forecast_lag(df_dataset):
    output_data = {}
    for yearweek_cutoff in tqdm([*range(202249, 202253), *range(202301, yearweek_max)]):
        try:
            df_cutoff = df_dataset.copy()
            # if yearweek_cutoff <= 202252:
            #     df_cutoff["FUTURE"].loc[
            #         (df_cutoff["YERWEEK"] > yearweek_cutoff)
            #         & (df_cutoff["YEARWEEK"] <= (yearweek_cutoff + 52))
            #     ] = "Y"
            # else:
            #     df_cutoff["FUTURE"].loc[
            #         (df_cutoff["YEARWEEK"] > yearweek_cutoff)
            #         & (df_cutoff["YEARWEEK"] <= yearweek_cutoff + 4)
            #     ] = "Y"
            # df_cutoff = df_cutoff.loc[
            #     (df_cutoff["FUTURE"] == "Y")
            #     | (df_cutoff["YEARWEEK"] <= yearweek_cutoff)
            # ]

            df_cutoff["FUTURE"].loc[(df_cutoff["YEARWEEK"] > yearweek_cutoff)] = "Y"
            print(yearweek_cutoff, df_cutoff.shape[0])

            df_forecast, df_error = baseline_snop_forecast(
                df_dataset=df_cutoff,
                target_var="BASELINE",
                exo_vars=[],
                categorical_vars=[],
            )

            df_forecast.to_csv(
                f"/dbfs/mnt/adls/NMHDAT_SNOP/DT/FC_BASELINE_SECONDARY/EVALUATE_2023/TRAINING_TO_{yearweek_cutoff}.csv",
                index=False,
            )

            # df_forecast['LAG_FC'] = df_forecast.groupby('KEY')['YEARWEEK'].rank()
            output_data[str(yearweek_cutoff)] = df_forecast
        except Exception as ex:
            print(traceback.format_exc())
            print(str(ex))
            pass
    return output_data

# COMMAND ----------

output_validation_dict = baseline_validation_forecast_lag(df_for_validation)

# COMMAND ----------

output_validation_dict

# COMMAND ----------

temp = output_validation_dict['202304']
temp.describe()

# COMMAND ----------

display(temp.sort_values(["KEY","YEARWEEK"]))

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
        for key, df_group in temp[temp["YEARWEEK"] == 202308].groupby("KEY")
    ]
)

# COMMAND ----------

display(df_accuracy_method.sort_values("KEY"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Find best accurate methods in the last 52 weeks (2022).

# COMMAND ----------

def accuracy_check(key, df_group, actual_col, predict_col_arr):
    max_accuracy = 0
    max_accuracy_col = "BLANK"
    for col in predict_col_arr:
        error = sum((df_group[actual_col] - df_group[col]).abs())
        accuracy = 1 - error / df_group[actual_col].sum()
        if accuracy > max_accuracy:
            max_accuracy = accuracy
            max_accuracy_col = col
    return {"KEY": key, "MAX_ACCURACY": max_accuracy, "COL": max_accuracy_col}

# COMMAND ----------

### Choose the best accuracy methods ###
DF_ACC_CHECK = DF.copy()
###################################
DF_ACC_CHECK = DF_ACC_CHECK[DF_ACC_CHECK["YEARWEEK"] <= 202252]
DF_ACC_CHECK["DPNAME"] = DF_ACC_CHECK["KEY"].str.split("|", expand=True)[2]
DF_ACC_CHECK = DF_ACC_CHECK[
    DF_ACC_CHECK["DPNAME"].isin(
        [
            # "AXE DEO BODYSPRAY DARK TMPTTN 12X150ML",
            # "CLEAR SHAMPOO MEN DEEP CLEANSE 900G",
            # "CLOSE-UP PASTE ARCTIC SHOCK 180 GR",
            # "COMFORT ELEGANT POUCH 3.6KG",
            # "DOVE BOTANICAL SELECTION ARGAN OIL&AVOCADO SH 500G",
            # "DOVE BOTANICAL SELECTION LOTUS&JOJOBA COND 500G",
            # "DOVE CONDITIONER ANTI HAIRFALL 320G",
            # "DOVE ORIGINAL R/O 40ML/24",
            # "DOVE SHAMPOO ANTI HAIRFALL 325G",
            # "DOVE SHAMPOO ANTI HAIRFALL 640G",
            # "DOVE THICKENING RITUAL HAIRCON 335G",
            # "MT TOPTEN5X(VAN3X57G+CHO2X57G)",
            "OMO LIQUID MATIC FLL BEAUTY CARE (POU) 2.9KG",
            "OMO LIQUID MATIC FLL BEAUTY CARE (POU) 3.7KG",
            "OMO LIQUID MATIC TLL CFT SS (POU) 2.9KG",
            "OMO LIQUID MATIC TLL CORE (POU) 3.1KG",
            # "P/S ANTI CAVITY 500ML",
            # "P/S BRUSH DOUBLE CARE SENSITIVE - MULTI PACK",
            # "P/S PASTE ALFRED 230 GR",
            # "P/S TP ROSEMARY 230G",
            # "PADDLE POP MELON 55G",
            # "REXONA FREE SPIRIT R/O 40ML - CHAPARAL",
            # "SUNLIGHT DW SAKURA 3600G",
            # "SUNLIGHT DW SAKURA 750G",
            # "SUNLIGHT FLOORCARE PINK POUCH 1000G",
            # "VASELINE HW FRESH&FR UV LOT VN 200ML",
        ]
    )
]
DF_ACC_CHECK.drop("DPNAME", axis=1, inplace=True)
DF_ACC_CHECK["FUTURE"] = "N"
DF_ACC_CHECK["FUTURE"].loc[(DF_ACC_CHECK["YEARWEEK"] >= 202201)] = "Y"
###################################

DF_ACC_CHECK = DF_ACC_CHECK.merge(df_calendar, on="YEARWEEK")
DF_ACC_CHECK["WORKINGDAY"] = DF_ACC_CHECK["DTWORKINGDAY"]
DF_ACC_CHECK.describe(include="all")

# COMMAND ----------


DF_ACC_CHECK, DF_ERROR = baseline_snop_forecast(
    DF_ACC_CHECK, target_var="BASELINE", exo_vars=[], categorical_vars=[]
)

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
display(DF_ERROR)

# COMMAND ----------

# df_accuracy_method.to_csv("/dbfs/mnt/adls/NMHDAT_SNOP/DT/FC_BASELINE_SECONDARY/EVALUATE_2023/ACC_BEST_2022.csv",index = False)

# DF_ERROR.to_csv("/dbfs/mnt/adls/NMHDAT_SNOP/DT/FC_BASELINE_SECONDARY/EVALUATE_2023/ACC_ERROR_2022.csv",index = False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Forecast for this year + next year (with Stats-related methods)

# COMMAND ----------

# DF_PERIOD_FORECAST_DATA = DF.copy()
# DF_PERIOD_FORECAST_DATA["FUTURE"] = "N"

# df_future = pd.DataFrame({"KEY": DF["KEY"].unique()})
# df_future["CHECK"] = 0
# df_yearweek_future = pd.DataFrame(
#     {"YEARWEEK": [*range(DF["YEARWEEK"].max() + 1, 202353), *range(202401, 202453)]}
# )
# df_yearweek_future["CHECK"] = 0
# df_future = df_future.merge(df_yearweek_future, on="CHECK", how="outer")
# df_future["FUTURE"] = "Y"

# DF_PERIOD_FORECAST_DATA = pd.concat([DF_PERIOD_FORECAST_DATA, df_future])
# DF_PERIOD_FORECAST_DATA = DF_PERIOD_FORECAST_DATA.fillna(0)

# DF_PERIOD_FORECAST_DATA = DF_PERIOD_FORECAST_DATA.merge(df_calendar, on="YEARWEEK")
# DF_PERIOD_FORECAST_DATA["WORKINGDAY"] = DF_PERIOD_FORECAST_DATA["DTWORKINGDAY"]

# COMMAND ----------

# DF_PERIOD_FORECAST_DATA_RESULT, DF_ERROR = baseline_snop_forecast(
#     DF_PERIOD_FORECAST_DATA, target_var="BASELINE", exo_vars=[], categorical_vars=[]
# )

# DF_PERIOD_FORECAST_DATA = DF.append(DF_PERIOD_FORECAST_DATA_RESULT)
# DF_PERIOD_FORECAST_DATA = DF_PERIOD_FORECAST_DATA.fillna(0)

# display(DF_ERROR)

# COMMAND ----------

# DF_PERIOD_FORECAST_DATA = DF_PERIOD_FORECAST_DATA.merge(df_accuracy_method, on="KEY")
# DF_PERIOD_FORECAST_DATA["PROPOSED_FC_DAILY"] = 0
# for col in df_accuracy_method["COL"].unique():
#     if col in DF_PERIOD_FORECAST_DATA.columns:
#         DF_PERIOD_FORECAST_DATA["PROPOSED_FC_DAILY"].loc[
#             (DF_PERIOD_FORECAST_DATA["COL"] == col)
#         ] = DF_PERIOD_FORECAST_DATA[col]
# DF_PERIOD_FORECAST_DATA["PROPOSED_FC_WEEKLY"] = (
#     DF_PERIOD_FORECAST_DATA["PROPOSED_FC_DAILY"] * DF_PERIOD_FORECAST_DATA["WORKINGDAY"]
# )

# COMMAND ----------

# DF_PERIOD_FORECAST_DATA["FUTURE"].loc[
#     (DF_PERIOD_FORECAST_DATA["YEARWEEK"] <= DF['YEARWEEK'].max())
# ] = "N"
# DF_PERIOD_FORECAST_DATA.drop(
#     columns=["DATE", "YEAR", "MONTH", "CHECK", "MAX_ACCURACY"], inplace=True
# )

# COMMAND ----------

# DF_PERIOD_FORECAST_DATA["FUTURE"] = DF_PERIOD_FORECAST_DATA["FUTURE"].astype(str)

# COMMAND ----------

# DF_PERIOD_FORECAST_DATA.to_parquet(
#     "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/TEMP_DATA/BASELINE_SEC_FORECAST.parquet",
#     index=False,
# )

# COMMAND ----------


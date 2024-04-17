# Databricks notebook source
# MAGIC %run "../EnvironmentSetup"

# COMMAND ----------

# MAGIC %run "./YAFSU_Landing_Setup"

# COMMAND ----------

# MAGIC %run "./YAFSU_Utils"

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Dataset in INPUT folder

# COMMAND ----------

DATA_FOLDER_PATH = INPUT_PATH + DATATYPE + "/"
if DATATYPE == "PARQUET":
    df = pd.read_parquet(DATA_FOLDER_PATH + "*.parquet")
if DATATYPE == "EXCEL_XLSX":
    df = read_excel_folder(DATA_FOLDER_PATH, sheet_name="DATA", skiprows=0)
if DATATYPE == "CSV":
    # df = pd.read_csv(INPUT_PATH + "*.csv")
    raise Exception ("Not implemented CSV Reader yet")

###############################################################

df["CUSTOMER"] = df[KEY_CUSTOMER].astype(str).apply("|".join, axis=1)
df["PRODUCT"] = df[KEY_PRODUCT].astype(str).apply("|".join, axis=1)
df["KEY_TS"] = df["CUSTOMER"] + "|" + df["PRODUCT"]
df["DATE"] = pd.to_datetime(df["YEARWEEK"].astype(str) + "-4", format="%G%V-%w")

# COMMAND ----------

# MAGIC %md
# MAGIC # Pandas Profiling EDA Review

# COMMAND ----------

from pandas_profiling import ProfileReport

# COMMAND ----------

@ray.remote
def REMOTE_pandas_profiler(df_profile, title):
    try:
        profile = ProfileReport(df_profile)
        title = filename_standardize(title)
        profile.to_file(EDA_REPORT_PATH + title + ".html")
        return {"TITLE": title, "ERROR": "N"}
    except Exception as ex:
        return {"TITLE": title, "ERROR": str(ex)}


def autoeda_pandas_profiler(df):
    tasks = [
        REMOTE_pandas_profiler.remote(
            df_profile=df, title=f"AutoEDA_Report_{TASK_ID}_ALL_DATA"
        ),
        *[
            REMOTE_pandas_profiler.remote(
                df_profile=df_group, title=f"AutoEDA_Report_{TASK_ID}_CATE_{key}"
            )
            for key, df_group in df.groupby("CATEGORY")
            # for key, df_group in df.groupby(KEY_PRODUCT)
        ],
        # *[
        #     REMOTE_pandas_profiler.remote(
        #         df_profile=df_group, title=f"AutoEDA_Report_{TASK_ID}_CUST_{key}"
        #     )
        #     for key, df_group in df.groupby("CUSTOMER")
        # ],
    ]
    tasks = ray.get(tasks)
    df_error = pd.DataFrame(tasks)
    df_error = df_error[(df_error["ERROR"] != "N")]
    return df_error

# COMMAND ----------

df_auto_eda_error = autoeda_pandas_profiler(df)
df_auto_eda_error
# This error gonna track later.

# COMMAND ----------

# MAGIC %md
# MAGIC # Manual Time-series Validation reviews

# COMMAND ----------

def auto_validation_timeseries(df):
    df = df.query("YEARWEEK > 202000")
    today_calendar = datetime.now().isocalendar()
    today_yearweek = today_calendar[0] * 100 + today_calendar[1]
    if "FUTURE" not in df.columns:
        df["FUTURE"] = "N"
        df["FUTURE"].loc[(df["YEARWEEK"] >= today_yearweek)] = "Y"

    df_historical_timeseries = (
        df.query("FUTURE == 'N' ")
        .groupby("KEY_TS")
        .agg({"YEARWEEK": [min, max, "count"]})
        .reset_index()
        .sort_values(by=("YEARWEEK", "count"), ascending=True)
    )

    ###############################################################

    df_error_less_than_26_weeks = df_historical_timeseries[
        (df_historical_timeseries[("YEARWEEK", "count")] <= 26)
    ]
    df_error_less_than_52_weeks = df_historical_timeseries[
        (df_historical_timeseries[("YEARWEEK", "count")].between(26, 52))
    ]
    df_error_recency_less_than_26_weeks = df_historical_timeseries[
        (
            df_historical_timeseries[("YEARWEEK", "max")]
            <= find_yearweek_diff(
                df_historical_timeseries[("YEARWEEK", "max")].max(), 26
            )
        )
    ]

    ###############################################################

    # Remove keys/skus that haven't sold in 26 weeks (> 26 weeks)
    df_validation = df[
        ~(df["KEY_TS"].isin(df_error_recency_less_than_26_weeks["KEY_TS"].unique()))
    ]
    # Remove any with sales week number < 26 weeks
    df_validation = df_validation[
        ~(df_validation["KEY_TS"].isin(df_error_less_than_26_weeks["KEY_TS"].unique()))
    ]

    df_validation.to_parquet(TEMP_PATH + "DF_VALIDATED.parquet", index=False)

    error_log_dict = {
        "KEY_TS_<_26W": df_error_less_than_26_weeks["KEY_TS"].unique(),
        "KEY_TS_<_52W": df_error_less_than_52_weeks["KEY_TS"].unique(),
        "KEY_TS_RECENCY_>_26W": df_error_recency_less_than_26_weeks["KEY_TS"].unique(),
        # 'KEY_TS_OKAY': df_validation['KEY_TS'].unique()
    }
    df_error_log = (
        pd.DataFrame.from_dict(error_log_dict, orient="index").transpose().fillna("")
    )
    df_error_log.to_csv(ERROR_LOG_PATH + f"Validation Log {TASK_ID}.csv", index=False)

    info_log_dict = {
        "TOTAL_KEY_TS": len(df["KEY_TS"].unique()),
        "KEY_TS_OK": len(df_validation["KEY_TS"].unique()),
        "KEY_TS_<_26W": len(df_error_less_than_26_weeks["KEY_TS"].unique()),
        "KEY_TS_<_52W": len(df_error_less_than_52_weeks["KEY_TS"].unique()),
        "KEY_TS_RECENCY_>_26W": len(
            df_error_recency_less_than_26_weeks["KEY_TS"].unique()
        ),
    }

    return info_log_dict

# COMMAND ----------

info_log_dict = auto_validation_timeseries(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Send email of error & validation

# COMMAND ----------

import zipfile
import base64

with zipfile.ZipFile(f"/tmp/Validation Report {TASK_ID}.zip", mode="a") as archive:
    archive.write(
        ERROR_LOG_PATH + f"Validation Log {TASK_ID}.csv",
        f"Validation Log {TASK_ID}.csv",
    )
    archive.write(
        EDA_REPORT_PATH
        + filename_standardize(f"AutoEDA_Report_{TASK_ID}_ALL_DATA")
        + ".html",
        filename_standardize(f"AutoEDA_Report_{TASK_ID}_ALL_DATA") + ".html",
    )

with open(f"/tmp/Validation Report {TASK_ID}.zip", "rb") as binary_file:
    binary_file_data = binary_file.read()
    base64_encoded_data = base64.b64encode(binary_file_data)
    base64_message = base64_encoded_data.decode("utf-8")
    print (f"Length: {len(base64_message)}")

ATTACHMENTS = [
    {"FILENAME": f"Validation Report {TASK_ID}.zip", "B64_CONTENT": base64_message}
]

# COMMAND ----------

# EMAIL = dbutils.
SUBJECT = "Auto Data Validation Log"

CONTENT = f"""
Hello {EMAIL} - This is auto-email validation report for forecasting task: {TASK_ID}.

Here is the summary of your data:
- Total submitted time-series: {info_log_dict['TOTAL_KEY_TS']}
- TOBE Forecast time-series: {info_log_dict['KEY_TS_OK']} 
- Error: Sales Week < 26W: {info_log_dict['KEY_TS_<_26W']}
- Warning: 26W < Sales Week < 52W: {info_log_dict['KEY_TS_<_52W']} 
- Error: Sales Recency > 26W: {info_log_dict['KEY_TS_RECENCY_>_26W']}

Here is error table for you to review your dataset. 
- KEY_TS_<_26W: Sales Week less than 26W - Completely remove
- KEY_TS_<_52W: Sales Week between 26W and 52W - Try to run but not assure accuracy.
- KEY_TS_RECENCY_>_26W: In the last 26W you don't have sales - Completely remove.

You can read the ATTACHMENTS for details:
- Error table
- Quick EDA for total data (corrlations, missing values,...)

Your forecast result for GOOD QUALITY DATA ({info_log_dict['KEY_TS_OK']} time-series) gonna release in the next 1-hour if no further issues happen.  
"""

HTML_CONTENT = f"""
<p> </p>
"""

utils_send_email(TASK_ID, EMAIL, SUBJECT, CONTENT, HTML_CONTENT, ATTACHMENTS)

# COMMAND ----------

if info_log_dict["KEY_TS_OK"] == 0:
    # Send notification to MS teams / Update to MS Tasks
    # Then, raise Exception to end the workflow.
    raise Exception(f"Critical Data Issue - No Quality data forecast for {TASK_ID}")
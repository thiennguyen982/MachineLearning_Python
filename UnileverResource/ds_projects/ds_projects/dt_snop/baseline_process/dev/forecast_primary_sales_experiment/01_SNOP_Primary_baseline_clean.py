# Databricks notebook source
# MAGIC %run "/Repos/lai-trung-minh.duc@unilever.com/SC_DT_Forecast_Project/EnvironmentSetup"

# COMMAND ----------

import pandas as pd
import numpy as np

import warnings
warnings.filterwarnings("ignore")


# COMMAND ----------

df_promo_classifier = pd.read_parquet(
    "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/TEMP_DATA/DATASET_PROMO_CLASSIFICATION.parquet"
)
df_promo_classifier

# COMMAND ----------

df_promo_classifier["GREEN"] = np.where(
  np.logical_or(df_promo_classifier["BIZ_FINAL_ACTIVITY_TYPE"] == "GREEN", df_promo_classifier["ABNORMAL"] == "Y"), 1, 0
)
df_promo_classifier = df_promo_classifier.rename(columns={"REGION": "BANNER"})

# COMMAND ----------

df_promo_classifier = df_promo_classifier[["BANNER", "DPNAME", "YEARWEEK", "GREEN"]]

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Primary Sale

# COMMAND ----------

df_pri_sales = pd.read_parquet("/dbfs/mnt/adls/SAP_HANA_DATASET/RAW_DATA/PRI_SALES_BANNER_WEEKLY_PARQUET")
df_pri_sales

# COMMAND ----------

df_pri_sales = df_pri_sales.dropna()
df_pri_sales["MATERIAL"] = df_pri_sales["MATERIAL"].astype(int)

df_pri_sales = df_pri_sales[
    df_pri_sales["BANNER"].isin(
        ['DT MEKONG DELTA', 'DT HCME', 'DT CENTRAL', 'DT North']
    )
]
df_pri_sales = df_pri_sales.groupby(["YEARWEEK", "MATERIAL"])["PCS"].sum().reset_index()
df_pri_sales["REGION"] = "NATIONWIDE"
df_pri_sales["BANNER"] = "NATIONWIDE"
df_pri_sales

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
    ["Category", "SAP Code", "DP name", "Pcs/CS", "NW per CS (selling-kg)"]
]
df_master_product.columns = ["CATEGORY", "MATERIAL", "DPNAME", "PCS/CS", "KG/CS"]
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

# COMMAND ----------

df_pri_sales = df_pri_sales.merge(df_master_product, on="MATERIAL")

df_pri_sales["TON"] = df_pri_sales["PCS"] * df_pri_sales["KG/PCS"] / 1000
df_pri_sales["CS"] = df_pri_sales["PCS"] / df_pri_sales["PCS/CS"]

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
    sheet_name="GWeek Master"
)
df_week_master = df_week_master[['Weeknum','Year','Month Num','Week Dolphin','MONTH PHASING']]

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
df_week_master = df_week_master.drop(["Weeknum","MONTH PHASING"], axis = 1)
df_week_master = df_week_master.rename(
    columns={"Week Dolphin": "YEARWEEK", 
             "Year": "YEAR", 
             "Month Num": "MONTH"
    }
)

# COMMAND ----------

df_pri_sales["DATE"] = pd.to_datetime(df_pri_sales["YEARWEEK"] + "-1", format="%G%V-%w")

df_pri_sales["YEARWEEK"] = df_pri_sales["YEARWEEK"].astype(int)

df_pri_sales = df_pri_sales.merge(df_calendar_workingday, on="YEARWEEK")

df_pri_sales = df_pri_sales.merge(df_week_master, on = "YEARWEEK")

# COMMAND ----------

df_pri_sales["DAILY_AVG_SALES"] = df_pri_sales["ACTUALSALE"].div(df_pri_sales["DTWORKINGDAY"].astype(float))
df_pri_sales["DAILY_AVG_SALES"].loc[(df_pri_sales["DTWORKINGDAY"] == 0)] = 0

# COMMAND ----------

df_pri_sales = df_pri_sales.sort_values(by=["BANNER", "CATEGORY", "DPNAME", "YEARWEEK"])
df_pri_sales["KEY"] = df_pri_sales["BANNER"] + "|" + df_pri_sales["CATEGORY"] + "|" + df_pri_sales["DPNAME"]

# COMMAND ----------

df_pri_sales = df_pri_sales.merge(df_promo_classifier, how="left", on=["BANNER",  "DPNAME", "YEARWEEK"])
df_pri_sales["GREEN"] = df_pri_sales["GREEN"].fillna(0)

# COMMAND ----------

df_pri_sales

# COMMAND ----------

df_promo_nationwide = df_pri_sales.groupby(["DPNAME", "YEARWEEK"])["GREEN"].max().reset_index()
df_promo_nationwide = df_promo_nationwide.rename(columns={"GREEN": "GREEN_NW"})

# COMMAND ----------

df_pri_sales = df_pri_sales.merge(df_promo_nationwide, on=["DPNAME", "YEARWEEK"], how="left")
df_pri_sales["GREEN_NW"] = df_pri_sales["GREEN_NW"].fillna(0)
df_pri_sales["GREEN"] = df_pri_sales[["GREEN", "GREEN_NW"]].max(axis=1)

# COMMAND ----------

display(df_pri_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Baseline Cleaning

# COMMAND ----------

from scipy import stats


def baseline_clean(key, df_group):
    window = 13
    df_group["DAILY_AVG_SALES_ORIGINAL"] = df_group["DAILY_AVG_SALES"]

    # Clean at first sight with +/- 1.5*STD
    df_group_yellow = df_group.query("GREEN == 0")
    total_mean = df_group_yellow["DAILY_AVG_SALES"].mean()
    total_std = df_group_yellow["DAILY_AVG_SALES"].std()
    total_range_up = total_mean + 1 * total_std
    total_range_down = total_mean - 1 * total_std

    df_group["DAILY_AVG_SALES"].loc[
        (df_group["DAILY_AVG_SALES"] > total_range_up)
    ] = total_range_up
    df_group["DAILY_AVG_SALES"].loc[
        (df_group["DAILY_AVG_SALES"] < total_range_down)
    ] = total_range_down

    # Clean with moving aggregations features
    moving_average = df_group["DAILY_AVG_SALES"].rolling(window).mean()
    moving_std = df_group["DAILY_AVG_SALES"].rolling(window).std()
    moving_median = df_group["DAILY_AVG_SALES"].rolling(window).median()
    moving_median_abs_deviation = (
        df_group["DAILY_AVG_SALES"].rolling(window).apply(stats.median_abs_deviation)
    )

    df_group["MA"] = moving_average
    df_group["MSTD"] = moving_std
    df_group["MM"] = moving_median
    df_group["MMAD"] = moving_median_abs_deviation

    df_group["RANGE_UP_MASTD"] = df_group["MA"] + df_group["MSTD"]
    df_group["RANGE_DOWN_MASTD"] = df_group["MA"] - df_group["MSTD"]
    df_group["RANGE_UP_MEDIAN"] = df_group["MM"] + 2 * df_group["MMAD"]
    df_group["RANGE_DOWN_MEDIAN"] = df_group["MM"] - 2 * df_group["MMAD"]

    # df_group['RANGE_UP'] = df_group[['RANGE_UP_MEDIAN', 'RANGE_UP_MASTD']].min(axis=1)
    # df_group['RANGE_DOWN'] = df_group[['RANGE_DOWN_MEDIAN', 'RANGE_DOWN_MASTD']].max(axis=1)

    df_group["RANGE_UP"] = df_group["RANGE_UP_MASTD"]
    df_group["RANGE_DOWN"] = df_group["RANGE_DOWN_MASTD"]

    df_group["BASELINE"] = df_group["DAILY_AVG_SALES"]
    df_group["BASELINE"].loc[
        (df_group["DAILY_AVG_SALES"] > df_group["RANGE_UP"])
    ] = df_group["RANGE_UP"]
    df_group["BASELINE"].loc[
        (df_group["DAILY_AVG_SALES"] < df_group["RANGE_DOWN"])
    ] = df_group["RANGE_DOWN"]
    df_group["BASELINE_DAILY"] = df_group["BASELINE"]
    df_group["BASELINE"] = df_group["BASELINE"] * df_group["DTWORKINGDAY"]

    df_group["KEY"] = key

    return df_group


def baseline_clean_52weeks(key, df_group):
    window = 13
    df_group["DAILY_AVG_SALES_ORIGINAL"] = df_group["DAILY_AVG_SALES"]
    std_ratio = 1

    df_group = df_group.sort_values("YEARWEEK")
    # Clean at first sight with 52-Weeks Yellow
    moving_average = (
        df_group["DAILY_AVG_SALES"]
        .rolling(52)
        .apply(
            lambda series: df_group.loc[series.index]
            .query("GREEN == 0")["DAILY_AVG_SALES"]
            .mean()
        )
    )
    moving_std = (
        df_group["DAILY_AVG_SALES"]
        .rolling(52)
        .apply(
            lambda series: df_group.loc[series.index]
            .query("GREEN == 0")["DAILY_AVG_SALES"]
            .std()
        )
    )
    df_group["MA_YELLOW"] = moving_average
    df_group["MSTD_YELLOW"] = moving_std
    df_group["RANGE_UP"] = df_group["MA_YELLOW"] + std_ratio * df_group["MSTD_YELLOW"]
    df_group["RANGE_DOWN"] = df_group["MA_YELLOW"] - std_ratio * df_group["MSTD_YELLOW"]

    df_group["DAILY_AVG_SALES"].loc[
        (df_group["DAILY_AVG_SALES"] > df_group["RANGE_UP"])
    ] = df_group["RANGE_UP"]
    df_group["DAILY_AVG_SALES"].loc[
        (df_group["DAILY_AVG_SALES"] < df_group["RANGE_DOWN"])
    ] = df_group["RANGE_DOWN"]

    # Clean with moving aggregations features
    moving_average = df_group["DAILY_AVG_SALES"].rolling(window).mean()
    moving_std = df_group["DAILY_AVG_SALES"].rolling(window).std()
    # moving_median = df_group['DAILY_AVG_SALES'].rolling(window).median()
    # moving_median_abs_deviation = df_group['DAILY_AVG_SALES'].rolling(window).apply(stats.median_abs_deviation)

    df_group["MA"] = moving_average
    df_group["MSTD"] = moving_std
    # df_group['MM'] = moving_median
    # df_group['MMAD'] = moving_median_abs_deviation

    # df_group['RANGE_UP_MASTD'] = df_group['MA'] + df_group['MSTD']
    # df_group['RANGE_DOWN_MASTD'] = df_group['MA'] - df_group['MSTD']
    # df_group['RANGE_UP_MEDIAN'] = df_group['MM'] + 2*df_group['MMAD']
    # df_group['RANGE_DOWN_MEDIAN'] = df_group['MM'] - 2*df_group['MMAD']

    # df_group['RANGE_UP'] = df_group[['RANGE_UP_MEDIAN', 'RANGE_UP_MASTD']].min(axis=1)
    # df_group['RANGE_DOWN'] = df_group[['RANGE_DOWN_MEDIAN', 'RANGE_DOWN_MASTD']].max(axis=1)

    df_group["RANGE_UP"] = df_group["MA"] + std_ratio * df_group["MSTD"]
    df_group["RANGE_DOWN"] = df_group["MA"] - std_ratio * df_group["MSTD"]

    df_group["BASELINE"] = df_group["DAILY_AVG_SALES"]
    df_group["BASELINE"].loc[
        (df_group["DAILY_AVG_SALES"] > df_group["RANGE_UP"])
    ] = df_group["RANGE_UP"]
    df_group["BASELINE"].loc[
        (df_group["DAILY_AVG_SALES"] < df_group["RANGE_DOWN"])
    ] = df_group["RANGE_DOWN"]
    df_group["BASELINE_DAILY"] = df_group["BASELINE"]
    df_group["BASELINE"] = df_group["BASELINE"] * df_group["DTWORKINGDAY"]

    df_group["KEY"] = key

    return df_group

# COMMAND ----------

def baseline_clean_20230524(key, df_group):
    df_group["DAILY_AVG_SALES_ORIGINAL"] = df_group["DAILY_AVG_SALES"]
    std_ratio = 1

    df_group = df_group.sort_values("YEARWEEK")

    ######### Stage 01

    # Calculate moving average backward / forward 26 weeks.

    moving_average_left = (
        df_group["DAILY_AVG_SALES"]
        .rolling(26)
        .apply(
            lambda series: df_group.loc[series.index]
            .query("GREEN == 0")["DAILY_AVG_SALES"]
            .mean()
        )
    )

    moving_average_right = (
        df_group["DAILY_AVG_SALES"]
        .iloc[::-1]
        .rolling(26)
        .apply(
            lambda series: df_group.loc[series.index]
            .query("GREEN == 0")["DAILY_AVG_SALES"]
            .mean()
        )
        .iloc[::-1]
    )

    moving_average = moving_average_left + moving_average_right
    moving_average = moving_average / 2
    moving_average = moving_average.fillna(moving_average_left).fillna(
        moving_average_right
    )

    # Calculate moving STD backward 26 weeks.
    moving_std = (
        df_group["DAILY_AVG_SALES"]
        .rolling(26)
        .apply(
            lambda series: df_group.loc[series.index]
            .query("GREEN == 0")["DAILY_AVG_SALES"]
            .std()
        )
    )

    # Cut outlier 1st Stage

    df_group["MA_YELLOW"] = moving_average
    df_group["MSTD_YELLOW"] = moving_std
    df_group["RANGE_UP_YELLOW"] = (
        df_group["MA_YELLOW"] + std_ratio * df_group["MSTD_YELLOW"]
    )
    df_group["RANGE_DOWN_YELLOW"] = (
        df_group["MA_YELLOW"] - std_ratio * df_group["MSTD_YELLOW"]
    )

    df_group["DAILY_AVG_SALES"].loc[
        (df_group["DAILY_AVG_SALES"] > df_group["RANGE_UP_YELLOW"])
    ] = df_group["RANGE_UP_YELLOW"]
    df_group["DAILY_AVG_SALES"].loc[
        (df_group["DAILY_AVG_SALES"] < df_group["RANGE_DOWN_YELLOW"])
    ] = df_group["RANGE_DOWN_YELLOW"]

    ######### Stage 02

    moving_average_left = df_group["DAILY_AVG_SALES"].shift(1).rolling(6).mean()
    moving_average_right = (
        df_group["DAILY_AVG_SALES"].shift(-1).iloc[::-1].rolling(6).mean().iloc[::-1]
    )
    moving_average = moving_average_left + moving_average_right
    moving_average = moving_average / 2
    moving_average = moving_average.fillna(moving_average_left).fillna(
        moving_average_right
    )

    moving_std_left = df_group["DAILY_AVG_SALES"].shift(1).rolling(6).std()
    moving_std_right = (
        df_group["DAILY_AVG_SALES"].shift(-1).iloc[::-1].rolling(6).std().iloc[::-1]
    )
    moving_std = moving_std_left + moving_std_right
    moving_std = moving_std / 2
    moving_std = moving_std.fillna(moving_std_left).fillna(moving_std_right)

    df_group["MA"] = moving_average
    df_group["MSTD"] = moving_std

    df_group["RANGE_UP"] = df_group["MA"] + std_ratio * df_group["MSTD"]
    df_group["RANGE_DOWN"] = df_group["MA"] - std_ratio * df_group["MSTD"]

    df_group["BASELINE"] = df_group["DAILY_AVG_SALES"]
    df_group["BASELINE"].loc[
        (df_group["DAILY_AVG_SALES"] > df_group["RANGE_UP"])
    ] = df_group["RANGE_UP"]
    df_group["BASELINE"].loc[
        (df_group["DAILY_AVG_SALES"] < df_group["RANGE_DOWN"])
    ] = df_group["RANGE_DOWN"]
    df_group["BASELINE_DAILY"] = df_group["BASELINE"]
    df_group["BASELINE"] = df_group["BASELINE"] * df_group["DTWORKINGDAY"]

    df_group["KEY"] = key

    return df_group

# COMMAND ----------

# import ray
# @ray.remote
def REMOTE_baseline_clean(key, df_group):
    warnings.filterwarnings("ignore")
    df_result = baseline_clean_20230524(key, df_group)
    print(key)
    # df_result = baseline_clean(key, df_group)
    # df_result = baseline_clean_52weeks(key, df_group)
    return df_result

# COMMAND ----------

tasks = [
    REMOTE_baseline_clean(key, df_group) for key, df_group in df_pri_sales.groupby("KEY")
]
# tasks = ray.get(tasks)
df_all = pd.concat(tasks)

# COMMAND ----------

df_all

# COMMAND ----------

!pip install pandas_profiling

# COMMAND ----------

df_all.to_csv("/Workspace/Users/ng-minh-hoang.dat@unilever.com/Forecast Primary Sales/Primary_baseline_clean.csv")


# COMMAND ----------


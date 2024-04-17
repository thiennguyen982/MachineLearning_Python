# Databricks notebook source
# MAGIC %run "/Repos/lai-trung-minh.duc@unilever.com/SC_DT_Forecast_Project/EnvironmentSetup"

# COMMAND ----------

import pandas as pd
import numpy as np
from scipy.stats import linregress
import math

import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go

import warnings
warnings.filterwarnings("ignore")

from tqdm import tqdm

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

# COMMAND ----------

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

df_pri_sales

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

df_pri_sales["DATE"] = pd.to_datetime(df_pri_sales["YEARWEEK"] + "-1", format="%G%V-%w")

df_pri_sales["YEARWEEK"] = df_pri_sales["YEARWEEK"].astype(int)

df_pri_sales = df_pri_sales.merge(df_calendar_workingday, on="YEARWEEK")

df_pri_sales = df_pri_sales.merge(df_week_master, on = "YEARWEEK")

df_pri_sales["QUARTER"] = ((df_pri_sales["MONTH"] - 1) / 3).astype(int) + 1

# COMMAND ----------

# df_pri_sales["WEEK/MONTH RATIO"] = df_pri_sales["PCS"] / df_pri_sales.groupby(
#     ["BANNER", "REGION", "CATEGORY", "DPNAME", "YEAR", "MONTH"]
# )["PCS"].transform(sum)

# df_pri_sales["SELLINGDAY RATIO"] = (
#     df_pri_sales["WEEK/MONTH RATIO"] / df_pri_sales["DTWORKINGDAY"]
# )

df_pri_sales = df_pri_sales.fillna(0)

# COMMAND ----------

df_pri_sales = df_pri_sales.sort_values(["CATEGORY","DPNAME","YEARWEEK"]).reset_index(drop = True)
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
df_sec_sales.head(3)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Convert Sec2Pri Baseline with median ratio_phasing Weekly/Month 2 years latest

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

def convert_data(df_convert, df_ratio_WEEK_MONTH):
    # df_convert = df_convert_pattern.copy()
    df_convert = df_convert.drop(
        "SELLINGDAY_WEEK/MONTH RATIO_median",
        axis=1,
    )

    df_convert = df_convert.merge(
        df_ratio_WEEK_MONTH,
        on=["WEEK/MONTH COUNT", "WEEK/MONTH ORDER", "KEY"],
        how="left",
    )

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
        df_convert.groupby(["KEY", "YEAR", "MONTH"])["PROPOSED_FC_WEEKLY"].transform(
            "sum"
        )
        * df_convert["SELLINGDAY_WEEK/MONTH RATIO_median"]
    )

    df_convert["FC_PRI_BASELINE_WEEKLY"] = (
        df_convert["FC_PRI_BASELINE"] * df_convert["DTWORKINGDAY"]
    )

    return df_convert

# COMMAND ----------

import datetime
from dateutil.relativedelta import relativedelta

current_date = datetime.datetime.now().isocalendar()
current_yearweek = current_date[0] * 100 + current_date[1]

df_future_time = df_week_master[df_week_master["YEARWEEK"] >= current_yearweek]
display(df_future_time)

# COMMAND ----------

dict_future_time = {}
for year_idx in df_future_time["YEAR"].unique():
    dict_future_time[year_idx] = []
    for month_idx in df_future_time[df_future_time["YEAR"] == year_idx]["MONTH"].unique():
      dict_future_time[year_idx].append(int(month_idx))

dict_future_time

# COMMAND ----------

df_convert = df_sec_sales.copy()
df_convert = df_convert.rename(columns={"ACTUALSALE": "SEC_SALES"})

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

df_convert = df_convert.merge(
    df_pri_sales[["CATEGORY", "DPNAME", "YEARWEEK", "ACTUALSALE"]],
    how="left",
    on=["CATEGORY", "DPNAME", "YEARWEEK"],
)
df_convert = df_convert.rename(columns={"ACTUALSALE": "PRI_SALES"})

df_convert = df_convert.sort_values(["KEY", "YEARWEEK"]).reset_index(drop=True)

df_convert["FC_PRI_BASELINE"] = 0
df_convert["FC_PRI_BASELINE_WEEKLY"] = df_convert[
    df_convert["YEARWEEK"] <= current_yearweek
]["PRI_SALES"]
df_convert["SELLINGDAY_WEEK/MONTH RATIO_median"] = 0

# df_convert["FC_PRI_BASELINE_WEEKLY"][
#     df_convert["YEARWEEK"] > current_yearweek
# ] = df_convert["FC_PRI_BASELINE_WEEKLY"].fillna(0)

# COMMAND ----------

for year_idx in dict_future_time.keys():
    for month_idx in dict_future_time[year_idx]:
        # phasing_lower = df_convert["YEARWEEK"][
        #     (df_convert["YEAR"] >= (year_idx - 2)) & (df_convert["MONTH"] == month_idx)
        # ].min()

        # if month_idx == 1:
        #     phasing_upper = df_convert["YEARWEEK"][
        #         df_convert["YEAR"] <= (year_idx - 1)
        #     ].max()
        # else:
        #     phasing_upper = df_convert["YEARWEEK"][
        #         (df_convert["YEAR"] <= year_idx) & (df_convert["MONTH"] == (month_idx - 1))
        #     ].max()
            
        # df_ratio_phasing = df_convert[
        #     (df_convert["YEARWEEK"] >= phasing_lower) & (df_convert["YEARWEEK"] <= phasing_upper)
        # ]
        start_date = datetime.datetime(year = year_idx, month = month_idx, day = 1) - relativedelta(months= 24)
        end_date = datetime.datetime(year = year_idx, month = month_idx, day = 28) - relativedelta(months= 1)
        df_ratio_phasing = df_convert[
            (df_convert["DATE"] >= start_date)
            & (df_convert["DATE"] <= end_date)
        ]

        df_ratio_WEEK_MONTH = create_ratio_phasing(df_ratio_phasing, "FC_PRI_BASELINE_WEEKLY")

        df_convert_pattern = convert_data(
            df_convert[
                (df_convert["YEAR"] == year_idx) & (df_convert["MONTH"] == month_idx)
            ],
            df_ratio_WEEK_MONTH,
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

display(
    df_convert[
        (df_convert["KEY"] == "NATIONWIDE|FABSOL|OMO LIQUID MATIC CFT SS (POU) 3.7KG")
        & (df_convert["YEARWEEK"] >= current_yearweek)
    ]
)

# COMMAND ----------

fig = px.line(
    data_frame=df_convert[
        (df_convert["KEY"] == "NATIONWIDE|FABSOL|OMO LIQUID MATIC CFT SS (POU) 3.7KG")
        & (df_convert["YEARWEEK"] >= current_yearweek)
    ],
    x="DATE",
    y=["PROPOSED_FC_WEEKLY", "FC_PRI_BASELINE_WEEKLY"],
)
fig.show()

# COMMAND ----------

write_excel_dataframe(
    df_convert,
    groupby_arr=["CATEGORY"],
    dbfs_directory="/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/OUTPUT_FORECAST/FC_BASELINE_SEC2PRI/",
    sheet_name="DATA",
)

# COMMAND ----------

# weekly save 
from datetime import date
OUTPUT_PATH =  "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/OUTPUT_FORECAST/FC_BASELINE_SEC2PRI_HIS/" + date.today().strftime("%Y%m%d") + '/'

if os.path.exists(OUTPUT_PATH) == False: 
    os.makedirs(OUTPUT_PATH)

write_excel_dataframe(
    df_convert,
    groupby_arr=["CATEGORY"],
    dbfs_directory=OUTPUT_PATH,
    sheet_name="DATA",
)

# COMMAND ----------


# Databricks notebook source
import pandas as pd
import numpy as np
from scipy.stats import linregress
from statsmodels.tsa.seasonal import STL, seasonal_decompose
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
df_master_dpname = df_master_dpname[["DP Name", "DP Name Current", "Segment"]]
df_master_dpname.columns = ["DPNAME", "DPNAME CURRENT", "SEGMENT"]
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

df_master_product

# COMMAND ----------

df_pri_sales = df_pri_sales.merge(df_master_product, on="MATERIAL")

df_pri_sales["TON"] = df_pri_sales["PCS"] * df_pri_sales["KG/PCS"] / 1000
df_pri_sales["CS"] = df_pri_sales["PCS"] / df_pri_sales["PCS/CS"]

df_pri_sales = (
    df_pri_sales.groupby(["YEARWEEK", "BANNER", "REGION", "CATEGORY", "SEGMENT", "DPNAME"])[
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
df_week_master

# COMMAND ----------

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

# df_pri_sales["YEAR"] = df_pri_sales["DATE"].dt.year
# df_pri_sales["MONTH"] = df_pri_sales["DATE"].dt.month
df_pri_sales = df_pri_sales.merge(df_week_master, on = "YEARWEEK")

df_pri_sales["QUARTER"] = ((df_pri_sales["MONTH"] - 1) / 3).astype(int)

# COMMAND ----------

df_pri_sales["WEEK/MONTH RATIO"] = df_pri_sales["PCS"] / df_pri_sales.groupby(
    ["BANNER", "REGION", "CATEGORY", "DPNAME", "YEAR", "MONTH"]
)["PCS"].transform(sum)

# df_pri_sales["SELLINGDAY RATIO"] = (
#     df_pri_sales["WEEK/MONTH RATIO"] / df_pri_sales["DTWORKINGDAY"]
# )
df_pri_sales["SELLINGDAY RATIO"] = df_pri_sales.apply(
    lambda x: 0 if x["DTWORKINGDAY"] == 0 else x["WEEK/MONTH RATIO"] / x["DTWORKINGDAY"], axis =1
)

# COMMAND ----------

df_pri_sales["PRI_SALES_QUARTERLY"] = (
    df_pri_sales.groupby(["DPNAME", "YEAR", "QUARTER"])["ACTUALSALE"]
    .transform("sum")
    .astype(float)
)

df_pri_sales["COUNT WEEKS"] = df_pri_sales.groupby(
    ["BANNER", "REGION", "CATEGORY", "DPNAME", "YEAR", "MONTH"]
)["YEARWEEK"].transform("count")

df_pri_sales = df_pri_sales.fillna(0)
df_pri_sales.isnull().sum().sum()

# COMMAND ----------

df_pri_sales

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Secondary Sale

# COMMAND ----------

df_sec_sales = pd.read_parquet("/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/TEMP_DATA/BASELINE_SEC_FORECAST.parquet")
df_sec_sales

# COMMAND ----------

df_sec_sales[['BANNER', 'CATEGORY', 'DPNAME']] = df_sec_sales['KEY'].str.split('|', expand=True)

df_sec_sales["KEY_NATIONWIDE"] = (
    df_sec_sales["CATEGORY"] + "|" + df_sec_sales["DPNAME"]
)

# COMMAND ----------

df_sec_sales = df_sec_sales.drop(["DTWORKINGDAY", "WORKINGDAY"], axis=1)
df_sec_sales = df_sec_sales.merge(df_calendar_workingday, on="YEARWEEK")
df_sec_sales = df_sec_sales[df_sec_sales["BANNER"] == "NATIONWIDE"]


# COMMAND ----------

df_sec_sales["DATE"] = pd.to_datetime(
    (df_sec_sales["YEARWEEK"]).astype(str) + "-1", format="%G%V-%w"
)

df_sec_sales = df_sec_sales.merge(df_week_master, on="YEARWEEK")

df_sec_sales["QUARTER"] = ((df_sec_sales["MONTH"] - 1) / 3).astype(int)

df_sec_sales["TOTAL_SEC_SALES"] = (
    df_sec_sales.groupby(["DPNAME"])["ACTUALSALE"].transform("sum").astype(float)
)

# COMMAND ----------

df_sec_sales = df_sec_sales.drop_duplicates(
    subset=["KEY", "YEARWEEK"], keep="first"
)

# COMMAND ----------

df_sec_sales

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # EDA consitence DP in Pri & Sec

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Data consistence DP

# COMMAND ----------

print(df_pri_sales['DPNAME'].nunique(), df_sec_sales['DPNAME'].nunique())
df_pri_sales.shape, df_sec_sales.shape

# COMMAND ----------

counts1 = df_pri_sales["DPNAME"].unique()
counts2 = df_sec_sales["DPNAME"].unique()
count_common_dp = 0
list_common_dp = []

for i, idx_pri in enumerate(counts1):
    for j, idx_sec in enumerate(counts2):
        if idx_sec == idx_pri:
            count_common_dp += 1
            list_common_dp.append(idx_sec)

# COMMAND ----------

df_consist_dp = df_pri_sales[["DPNAME", "YEARWEEK", "DATE","CATEGORY", "ACTUALSALE"]][
    df_pri_sales["DPNAME"].isin(list_common_dp)
].merge(
    df_sec_sales[["DPNAME", "YEARWEEK", "DATE","CATEGORY", "ACTUALSALE"]][
        df_sec_sales["DPNAME"].isin(list_common_dp)
    ],
    on=["DPNAME","YEARWEEK", "DATE","CATEGORY"],
    how="outer",
)

# COMMAND ----------

df_consist_dp = df_consist_dp.rename(
    columns={"ACTUALSALE_x": "PRI_SALES", "ACTUALSALE_y": "SEC_SALES"}
)
df_consist_dp = df_consist_dp[df_consist_dp["YEARWEEK"] < 202338]

df_consist_dp["COUNT_PRI_SALES"] = (
    df_consist_dp.groupby(["DPNAME"])["PRI_SALES"].transform("count").astype(int)
)
df_consist_dp["COUNT_SEC_SALES"] = (
    df_consist_dp.groupby(["DPNAME"])["SEC_SALES"].transform("count").astype(int)
)

df_consist_dp["TOTAL_SEC_SALES"] = (
    df_consist_dp.groupby(["DPNAME"])["SEC_SALES"].transform("sum").astype(float)
)

df_consist_dp["TOTAL_PRI_SALES"] = (
    df_consist_dp.groupby(["DPNAME"])["PRI_SALES"].transform("sum").astype(float)
)

df_consist_dp["PRI_SALES"] = df_consist_dp["PRI_SALES"].fillna(0)
df_consist_dp["SEC_SALES"] = df_consist_dp["SEC_SALES"].fillna(0)

df_consist_dp = df_consist_dp.merge(df_week_master, on="YEARWEEK")
df_consist_dp = df_consist_dp.merge(df_calendar_workingday, on="YEARWEEK")

df_consist_dp = df_consist_dp.sort_values(["DPNAME", "DATE"]).reset_index(drop=True)

# COMMAND ----------

df_consist_dp["RATIO_WEEKLY"] = (
    df_consist_dp["SEC_SALES"] - df_consist_dp["PRI_SALES"]
) / df_consist_dp["PRI_SALES"]

df_consist_dp["PRI_SALES_MONTHLY"] = (
    df_consist_dp.groupby(["DPNAME", "YEAR", "MONTH"])["PRI_SALES"]
    .transform("sum")
    .astype(float)
)

df_consist_dp["SEC_SALES_MONTHLY"] = (
    df_consist_dp.groupby(["DPNAME", "YEAR", "MONTH"])["SEC_SALES"]
    .transform("sum")
    .astype(float)
)

df_consist_dp["RATIO_MONTHLY"] = (
    df_consist_dp["SEC_SALES_MONTHLY"] - df_consist_dp["PRI_SALES_MONTHLY"]
) / df_consist_dp["PRI_SALES_MONTHLY"]

df_consist_dp["QUARTER"] = ((df_consist_dp["MONTH"] - 1) / 3).astype(int)

df_consist_dp["PRI_SALES_QUARTERLY"] = (
    df_consist_dp.groupby(["DPNAME", "YEAR", "QUARTER"])["PRI_SALES"]
    .transform("sum")
    .astype(float)
)

df_consist_dp["SEC_SALES_QUARTERLY"] = (
    df_consist_dp.groupby(["DPNAME", "YEAR", "QUARTER"])["SEC_SALES"]
    .transform("sum")
    .astype(float)
)

df_consist_dp["RATIO_QUARTERLY"] = (
    df_consist_dp["SEC_SALES_QUARTERLY"] - df_consist_dp["PRI_SALES_QUARTERLY"]
) / df_consist_dp["PRI_SALES_QUARTERLY"]

df_consist_dp["MA_RATIO_WEEK/MONTH"] = (
    df_consist_dp["RATIO_WEEKLY"].rolling(window=4).mean()
)
df_consist_dp["MA_RATIO_WEEK/QUARTER"] = (
    df_consist_dp["RATIO_WEEKLY"].rolling(window=13).mean()
)
df_consist_dp["MA_RATIO_MONTH/QUARTER"] = (
    df_consist_dp["RATIO_MONTHLY"].rolling(window=3).mean()
)

df_consist_dp

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Validate Current Convert Sec2Pri Logic

# COMMAND ----------

# Agg to NATIONWIDE
# Group 1: By DPNAME | group 4 weeks/month | week-order | from 2018 to now --> mean/median/std
# Group 2: By DPNAME | group 5 weeks/month | week-order | from 2018 to now --> mean/median/std

# Secondary Sales Forecast:
# Step 1: Agg to Month
# Step 2: Map ratio of primary to each week-order
# Step 3: Multiply Month * Week-order ratio of primary.

# COMMAND ----------

for i in df_pri_sales["CATEGORY"].unique():
    for j in df_pri_sales["CATEGORY"].unique():
        if i != j:
            if any(
                (df_pri_sales["SEGMENT"][df_pri_sales["CATEGORY"] == i]).isin(
                    df_pri_sales["SEGMENT"][df_pri_sales["CATEGORY"] == j]
                )
            ):
                print(i, j)

# COMMAND ----------

oral = df_pri_sales[df_pri_sales['CATEGORY'] == 'ORAL']
tbrush = df_pri_sales[df_pri_sales['CATEGORY'] == 'TBRUSH']
oral['SEGMENT'].unique(), tbrush['SEGMENT'].unique()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Evaluation function

# COMMAND ----------

def create_ratio_3phase(df_pri_sales):
    ratio_WEEK_MONTH_arr = []
    ratio_WEEK_QUARTER_arr = []
    ratio_MONTH_QUARTER_arr = []

    # ********************************************************
    # Ratio Week per Month
    for key, df_group in tqdm(
        df_pri_sales.groupby(["BANNER", "REGION", "CATEGORY", "DPNAME"])
    ):
        # df_group = df_group.set_index("DATE")
        # df_group = df_group.asfreq("W-MON")
        # df_group = df_group.sort_index()
        df_group = df_group.sort_values('DATE')
        df_group = df_group.drop(['DATE'], axis = 1)
        df_group["ACTUALSALE"] = df_group["ACTUALSALE"].fillna(0)

        df_group["WEEK/MONTH RATIO"] = df_group["ACTUALSALE"] / df_group.groupby(
            ["YEAR", "MONTH"]
        )["ACTUALSALE"].transform("sum")

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

    df_ratio_WEEK_MONTH["WEEK/MONTH ORDER_"] = df_ratio_WEEK_MONTH["WEEK/MONTH ORDER_"].astype(int)

    df_ratio_WEEK_MONTH.columns = [
        "WEEK/MONTH COUNT",
        "WEEK/MONTH ORDER",
        "SELLINGDAY_WEEK/MONTH RATIO_mean",
        "SELLINGDAY_WEEK/MONTH RATIO_median",
        "SELLINGDAY_WEEK/MONTH RATIO_std",
        "KEY_NATIONWIDE",
    ]



    # ********************************************************
    # Ratio Week per Quarter
    for key, df_group in tqdm(
        df_pri_sales.groupby(["BANNER", "REGION", "CATEGORY", "DPNAME"])
    ):
        # df_group = df_group.set_index("DATE")
        # # df_group = df_group.asfreq("W-MON")
        # df_group = df_group.sort_index()
        df_group = df_group.sort_values('DATE')
        df_group = df_group.drop(['DATE'], axis = 1)
        df_group["ACTUALSALE"] = df_group["ACTUALSALE"].fillna(0)

        df_group["WEEK/QUARTER RATIO"] = df_group["ACTUALSALE"] / df_group.groupby(
            ["YEAR", "QUARTER"]
        )["ACTUALSALE"].transform("sum")

        df_group["WEEK/QUARTER COUNT"] = df_group.groupby(["YEAR", "QUARTER"])[
            "YEARWEEK"
        ].transform("count")

        df_group["WEEK/QUARTER ORDER"] = df_group.groupby(["YEAR", "QUARTER"])[
            "YEARWEEK"
        ].transform("rank")

        # Contribution of selling day follow phase Week/Quarter
        df_group["SELLINGDAY_WEEK/QUARTER RATIO"] = (
            df_group["WEEK/QUARTER RATIO"] / df_group["DTWORKINGDAY"]
        )

        ratio_WEEK_QUARTER = (
            df_group.groupby(["WEEK/QUARTER COUNT", "WEEK/QUARTER ORDER"])
            .agg({"SELLINGDAY_WEEK/QUARTER RATIO": ["mean", "median", "std"]})
            .reset_index()
        )
        
        ratio_WEEK_QUARTER.columns = ["_".join(col) for col in ratio_WEEK_QUARTER.columns]
        ratio_WEEK_QUARTER["KEY"] = "|".join(key)
        ratio_WEEK_QUARTER_arr.append(ratio_WEEK_QUARTER)

    # ********************************************************
    df_ratio_WEEK_QUARTER = pd.concat(ratio_WEEK_QUARTER_arr)
    df_ratio_WEEK_QUARTER = df_ratio_WEEK_QUARTER.query("`WEEK/QUARTER COUNT_` >= 11")
    df_ratio_WEEK_QUARTER = df_ratio_WEEK_QUARTER.dropna(
        subset=["SELLINGDAY_WEEK/QUARTER RATIO_median"]
    )
    df_ratio_WEEK_QUARTER["KEY"] = df_ratio_WEEK_QUARTER["KEY"].str.replace(
        "NATIONWIDE\|NATIONWIDE\|", ""
    )

    df_ratio_WEEK_QUARTER["WEEK/QUARTER ORDER_"] = (
        df_ratio_WEEK_QUARTER["WEEK/QUARTER ORDER_"].astype(int)
    )

    df_ratio_WEEK_QUARTER.columns = [
        "WEEK/QUARTER COUNT",
        "WEEK/QUARTER ORDER",
        "SELLINGDAY_WEEK/QUARTER RATIO_mean",
        "SELLINGDAY_WEEK/QUARTER RATIO_median",
        "SELLINGDAY_WEEK/QUARTER RATIO_std",
        "KEY_NATIONWIDE",
    ]



    # ********************************************************
    # Ratio Month per Quarter
    # for key, df_group in tqdm(
    #     df_pri_sales.groupby(["BANNER", "REGION", "CATEGORY", "DPNAME"])
    # ):
    #     df_group = df_group.set_index("DATE")
    #     df_group = df_group.asfreq("W-MON")
    #     df_group = df_group.sort_index()
    #     df_group["ACTUALSALE"] = df_group["ACTUALSALE"].fillna(0)

    #     df_group["PRI_SALES_MONTHLY"] = (
    #         df_group.groupby(["YEAR", "MONTH"])["ACTUALSALE"].transform("sum").astype(float)
    #     )

    #     df_group["MONTH/QUARTER RATIO"] = df_group["PRI_SALES_MONTHLY"] / df_group.groupby(
    #         ["YEAR", "QUARTER"]
    #     )["PRI_SALES_MONTHLY"].transform(lambda x: x.unique().sum())

    #     df_group["MONTH/QUARTER COUNT"] = df_group.groupby(["YEAR", "QUARTER"])[
    #         "MONTH"
    #     ].transform(lambda x: x.nunique())

    #     df_group["MONTH/QUARTER ORDER"] = df_group.groupby(["YEAR", "QUARTER"])[
    #         "MONTH"
    #     ].rank(method = 'dense')

    #     df_group["COUNT WEEK"] = df_group.groupby(["YEAR", "MONTH"])[
    #         "YEARWEEK"
    #     ].transform("count")

    #     df_group["WORKINGDAY MONTH COUNT"] = df_group["DTWORKINGDAY"]*df_group["COUNT WEEK"]

    #     # Contribution of selling day follow phase Week/Quarter
    #     df_group["SELLINGDAY_MONTH/QUARTER RATIO"] = (
    #         df_group["MONTH/QUARTER RATIO"] / df_group["WORKINGDAY MONTH COUNT"]
    #     )

    #     ratio_MONTH_QUARTER = (
    #         df_group.groupby(["MONTH/QUARTER COUNT", "MONTH/QUARTER ORDER"])
    #         .agg({"SELLINGDAY_MONTH/QUARTER RATIO": ["mean", "median", "std"]})
    #         .reset_index()
    #     )

    #     ratio_MONTH_QUARTER.columns = ["_".join(col) for col in ratio_MONTH_QUARTER.columns]
    #     ratio_MONTH_QUARTER["KEY"] = "|".join(key)
    #     ratio_MONTH_QUARTER_arr.append(ratio_MONTH_QUARTER)

    # # ********************************************************
    # df_ratio_MONTH_QUARTER = pd.concat(ratio_MONTH_QUARTER_arr)
    # df_ratio_MONTH_QUARTER = df_ratio_MONTH_QUARTER.query("`MONTH/QUARTER COUNT_` >= 3")
    # df_ratio_MONTH_QUARTER = df_ratio_MONTH_QUARTER.dropna(
    #     subset=["SELLINGDAY_MONTH/QUARTER RATIO_median"]
    # )
    # df_ratio_MONTH_QUARTER["KEY"] = df_ratio_MONTH_QUARTER["KEY"].str.replace(
    #     "NATIONWIDE\|NATIONWIDE\|", ""
    # )

    # df_ratio_MONTH_QUARTER["MONTH/QUARTER ORDER_"] = df_ratio_MONTH_QUARTER["MONTH/QUARTER ORDER_"].astype(int)

    # df_ratio_MONTH_QUARTER.columns = [
    #     "MONTH/QUARTER COUNT",
    #     "MONTH/QUARTER ORDER",
    #     "SELLINGDAY_MONTH/QUARTER RATIO_mean",
    #     "SELLINGDAY_MONTH/QUARTER RATIO_median",
    #     "SELLINGDAY_MONTH/QUARTER RATIO_std",
    #     "KEY_NATIONWIDE",
    # ]

    return df_ratio_WEEK_MONTH, df_ratio_WEEK_QUARTER, _

# COMMAND ----------

def convert_data(df_sec_sales, df_ratio_WEEK_MONTH, df_ratio_WEEK_QUARTER, df_ratio_MONTH_QUARTER, df_pri_sales):
    df_convert = df_sec_sales[
    [
        "KEY",
        "YEARWEEK",
        "ACTUALSALE",
        "BANNER",
        "CATEGORY",
        "SEGMENT",
        "DPNAME",
        "KEY_NATIONWIDE",
        "DTWORKINGDAY",
        "DATE",
        "YEAR",
        "MONTH",
        "QUARTER",
    ]
    ].copy()
    # df_convert = df_convert[df_convert["YEARWEEK"] < 202338]
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

    df_convert["WEEK/QUARTER COUNT"] = (
        df_convert.groupby(["KEY", "YEAR", "QUARTER"])["YEARWEEK"]
        .transform("count")
        .astype(int)
    )
    df_convert["WEEK/QUARTER ORDER"] = (
        df_convert.groupby(["KEY", "YEAR", "QUARTER"])["YEARWEEK"]
        .transform("rank")
        .astype(int)
    )

    df_convert["SEC_SALES_MONTHLY"] = (
        df_convert.groupby(["KEY", "YEAR", "MONTH"])["SEC_SALES"].transform("sum").astype(float)
    )

    # df_convert["MONTH/QUARTER COUNT"] = df_convert.groupby(["KEY", "YEAR", "QUARTER"])[
    #     "MONTH"
    # ].transform(lambda x: x.nunique())
    # df_convert["MONTH/QUARTER ORDER"] = df_convert.groupby(["KEY", "YEAR", "QUARTER"])[
    #     "MONTH"
    # ].rank(method = 'dense')

    # df_convert["WORKINGDAY_MONTH_COUNT"] = df_convert.groupby(["KEY", "YEAR", "MONTH"])["DTWORKINGDAY"].transform("sum").astype(int)

    df_convert = df_convert.sort_values(["KEY", "DATE"]).reset_index(drop=True)


    # ****************************************************
    df_convert = df_convert.merge(
        df_ratio_WEEK_MONTH, on=["WEEK/MONTH COUNT", "WEEK/MONTH ORDER", "KEY_NATIONWIDE"], how="left"
    )

    ratio_WEEK_MONTH_segment = (
        df_convert.groupby(["CATEGORY","WEEK/MONTH COUNT", "WEEK/MONTH ORDER"])
        .agg({"SELLINGDAY_WEEK/MONTH RATIO_median": ["median"]})
        .reset_index()
    )

    ratio_WEEK_MONTH_segment.columns = [
        "_".join(col) for col in ratio_WEEK_MONTH_segment.columns
    ]

    ratio_WEEK_MONTH_segment.columns = [
        "CATEGORY",
        "WEEK/MONTH COUNT",
        "WEEK/MONTH ORDER",
        "SELLINGDAY_WEEK/MONTH RATIO_median",
    ]
    df_convert = df_convert.drop('SELLINGDAY_WEEK/MONTH RATIO_median', axis = 1)

    df_convert = df_convert.merge(ratio_WEEK_MONTH_segment, on = ['CATEGORY','WEEK/MONTH COUNT','WEEK/MONTH ORDER'], how = 'left')


    # **********************************************
    df_convert = df_convert.merge(
        df_ratio_WEEK_QUARTER, on=["WEEK/QUARTER COUNT", "WEEK/QUARTER ORDER", "KEY_NATIONWIDE"], how="left"
    )

    ratio_WEEK_QUARTER_segment = (
        df_convert.groupby(["CATEGORY","WEEK/QUARTER COUNT", "WEEK/QUARTER ORDER"])
        .agg({"SELLINGDAY_WEEK/QUARTER RATIO_median": ["median"]})
        .reset_index()
    )

    ratio_WEEK_QUARTER_segment.columns = [
        "_".join(col) for col in ratio_WEEK_QUARTER_segment.columns
    ]

    ratio_WEEK_QUARTER_segment.columns = [
        "CATEGORY",
        "WEEK/QUARTER COUNT",
        "WEEK/QUARTER ORDER",
        "SELLINGDAY_WEEK/QUARTER RATIO_median",
    ]
    df_convert = df_convert.drop('SELLINGDAY_WEEK/QUARTER RATIO_median', axis = 1)

    df_convert = df_convert.merge(ratio_WEEK_QUARTER_segment, on = ['CATEGORY','WEEK/QUARTER COUNT','WEEK/QUARTER ORDER'], how = 'left')

    # df_convert = df_convert.merge(
    #     df_ratio_MONTH_QUARTER, on=["MONTH/QUARTER COUNT", "MONTH/QUARTER ORDER", "KEY_NATIONWIDE"], how="left"
    # )


    # ****************************************************
    df_convert["RATIO_median_WEEK/MONTH"] = (
        df_convert["SELLINGDAY_WEEK/MONTH RATIO_median"] * df_convert["DTWORKINGDAY"]
    )
    df_convert["Normalized_RATIO_median_WEEK/MONTH"] = df_convert[
        "RATIO_median_WEEK/MONTH"
    ] / df_convert.groupby(["KEY_NATIONWIDE", "WEEK/MONTH COUNT"])[
        "RATIO_median_WEEK/MONTH"
    ].transform(
        lambda x: x.unique().sum()
    ).astype(float)

    df_convert["RATIO_median_WEEK/QUARTER"] = (
        df_convert["SELLINGDAY_WEEK/QUARTER RATIO_median"] * df_convert["DTWORKINGDAY"]
    )
    df_convert["Normalized_RATIO_median_WEEK/QUARTER"] = df_convert[
        "RATIO_median_WEEK/QUARTER"
    ] / df_convert.groupby(["KEY_NATIONWIDE", "WEEK/QUARTER COUNT"])[
        "RATIO_median_WEEK/QUARTER"
    ].transform(
        lambda x: x.unique().sum()
    ).astype(float)

    # df_convert["RATIO_median_MONTH/QUARTER"] = (
    #     df_convert["SELLINGDAY_MONTH/QUARTER RATIO_median"] * df_convert["WORKINGDAY_MONTH_COUNT"]
    # )
    # df_convert["Normalized_RATIO_median_MONTH/QUARTER"] = df_convert[
    #     "RATIO_median_MONTH/QUARTER"
    # ] / df_convert.groupby(["KEY_NATIONWIDE", "MONTH/QUARTER COUNT"])[
    #     "RATIO_median_MONTH/QUARTER"
    # ].transform(
    #     lambda x: x.unique().sum()
    # ).astype(float)


    # ****************************************************
    df_convert["Sec2Pri_SALES_WEEK/MONTH_normalized"] = (
        df_convert.groupby(["KEY", "YEAR", "MONTH"])["SEC_SALES"].transform("sum")
        * df_convert["Normalized_RATIO_median_WEEK/MONTH"]
    )
    df_convert["Sec2Pri_SALES_WEEK/MONTH"] = (
        df_convert.groupby(["KEY", "YEAR", "MONTH"])["SEC_SALES"].transform("sum")
        * df_convert["RATIO_median_WEEK/MONTH"]
    )

    df_convert["Sec2Pri_SALES_WEEK/QUARTER_normalized"] = (
        df_convert.groupby(["KEY", "YEAR", "QUARTER"])["SEC_SALES"].transform("sum")
        * df_convert["Normalized_RATIO_median_WEEK/QUARTER"]
    )
    df_convert["Sec2Pri_SALES_WEEK/QUARTER"] = (
        df_convert.groupby(["KEY", "YEAR", "QUARTER"])["SEC_SALES"].transform("sum")
        * df_convert["RATIO_median_WEEK/QUARTER"]
    )

    # df_convert["Sec2Pri_SALES_MONTH/QUARTER"] = (
    #     df_convert.groupby(["KEY", "YEAR", "QUARTER"])["SEC_SALES_MONTHLY"].transform(lambda x: x.unique().sum())
    #     * df_convert["Normalized_RATIO_median_MONTH/QUARTER"]
    # )
    # df_convert["Sec2Pri_SALES_MONTH/QUARTER_notnormalized"] = (
    #     df_convert.groupby(["KEY", "YEAR", "QUARTER"])["SEC_SALES_MONTHLY"].transform(lambda x: x.unique().sum())
    #     * df_convert["RATIO_median_MONTH/QUARTER"]
    # )


    # ****************************************************
    df_convert = df_convert.merge(
        df_pri_sales[["CATEGORY", "DPNAME", "YEARWEEK", "ACTUALSALE"]],
        how="outer",
        on=["CATEGORY", "DPNAME", "YEARWEEK"],
    )

    df_convert = df_convert.rename(columns={"ACTUALSALE": "PRI_SALES"})

    for feature in [
        "PRI_SALES",
        "SEC_SALES",
        "Sec2Pri_SALES_WEEK/MONTH_normalized",
        "Sec2Pri_SALES_WEEK/MONTH",
        "Sec2Pri_SALES_WEEK/QUARTER_normalized",
        "Sec2Pri_SALES_WEEK/QUARTER",
    ]:
        df_convert[feature] = df_convert[feature].fillna(0)

    df_convert.sort_values(["KEY", "DATE"]).reset_index(drop=True)

    return df_convert

# COMMAND ----------

def accuracy_check(key, df_group, actual_col, predict_col_arr, Ratio=1):

    performance = dict()
    sum_actualsale = df_group[actual_col].sum()

    performance = {"CATEGORY": key, "Sum_actualsale": sum_actualsale}

    for predict_col in predict_col_arr:
        df_group[predict_col] = df_group[predict_col] * Ratio

        error = sum((df_group[actual_col] - df_group[predict_col]).abs())
        accuracy = 1 - error / df_group[actual_col].sum()
        sum_predictsale = df_group[predict_col].sum()

        performance["Sum_predictsale_" + predict_col] = sum_predictsale
        performance["Accuracy_" + predict_col] = accuracy
        performance["Error_" + predict_col] = error

    return performance


actual_col = "PRI_SALES"
predict_col_arr = [
    "Sec2Pri_SALES_WEEK/MONTH_normalized",
    "Sec2Pri_SALES_WEEK/MONTH",
    "Sec2Pri_SALES_WEEK/QUARTER_normalized",
    "Sec2Pri_SALES_WEEK/QUARTER",
]

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Evaluate original assumption

# COMMAND ----------

df_sec_clone = pd.DataFrame(columns=df_sec_sales.columns)
df_pri_clone = pd.DataFrame(columns=df_pri_sales.columns)

common_dp = df_consist_dp["DPNAME"][
    (df_consist_dp["YEARWEEK"] >= 202301) & (df_consist_dp["YEARWEEK"] <= 202335)
].unique()

df_sec_clone = df_sec_sales[df_sec_sales["DPNAME"].isin(common_dp)]
df_sec_clone = df_sec_clone[
    (df_sec_clone["YEARWEEK"] >= 202301) & (df_sec_clone["YEARWEEK"] <= 202335)
]

df_pri_clone = df_pri_sales[df_pri_sales["DPNAME"].isin(common_dp)]
df_pri_clone = df_pri_clone[
    (df_pri_clone["YEARWEEK"] >= 202301) & (df_pri_clone["YEARWEEK"] <= 202335)
]

# COMMAND ----------

df_pri_unclone = df_pri_sales[~df_pri_sales["DPNAME"].isin(common_dp)]
print(df_pri_unclone.shape, df_pri_unclone['DPNAME'].nunique())
df_pri_unclone['CATEGORY'].value_counts(normalize = True)

# COMMAND ----------

# tuning 100 largest total sale DP

# df_sec_clone = pd.DataFrame(columns=df_sec_sales.columns)
# df_pri_clone = pd.DataFrame(columns=df_pri_sales.columns)

# top_dp_largest = (
#     df_consist_dp[
#         (df_consist_dp["YEARWEEK"] >= 202301) & (df_consist_dp["YEARWEEK"] <= 202335)
#     ]
#     .drop_duplicates(subset=["TOTAL_SEC_SALES"])
#     .nlargest(100, columns=["TOTAL_SEC_SALES"])["DPNAME"]
#     .unique()
# )

# df_sec_clone = df_sec_sales[df_sec_sales["DPNAME"].isin(top_dp_largest)]
# df_sec_clone = df_sec_clone[
#     (df_sec_clone["YEARWEEK"] >= 202301) & (df_sec_clone["YEARWEEK"] <= 202335)
# ]

# df_pri_clone = df_pri_sales[df_pri_sales["DPNAME"].isin(top_dp_largest)]
# df_pri_clone = df_pri_clone[
#     (df_pri_clone["YEARWEEK"] >= 202301) & (df_pri_clone["YEARWEEK"] <= 202335)
# ]

# COMMAND ----------

# tuning using 100DP follow percent category

# df_sec_clone = pd.DataFrame(columns=df_sec_sales.columns)
# df_pri_clone = pd.DataFrame(columns=df_pri_sales.columns)

# df_consist_dp_clone = df_consist_dp[
#     (df_consist_dp["YEARWEEK"] >= 202301) & (df_consist_dp["YEARWEEK"] <= 202335)
# ]

# list_cate = (df_consist_dp_clone["CATEGORY"].value_counts(normalize=True) * 100).apply(
#     lambda x: math.ceil(x)
# )

# # if take all category, there is TEA dont have common DP of pri&sec from jan 2023 to august 2023 so Im gonna minus 1 in for loop as the last category in list_cate 
# for i in range(len(list_cate) - 1):
#     df_temp = df_consist_dp_clone[df_consist_dp_clone["CATEGORY"] == list_cate.index[i]]

#     # pick pattern following largest DP with len = percentage of category in data
#     top_dp_largest = (
#         df_temp.drop_duplicates(subset=["TOTAL_SEC_SALES"])
#         .nlargest(list_cate[i], columns=["TOTAL_SEC_SALES"])["DPNAME"]
#         .unique()
#     )

#     df_concat = df_sec_sales[df_sec_sales["DPNAME"].isin(top_dp_largest)]
#     df_sec_clone = pd.concat([df_sec_clone, df_concat])

#     df_concat = df_pri_sales[df_pri_sales["DPNAME"].isin(top_dp_largest)]
#     df_pri_clone = pd.concat([df_pri_clone, df_concat])

# df_sec_clone = df_sec_clone[
#     (df_sec_clone["YEARWEEK"] >= 202301) & (df_sec_clone["YEARWEEK"] <= 202335)
# ]
# df_pri_clone = df_pri_clone[
#     (df_pri_clone["YEARWEEK"] >= 202301) & (df_pri_clone["YEARWEEK"] <= 202335)
# ]

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Phasing all

# COMMAND ----------

df_pri_sales_phasing_all = df_pri_sales[df_pri_sales['YEARWEEK'] < 202301]

df_pri_sales_phasing_all['YEARWEEK'].min(), df_pri_sales_phasing_all['YEARWEEK'].max()

# COMMAND ----------

df_ratio_WEEK_MONTH, df_ratio_WEEK_QUARTER, _ = create_ratio_3phase(df_pri_sales_phasing_all)

# COMMAND ----------

df_ratio_WEEK_MONTH

# COMMAND ----------

df_convert = convert_data(df_sec_clone, df_ratio_WEEK_MONTH, df_ratio_WEEK_QUARTER, _, df_pri_clone)

# COMMAND ----------

# pattern_segment = df_convert[['SEGMENT','DPNAME','YEARWEEK','SEC_SALES','MONTH','SELLINGDAY_WEEK/MONTH RATIO_median','WEEK/MONTH COUNT','WEEK/MONTH ORDER']][df_convert['SEGMENT'] == 'White']
# pattern_segment

# COMMAND ----------

# ratio_WEEK_MONTH_segment = (
#             df_convert.groupby(["SEGMENT","WEEK/MONTH COUNT", "WEEK/MONTH ORDER"])
#             .agg({"SELLINGDAY_WEEK/MONTH RATIO_median": ["median"]})
#             .reset_index()
# )
# ratio_WEEK_MONTH_segment

# ratio_WEEK_MONTH_segment.columns = [
#     "_".join(col) for col in ratio_WEEK_MONTH_segment.columns
# ]

# ratio_WEEK_MONTH_segment.columns = [
#     "SEGMENT",
#     "WEEK/MONTH COUNT",
#     "WEEK/MONTH ORDER",
#     "SELLINGDAY_WEEK/MONTH RATIO_median",
# ]

# pattern_segment = pattern_segment.drop('SELLINGDAY_WEEK/MONTH RATIO_median', axis = 1)

# pattern_segment = pattern_segment.merge(ratio_WEEK_MONTH_segment, on = ['SEGMENT','WEEK/MONTH COUNT','WEEK/MONTH ORDER'], how = 'left')

# COMMAND ----------

# ratio_WEEK_MONTH_segment[ratio_WEEK_MONTH_segment['SEGMENT'] == 'White']

# COMMAND ----------

# df_temp = pattern_segment.merge(ratio_WEEK_MONTH_segment, on = ['SEGMENT','WEEK/MONTH COUNT','WEEK/MONTH ORDER'], how = 'left')
# df_temp[(df_temp['WEEK/MONTH COUNT'] == 5) & (df_temp['WEEK/MONTH ORDER'] == 4)]

# COMMAND ----------

df_accuracy_phase_all = pd.DataFrame.from_dict(
    [
        accuracy_check(key, df_group, actual_col, predict_col_arr, Ratio = 1)
        for key, df_group in df_convert.groupby("CATEGORY")
    ]
)

df_accuracy_phase_all.columns = [
    "CATEGORY",
    "Sum_actualsale",
    "Sum_predictsale_WEEK/MONTH_normalized_PhasingAll",
    "Accuracy_WEEK/MONTH_normalized_PhasingAll",
    "Error_WEEK/MONTH_normalized_PhasingAll",
    "Sum_predictsale_WEEK/MONTH_PhasingAll",
    "Accuracy_WEEK/MONTH_PhasingAll",
    "Error_WEEK/MONTH_PhasingAll",
    "Sum_predictsale_WEEK/QUARTER_normalized_PhasingAll",
    "Accuracy_WEEK/QUARTER_normalized_PhasingAll",
    "Error_WEEK/QUARTER_normalized_PhasingAll",
    "Sum_predictsale_WEEK/QUARTER_PhasingAll",
    "Accuracy_WEEK/QUARTER_PhasingAll",
    "Error_WEEK/QUARTER_PhasingAll"
]

# COMMAND ----------

df_accuracy_phase_all

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Phasing 1Y

# COMMAND ----------

df_pri_sales_phasing_1Y = df_pri_sales[
    (df_pri_sales["YEARWEEK"] < 202301) & (df_pri_sales["YEARWEEK"] >= 202201)
]

df_pri_sales_phasing_1Y["YEARWEEK"].min(), df_pri_sales_phasing_1Y["YEARWEEK"].max()

# COMMAND ----------

df_ratio_WEEK_MONTH, df_ratio_WEEK_QUARTER, df_ratio_MONTH_QUARTER = create_ratio_3phase(df_pri_sales_phasing_1Y)

# COMMAND ----------

df_convert = convert_data(df_sec_clone, df_ratio_WEEK_MONTH, df_ratio_WEEK_QUARTER, df_ratio_MONTH_QUARTER, df_pri_clone)

# COMMAND ----------

df_accuracy_phase_1Y = pd.DataFrame.from_dict(
    [
        accuracy_check(key, df_group, actual_col, predict_col_arr, Ratio = 1)
        for key, df_group in df_convert.groupby("CATEGORY")
    ]
)

df_accuracy_phase_1Y.columns = [
    "CATEGORY",
    "Sum_actualsale",
    "Sum_predictsale_WEEK/MONTH_normalized_Phasing1Y",
    "Accuracy_WEEK/MONTH_normalized_Phasing1Y",
    "Error_WEEK/MONTH_normalized_Phasing1Y",
    "Sum_predictsale_WEEK/MONTH_Phasing1Y",
    "Accuracy_WEEK/MONTH_Phasing1Y",
    "Error_WEEK/MONTH_Phasing1Y",
    "Sum_predictsale_WEEK/QUARTER_normalized_Phasing1Y",
    "Accuracy_WEEK/QUARTER_normalized_Phasing1Y",
    "Error_WEEK/QUARTER_normalized_Phasing1Y",
    "Sum_predictsale_WEEK/QUARTER_Phasing1Y",
    "Accuracy_WEEK/QUARTER_Phasing1Y",
    "Error_WEEK/QUARTER_Phasing1Y"
]

df_accuracy_phase_1Y

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Phasing 2Y

# COMMAND ----------

df_pri_sales_phasing_2Y = df_pri_sales[
    (df_pri_sales["YEARWEEK"] < 202301) & (df_pri_sales["YEARWEEK"] >= 202101)
]

df_pri_sales_phasing_2Y["YEARWEEK"].min(), df_pri_sales_phasing_2Y["YEARWEEK"].max()

# COMMAND ----------

df_ratio_WEEK_MONTH, df_ratio_WEEK_QUARTER, df_ratio_MONTH_QUARTER = create_ratio_3phase(df_pri_sales_phasing_2Y)

# COMMAND ----------

df_convert = convert_data(df_sec_clone, df_ratio_WEEK_MONTH, df_ratio_WEEK_QUARTER, df_ratio_MONTH_QUARTER, df_pri_clone)

# COMMAND ----------

df_accuracy_phase_2Y = pd.DataFrame.from_dict(
    [
        accuracy_check(key, df_group, actual_col, predict_col_arr, Ratio = 1)
        for key, df_group in df_convert.groupby("CATEGORY")
    ]
)

df_accuracy_phase_2Y.columns = [
    "CATEGORY",
    "Sum_actualsale",
    "Sum_predictsale_WEEK/MONTH_normalized_Phasing2Y",
    "Accuracy_WEEK/MONTH_normalized_Phasing2Y",
    "Error_WEEK/MONTH_normalized_Phasing2Y",
    "Sum_predictsale_WEEK/MONTH_Phasing2Y",
    "Accuracy_WEEK/MONTH_Phasing2Y",
    "Error_WEEK/MONTH_Phasing2Y",
    "Sum_predictsale_WEEK/QUARTER_normalized_Phasing2Y",
    "Accuracy_WEEK/QUARTER_normalized_Phasing2Y",
    "Error_WEEK/QUARTER_normalized_Phasing2Y",
    "Sum_predictsale_WEEK/QUARTER_Phasing2Y",
    "Accuracy_WEEK/QUARTER_Phasing2Y",
    "Error_WEEK/QUARTER_Phasing2Y"
]

df_accuracy_phase_2Y

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Phasing 3Y

# COMMAND ----------

df_pri_sales_phasing_3Y = df_pri_sales[
    (df_pri_sales["YEARWEEK"] < 202301) & (df_pri_sales["YEARWEEK"] >= 202001)
]

df_pri_sales_phasing_3Y["YEARWEEK"].min(), df_pri_sales_phasing_3Y["YEARWEEK"].max()

# COMMAND ----------

df_ratio_WEEK_MONTH, df_ratio_WEEK_QUARTER, df_ratio_MONTH_QUARTER = create_ratio_3phase(df_pri_sales_phasing_3Y)

# COMMAND ----------

df_convert = convert_data(df_sec_clone, df_ratio_WEEK_MONTH, df_ratio_WEEK_QUARTER, df_ratio_MONTH_QUARTER, df_pri_clone)

# COMMAND ----------

df_accuracy_phase_3Y = pd.DataFrame.from_dict(
    [
        accuracy_check(key, df_group, actual_col, predict_col_arr, Ratio = 1)
        for key, df_group in df_convert.groupby("CATEGORY")
    ]
)

df_accuracy_phase_3Y.columns = [
    "CATEGORY",
    "Sum_actualsale",
    "Sum_predictsale_WEEK/MONTH_normalized_Phasing3Y",
    "Accuracy_WEEK/MONTH_normalized_Phasing3Y",
    "Error_WEEK/MONTH_normalized_Phasing3Y",
    "Sum_predictsale_WEEK/MONTH_Phasing3Y",
    "Accuracy_WEEK/MONTH_Phasing3Y",
    "Error_WEEK/MONTH_Phasing3Y",
    "Sum_predictsale_WEEK/QUARTER_normalized_Phasing3Y",
    "Accuracy_WEEK/QUARTER_normalized_Phasing3Y",
    "Error_WEEK/QUARTER_normalized_Phasing3Y",
    "Sum_predictsale_WEEK/QUARTER_Phasing3Y",
    "Accuracy_WEEK/QUARTER_Phasing3Y",
    "Error_WEEK/QUARTER_Phasing3Y"
]

df_accuracy_phase_3Y

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Merge Phasing

# COMMAND ----------

df_accuracy_all_data = (
    df_accuracy_phase_all.merge(df_accuracy_phase_1Y, on=["CATEGORY","Sum_actualsale"])
    .merge(df_accuracy_phase_2Y, on=["CATEGORY","Sum_actualsale"])
    .merge(df_accuracy_phase_3Y, on=["CATEGORY", "Sum_actualsale"])
)
print(df_accuracy_all_data.shape) 
df_accuracy_all_data = df_accuracy_all_data.fillna(0)
df_accuracy_all_data

# COMMAND ----------

with pd.ExcelWriter(
    "/Workspace/Users/ng-minh-hoang.dat@unilever.com/Accuracy_convert_8month2023.xlsx"
) as writer:
    df_accuracy_all_data.to_excel(writer, sheet_name = "Total all phase")
    df_accuracy_phase_all.to_excel(writer, sheet_name = "Phasing all (from 2018)")
    df_accuracy_phase_1Y.to_excel(writer, sheet_name = "Phasing 1 Year")
    df_accuracy_phase_2Y.to_excel(writer, sheet_name = "Phasing 2 Year")
    df_accuracy_phase_3Y.to_excel(writer, sheet_name = "Phasing 3 Year")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Tuning using ratio all data

# COMMAND ----------

list_ratio = np.arange(1.01, 1.3, 0.01)
list_ratio = np.append(list_ratio, [1.38, 1.44, 1.47, 1.51, 1.61, 1.67, 1.8, 1.92, 2, 2.2, 2.4, 2.6 , 3])

mean_acc = dict()
weighted_mean_acc = dict()

list_ratio

# COMMAND ----------

for RATIO in list_ratio:
    df_sec_clone = pd.DataFrame(columns=df_sec_sales.columns)
    df_pri_clone = pd.DataFrame(columns=df_pri_sales.columns)

    common_dp = df_consist_dp["DPNAME"][
        (df_consist_dp["YEARWEEK"] >= 202301) & (df_consist_dp["YEARWEEK"] <= 202335)
    ].unique()

    df_sec_clone = df_sec_sales[df_sec_sales["DPNAME"].isin(common_dp)]
    df_sec_clone = df_sec_clone[
        (df_sec_clone["YEARWEEK"] >= 202301) & (df_sec_clone["YEARWEEK"] <= 202335)
    ]

    df_pri_clone = df_pri_sales[df_pri_sales["DPNAME"].isin(common_dp)]
    df_pri_clone = df_pri_clone[
        (df_pri_clone["YEARWEEK"] >= 202301) & (df_pri_clone["YEARWEEK"] <= 202335)
    ]


    ### Phasing all
    df_pri_sales_phasing_all = df_pri_sales[df_pri_sales['YEARWEEK'] < 202301]

    df_ratio_WEEK_MONTH, df_ratio_WEEK_QUARTER, df_ratio_MONTH_QUARTER = create_ratio_3phase(df_pri_sales_phasing_all)

    df_convert = convert_data(df_sec_clone, df_ratio_WEEK_MONTH, df_ratio_WEEK_QUARTER, df_ratio_MONTH_QUARTER, df_pri_clone)

    df_accuracy_phase_all = pd.DataFrame.from_dict(
        [
            accuracy_check(key, df_group, actual_col, predict_col_arr, Ratio = RATIO)
            for key, df_group in df_convert.groupby("CATEGORY")
        ]
    )

    df_accuracy_phase_all.columns = [
        "CATEGORY",
        "Sum_actualsale",
        "Sum_predictsale_WEEK/MONTH_normalized_PhasingAll",
        "Accuracy_WEEK/MONTH_normalized_PhasingAll",
        "Error_WEEK/MONTH_normalized_PhasingAll",
        "Sum_predictsale_WEEK/MONTH_PhasingAll",
        "Accuracy_WEEK/MONTH_PhasingAll",
        "Error_WEEK/MONTH_PhasingAll",
        "Sum_predictsale_WEEK/QUARTER_normalized_PhasingAll",
        "Accuracy_WEEK/QUARTER_normalized_PhasingAll",
        "Error_WEEK/QUARTER_normalized_PhasingAll",
        "Sum_predictsale_WEEK/QUARTER_PhasingAll",
        "Accuracy_WEEK/QUARTER_PhasingAll",
        "Error_WEEK/QUARTER_PhasingAll"
    ]
    ### Phasing 1Y
    df_pri_sales_phasing_1Y = df_pri_sales[
        (df_pri_sales["YEARWEEK"] < 202301) & (df_pri_sales["YEARWEEK"] >= 202201)
    ]

    df_ratio_WEEK_MONTH, df_ratio_WEEK_QUARTER, df_ratio_MONTH_QUARTER = create_ratio_3phase(df_pri_sales_phasing_1Y)

    df_convert = convert_data(df_sec_clone, df_ratio_WEEK_MONTH, df_ratio_WEEK_QUARTER, df_ratio_MONTH_QUARTER, df_pri_clone)

    df_accuracy_phase_1Y = pd.DataFrame.from_dict(
        [
            accuracy_check(key, df_group, actual_col, predict_col_arr, Ratio = RATIO)
            for key, df_group in df_convert.groupby("CATEGORY")
        ]
    )

    df_accuracy_phase_1Y.columns = [
        "CATEGORY",
        "Sum_actualsale",
        "Sum_predictsale_WEEK/MONTH_normalized_Phasing1Y",
        "Accuracy_WEEK/MONTH_normalized_Phasing1Y",
        "Error_WEEK/MONTH_normalized_Phasing1Y",
        "Sum_predictsale_WEEK/MONTH_Phasing1Y",
        "Accuracy_WEEK/MONTH_Phasing1Y",
        "Error_WEEK/MONTH_Phasing1Y",
        "Sum_predictsale_WEEK/QUARTER_normalized_Phasing1Y",
        "Accuracy_WEEK/QUARTER_normalized_Phasing1Y",
        "Error_WEEK/QUARTER_normalized_Phasing1Y",
        "Sum_predictsale_WEEK/QUARTER_Phasing1Y",
        "Accuracy_WEEK/QUARTER_Phasing1Y",
        "Error_WEEK/QUARTER_Phasing1Y"
    ]
    ### Phasing 2Y
    df_pri_sales_phasing_2Y = df_pri_sales[
        (df_pri_sales["YEARWEEK"] < 202301) & (df_pri_sales["YEARWEEK"] >= 202101)
    ]

    df_ratio_WEEK_MONTH, df_ratio_WEEK_QUARTER, df_ratio_MONTH_QUARTER = create_ratio_3phase(df_pri_sales_phasing_2Y)

    df_convert = convert_data(df_sec_clone, df_ratio_WEEK_MONTH, df_ratio_WEEK_QUARTER, df_ratio_MONTH_QUARTER, df_pri_clone)

    df_accuracy_phase_2Y = pd.DataFrame.from_dict(
        [
            accuracy_check(key, df_group, actual_col, predict_col_arr, Ratio = RATIO)
            for key, df_group in df_convert.groupby("CATEGORY")
        ]
    )

    df_accuracy_phase_2Y.columns = [
        "CATEGORY",
        "Sum_actualsale",
        "Sum_predictsale_WEEK/MONTH_normalized_Phasing2Y",
        "Accuracy_WEEK/MONTH_normalized_Phasing2Y",
        "Error_WEEK/MONTH_normalized_Phasing2Y",
        "Sum_predictsale_WEEK/MONTH_Phasing2Y",
        "Accuracy_WEEK/MONTH_Phasing2Y",
        "Error_WEEK/MONTH_Phasing2Y",
        "Sum_predictsale_WEEK/QUARTER_normalized_Phasing2Y",
        "Accuracy_WEEK/QUARTER_normalized_Phasing2Y",
        "Error_WEEK/QUARTER_normalized_Phasing2Y",
        "Sum_predictsale_WEEK/QUARTER_Phasing2Y",
        "Accuracy_WEEK/QUARTER_Phasing2Y",
        "Error_WEEK/QUARTER_Phasing2Y"
    ]
    ### Phasing 3Y
    df_pri_sales_phasing_3Y = df_pri_sales[
        (df_pri_sales["YEARWEEK"] < 202301) & (df_pri_sales["YEARWEEK"] >= 202001)
    ]

    df_ratio_WEEK_MONTH, df_ratio_WEEK_QUARTER, df_ratio_MONTH_QUARTER = create_ratio_3phase(df_pri_sales_phasing_3Y)

    df_convert = convert_data(df_sec_clone, df_ratio_WEEK_MONTH, df_ratio_WEEK_QUARTER, df_ratio_MONTH_QUARTER, df_pri_clone)

    df_accuracy_phase_3Y = pd.DataFrame.from_dict(
        [
            accuracy_check(key, df_group, actual_col, predict_col_arr, Ratio = RATIO)
            for key, df_group in df_convert.groupby("CATEGORY")
        ]
    )

    df_accuracy_phase_3Y.columns = [
        "CATEGORY",
        "Sum_actualsale",
        "Sum_predictsale_WEEK/MONTH_normalized_Phasing3Y",
        "Accuracy_WEEK/MONTH_normalized_Phasing3Y",
        "Error_WEEK/MONTH_normalized_Phasing3Y",
        "Sum_predictsale_WEEK/MONTH_Phasing3Y",
        "Accuracy_WEEK/MONTH_Phasing3Y",
        "Error_WEEK/MONTH_Phasing3Y",
        "Sum_predictsale_WEEK/QUARTER_normalized_Phasing3Y",
        "Accuracy_WEEK/QUARTER_normalized_Phasing3Y",
        "Error_WEEK/QUARTER_normalized_Phasing3Y",
        "Sum_predictsale_WEEK/QUARTER_Phasing3Y",
        "Accuracy_WEEK/QUARTER_Phasing3Y",
        "Error_WEEK/QUARTER_Phasing3Y"
    ]
    df_accuracy_all_data = (
        df_accuracy_phase_all.merge(df_accuracy_phase_1Y, on=["CATEGORY","Sum_actualsale"])
        .merge(df_accuracy_phase_2Y, on=["CATEGORY","Sum_actualsale"])
        .merge(df_accuracy_phase_3Y, on=["CATEGORY", "Sum_actualsale"])
    )

    df_accuracy_all_data = df_accuracy_all_data.fillna(0)
    df_acc = df_accuracy_all_data[
        [
            "CATEGORY",
            "Sum_actualsale",
            "Accuracy_WEEK/MONTH_normalized_PhasingAll",
            "Accuracy_WEEK/MONTH_PhasingAll",
            "Accuracy_WEEK/QUARTER_normalized_PhasingAll",
            "Accuracy_WEEK/QUARTER_PhasingAll",
            "Accuracy_WEEK/MONTH_normalized_Phasing1Y",
            "Accuracy_WEEK/MONTH_Phasing1Y",
            "Accuracy_WEEK/QUARTER_normalized_Phasing1Y",
            "Accuracy_WEEK/QUARTER_Phasing1Y",
            "Accuracy_WEEK/MONTH_normalized_Phasing2Y",
            "Accuracy_WEEK/MONTH_Phasing2Y",
            "Accuracy_WEEK/QUARTER_normalized_Phasing2Y",
            "Accuracy_WEEK/QUARTER_Phasing2Y",
            "Accuracy_WEEK/MONTH_normalized_Phasing3Y",
            "Accuracy_WEEK/MONTH_Phasing3Y",
            "Accuracy_WEEK/QUARTER_normalized_Phasing3Y",
            "Accuracy_WEEK/QUARTER_Phasing3Y",
        ]
    ]

    sum_totalsale = df_acc['Sum_actualsale'].sum()
    weighted_mean_acc_cate = []
    for i in range(2, df_acc.shape[1]):
        weighted_mean_acc_cate.append((df_acc.iloc[1:12, i ] * df_acc.iloc[:, 1] / sum_totalsale).sum().round(2))

    mean_acc[RATIO] = df_acc.iloc[1:12, 2:].mean().values.round(2)
    weighted_mean_acc[RATIO] = weighted_mean_acc_cate
    
    print("RATIO =",RATIO,
          "\nMean accuracy", mean_acc[RATIO],
          "\nWeighted mean accuracy",weighted_mean_acc[RATIO]
    )

# COMMAND ----------

df_tuning_acc = pd.DataFrame(data = {'Accuracy_type': [
    "Accuracy_WEEK/MONTH_normalized_PhasingAll",
    "Accuracy_WEEK/MONTH_PhasingAll",
    "Accuracy_WEEK/QUARTER_normalized_PhasingAll",
    "Accuracy_WEEK/QUARTER_PhasingAll",
    "Accuracy_WEEK/MONTH_normalized_Phasing1Y",
    "Accuracy_WEEK/MONTH_Phasing1Y",
    "Accuracy_WEEK/QUARTER_normalized_Phasing1Y",
    "Accuracy_WEEK/QUARTER_Phasing1Y",
    "Accuracy_WEEK/MONTH_normalized_Phasing2Y",
    "Accuracy_WEEK/MONTH_Phasing2Y",
    "Accuracy_WEEK/QUARTER_normalized_Phasing2Y",
    "Accuracy_WEEK/QUARTER_Phasing2Y",
    "Accuracy_WEEK/MONTH_normalized_Phasing3Y",
    "Accuracy_WEEK/MONTH_Phasing3Y",
    "Accuracy_WEEK/QUARTER_normalized_Phasing3Y",
    "Accuracy_WEEK/QUARTER_Phasing3Y",
]
})

for key, values in weighted_mean_acc.items():
    name = str(key) + '_Mean_Accuracy'
    df_tuning_acc[name] = mean_acc[key]
    name = str(key) + '_Weighted_Mean_Accuracy'
    df_tuning_acc[name] = weighted_mean_acc[key]

df_tuning_acc

# COMMAND ----------

df_tuning_acc.to_excel("/Workspace/Users/ng-minh-hoang.dat@unilever.com/Tuning_Accuracy_alldata.xlsx")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Tuning using ratio each category

# COMMAND ----------

# DEO
# FABSEN
# FABSOL
# FOODSOL
# HAIR
# HNH
# IC
# ORAL
# SAVOURY
# SKINCARE
# SKINCLEANSING
# TBRUSH
# TEA

list_ratio = [
    [1, 1.08, 1.01, 1, 1.04, 1.01, 1.01, 1.03, 1.01, 1.1, 1.04, 1.01, 1], 
    [1, 4.54, 1.04, 1.06, 1.17, 1.02, 1.03, 1.1, 1.06, 1.18, 1.11, 1.05, 1],
    [1, 1.01, 1.04, 1, 1.07, 1.04, 1.08, 1.05, 1.05, 3.71, 1.07, 1.04, 1],
    [1, 1.14, 1.03, 1, 1.07, 1.03, 1.04, 1.06, 1.03, 1.16, 1.07, 1.02, 1],
    [1, 6.16, 5.06, 1.08, 8, 5.56, 1.03, 3.23, 1.06, 1.27, 7, 1.05, 1],
    [1, 1,28, 1.03, 1, 1.16, 1.03, 1.03, 1.11, 1.03, 1.15, 1.13, 1.02, 1],
    [1, 1.17, 1.01, 1, 1.02, 1.01, 1.01, 1.03, 1.02, 1.04, 1.03, 1, 1],
    [1, 1.17, 1.01, 1, 1.01, 1.02, 1, 1.03, 1.02, 1.06, 1.03, 1, 1],
    [1, 3,86, 1.03, 1, 1.2, 1.02, 1.01, 1.06, 1.03, 1.12, 1.12, 1.03, 1],
    [1, 1.2, 1.02, 1, 1.1, 1.02, 1, 1.06, 1.02, 1.09, 1.12, 1.02, 1],
    [1, 1, 1.02, 1, 1.04, 1.02, 1, 1.03, 1.02, 1.14, 1.02, 1.04, 1],
    [1, 1.3, 1.02, 1, 1.05, 1.03, 1, 1.02, 1.02, 1.13, 1.01, 1.04, 1],
    [1, 8.2, 1.06, 1.16, 1.2, 1.02, 1.02, 1.1, 1.08, 1.23, 1.08, 1.08, 1],
    [1, 1.9, 1.03, 1, 1.2, 1.01, 1, 1.1, 1.07, 1.17, 1.09, 1.04, 1],
    [1, 1, 1, 1, 1.01, 1.02, 1, 1.05, 1.03, 1.16, 1.03, 1.01, 1],
    [1, 1, 1, 1, 1.07, 1.02, 1, 1.12, 1.07, 1.22, 1.07, 1, 1]
]
mean_acc = dict()
weighted_mean_acc = dict()

# COMMAND ----------

for idx, RATIO in enumerate(list_ratio):
    df_sec_clone = pd.DataFrame(columns=df_sec_sales.columns)
    df_pri_clone = pd.DataFrame(columns=df_pri_sales.columns)

    common_dp = df_consist_dp["DPNAME"][
        (df_consist_dp["YEARWEEK"] >= 202301) & (df_consist_dp["YEARWEEK"] <= 202335)
    ].unique()

    df_sec_clone = df_sec_sales[df_sec_sales["DPNAME"].isin(common_dp)]
    df_sec_clone = df_sec_clone[
        (df_sec_clone["YEARWEEK"] >= 202301) & (df_sec_clone["YEARWEEK"] <= 202335)
    ]

    df_pri_clone = df_pri_sales[df_pri_sales["DPNAME"].isin(common_dp)]
    df_pri_clone = df_pri_clone[
        (df_pri_clone["YEARWEEK"] >= 202301) & (df_pri_clone["YEARWEEK"] <= 202335)
    ]


    ### Phasing all
    df_pri_sales_phasing_all = df_pri_sales[df_pri_sales['YEARWEEK'] < 202301]

    df_ratio_WEEK_MONTH, df_ratio_WEEK_QUARTER, df_ratio_MONTH_QUARTER = create_ratio_3phase(df_pri_sales_phasing_all)

    df_convert = convert_data(df_sec_clone, df_ratio_WEEK_MONTH, df_ratio_WEEK_QUARTER, df_ratio_MONTH_QUARTER, df_pri_clone)

    df_accuracy_phase_all = pd.DataFrame.from_dict(
        [
            accuracy_check(key, df_group, actual_col, predict_col_arr, Ratio = RATIO[i])
            for i, (key, df_group) in enumerate(df_convert.groupby("CATEGORY"))
        ]
    )

    df_accuracy_phase_all.columns = [
        "CATEGORY",
        "Sum_actualsale",
        "Sum_predictsale_WEEK/MONTH_normalized_PhasingAll",
        "Accuracy_WEEK/MONTH_normalized_PhasingAll",
        "Error_WEEK/MONTH_normalized_PhasingAll",
        "Sum_predictsale_WEEK/MONTH_PhasingAll",
        "Accuracy_WEEK/MONTH_PhasingAll",
        "Error_WEEK/MONTH_PhasingAll",
        "Sum_predictsale_WEEK/QUARTER_normalized_PhasingAll",
        "Accuracy_WEEK/QUARTER_normalized_PhasingAll",
        "Error_WEEK/QUARTER_normalized_PhasingAll",
        "Sum_predictsale_WEEK/QUARTER_PhasingAll",
        "Accuracy_WEEK/QUARTER_PhasingAll",
        "Error_WEEK/QUARTER_PhasingAll"
    ]
    ### Phasing 1Y
    df_pri_sales_phasing_1Y = df_pri_sales[
        (df_pri_sales["YEARWEEK"] < 202301) & (df_pri_sales["YEARWEEK"] >= 202201)
    ]

    df_ratio_WEEK_MONTH, df_ratio_WEEK_QUARTER, df_ratio_MONTH_QUARTER = create_ratio_3phase(df_pri_sales_phasing_1Y)

    df_convert = convert_data(df_sec_clone, df_ratio_WEEK_MONTH, df_ratio_WEEK_QUARTER, df_ratio_MONTH_QUARTER, df_pri_clone)

    df_accuracy_phase_1Y = pd.DataFrame.from_dict(
        [
            accuracy_check(key, df_group, actual_col, predict_col_arr, Ratio = RATIO[i])
            for i, (key, df_group) in enumerate(df_convert.groupby("CATEGORY"))
        ]
    )

    df_accuracy_phase_1Y.columns = [
        "CATEGORY",
        "Sum_actualsale",
        "Sum_predictsale_WEEK/MONTH_normalized_Phasing1Y",
        "Accuracy_WEEK/MONTH_normalized_Phasing1Y",
        "Error_WEEK/MONTH_normalized_Phasing1Y",
        "Sum_predictsale_WEEK/MONTH_Phasing1Y",
        "Accuracy_WEEK/MONTH_Phasing1Y",
        "Error_WEEK/MONTH_Phasing1Y",
        "Sum_predictsale_WEEK/QUARTER_normalized_Phasing1Y",
        "Accuracy_WEEK/QUARTER_normalized_Phasing1Y",
        "Error_WEEK/QUARTER_normalized_Phasing1Y",
        "Sum_predictsale_WEEK/QUARTER_Phasing1Y",
        "Accuracy_WEEK/QUARTER_Phasing1Y",
        "Error_WEEK/QUARTER_Phasing1Y"
    ]
    ### Phasing 2Y
    df_pri_sales_phasing_2Y = df_pri_sales[
        (df_pri_sales["YEARWEEK"] < 202301) & (df_pri_sales["YEARWEEK"] >= 202101)
    ]

    df_ratio_WEEK_MONTH, df_ratio_WEEK_QUARTER, df_ratio_MONTH_QUARTER = create_ratio_3phase(df_pri_sales_phasing_2Y)

    df_convert = convert_data(df_sec_clone, df_ratio_WEEK_MONTH, df_ratio_WEEK_QUARTER, df_ratio_MONTH_QUARTER, df_pri_clone)

    df_accuracy_phase_2Y = pd.DataFrame.from_dict(
        [
            accuracy_check(key, df_group, actual_col, predict_col_arr, Ratio = RATIO[i])
            for i, (key, df_group) in enumerate(df_convert.groupby("CATEGORY"))
        ]
    )

    df_accuracy_phase_2Y.columns = [
        "CATEGORY",
        "Sum_actualsale",
        "Sum_predictsale_WEEK/MONTH_normalized_Phasing2Y",
        "Accuracy_WEEK/MONTH_normalized_Phasing2Y",
        "Error_WEEK/MONTH_normalized_Phasing2Y",
        "Sum_predictsale_WEEK/MONTH_Phasing2Y",
        "Accuracy_WEEK/MONTH_Phasing2Y",
        "Error_WEEK/MONTH_Phasing2Y",
        "Sum_predictsale_WEEK/QUARTER_normalized_Phasing2Y",
        "Accuracy_WEEK/QUARTER_normalized_Phasing2Y",
        "Error_WEEK/QUARTER_normalized_Phasing2Y",
        "Sum_predictsale_WEEK/QUARTER_Phasing2Y",
        "Accuracy_WEEK/QUARTER_Phasing2Y",
        "Error_WEEK/QUARTER_Phasing2Y"
    ]
    ### Phasing 3Y
    df_pri_sales_phasing_3Y = df_pri_sales[
        (df_pri_sales["YEARWEEK"] < 202301) & (df_pri_sales["YEARWEEK"] >= 202001)
    ]

    df_ratio_WEEK_MONTH, df_ratio_WEEK_QUARTER, df_ratio_MONTH_QUARTER = create_ratio_3phase(df_pri_sales_phasing_3Y)

    df_convert = convert_data(df_sec_clone, df_ratio_WEEK_MONTH, df_ratio_WEEK_QUARTER, df_ratio_MONTH_QUARTER, df_pri_clone)

    df_accuracy_phase_3Y = pd.DataFrame.from_dict(
        [
            accuracy_check(key, df_group, actual_col, predict_col_arr, Ratio = RATIO[i])
            for i, (key, df_group) in enumerate(df_convert.groupby("CATEGORY"))
        ]
    )

    df_accuracy_phase_3Y.columns = [
        "CATEGORY",
        "Sum_actualsale",
        "Sum_predictsale_WEEK/MONTH_normalized_Phasing3Y",
        "Accuracy_WEEK/MONTH_normalized_Phasing3Y",
        "Error_WEEK/MONTH_normalized_Phasing3Y",
        "Sum_predictsale_WEEK/MONTH_Phasing3Y",
        "Accuracy_WEEK/MONTH_Phasing3Y",
        "Error_WEEK/MONTH_Phasing3Y",
        "Sum_predictsale_WEEK/QUARTER_normalized_Phasing3Y",
        "Accuracy_WEEK/QUARTER_normalized_Phasing3Y",
        "Error_WEEK/QUARTER_normalized_Phasing3Y",
        "Sum_predictsale_WEEK/QUARTER_Phasing3Y",
        "Accuracy_WEEK/QUARTER_Phasing3Y",
        "Error_WEEK/QUARTER_Phasing3Y"
    ]
    df_accuracy_all_data = (
        df_accuracy_phase_all.merge(df_accuracy_phase_1Y, on=["CATEGORY","Sum_actualsale"])
        .merge(df_accuracy_phase_2Y, on=["CATEGORY","Sum_actualsale"])
        .merge(df_accuracy_phase_3Y, on=["CATEGORY", "Sum_actualsale"])
    )

    df_accuracy_all_data = df_accuracy_all_data.fillna(0)
    df_acc = df_accuracy_all_data[
        [
            "CATEGORY",
            "Sum_actualsale",
            "Accuracy_WEEK/MONTH_normalized_PhasingAll",
            "Accuracy_WEEK/MONTH_PhasingAll",
            "Accuracy_WEEK/QUARTER_normalized_PhasingAll",
            "Accuracy_WEEK/QUARTER_PhasingAll",
            "Accuracy_WEEK/MONTH_normalized_Phasing1Y",
            "Accuracy_WEEK/MONTH_Phasing1Y",
            "Accuracy_WEEK/QUARTER_normalized_Phasing1Y",
            "Accuracy_WEEK/QUARTER_Phasing1Y",
            "Accuracy_WEEK/MONTH_normalized_Phasing2Y",
            "Accuracy_WEEK/MONTH_Phasing2Y",
            "Accuracy_WEEK/QUARTER_normalized_Phasing2Y",
            "Accuracy_WEEK/QUARTER_Phasing2Y",
            "Accuracy_WEEK/MONTH_normalized_Phasing3Y",
            "Accuracy_WEEK/MONTH_Phasing3Y",
            "Accuracy_WEEK/QUARTER_normalized_Phasing3Y",
            "Accuracy_WEEK/QUARTER_Phasing3Y",
        ]
    ]
    sum_totalsale = df_acc['Sum_actualsale'].sum()
    weighted_mean_acc_cate = []
    for i in range(2, df_acc.shape[1]):
        weighted_mean_acc_cate.append((df_acc.iloc[1:12, i ] * df_acc.iloc[:, 1] / sum_totalsale).sum().round(2))

    mean_acc[idx] = df_acc.iloc[1:12, 2:].mean().values.round(2)
    weighted_mean_acc[idx] = weighted_mean_acc_cate
    
    print("RATIO =",RATIO,
          "\nMean accuracy", mean_acc[idx],
          "\nWeighted mean accuracy",weighted_mean_acc[idx]
    )

# COMMAND ----------

df_tuning_acc = pd.DataFrame(data = {'Accuracy_type': [
    "Accuracy_WEEK/MONTH_normalized_PhasingAll",
    "Accuracy_WEEK/MONTH_PhasingAll",
    "Accuracy_WEEK/QUARTER_normalized_PhasingAll",
    "Accuracy_WEEK/QUARTER_PhasingAll",
    "Accuracy_WEEK/MONTH_normalized_Phasing1Y",
    "Accuracy_WEEK/MONTH_Phasing1Y",
    "Accuracy_WEEK/QUARTER_normalized_Phasing1Y",
    "Accuracy_WEEK/QUARTER_Phasing1Y",
    "Accuracy_WEEK/MONTH_normalized_Phasing2Y",
    "Accuracy_WEEK/MONTH_Phasing2Y",
    "Accuracy_WEEK/QUARTER_normalized_Phasing2Y",
    "Accuracy_WEEK/QUARTER_Phasing2Y",
    "Accuracy_WEEK/MONTH_normalized_Phasing3Y",
    "Accuracy_WEEK/MONTH_Phasing3Y",
    "Accuracy_WEEK/QUARTER_normalized_Phasing3Y",
    "Accuracy_WEEK/QUARTER_Phasing3Y",
]
})

for key, values in weighted_mean_acc.items():
    name = str(key) + '_Mean_Accuracy'
    df_tuning_acc[name] = mean_acc[key]
    name = str(key) + '_Weighted_Mean_Accuracy'
    df_tuning_acc[name] = weighted_mean_acc[key]

df_tuning_acc

# COMMAND ----------

df_tuning_acc.to_excel("/Workspace/Users/ng-minh-hoang.dat@unilever.com/Tuning_Accuracy_mean_category.xlsx")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Evaluate Monthly 1/2023 - 8/2023 phasing 2Y

# COMMAND ----------

for i in range(1, 9):
    lower = df_pri_sales["YEARWEEK"][
        (df_pri_sales["YEAR"] >= 2020) & (df_pri_sales["MONTH"] == i)
    ].min()
    if i == 1:
        upper = df_pri_sales["YEARWEEK"][df_pri_sales["YEAR"] < 2023].max()
    else:
        upper = df_pri_sales["YEARWEEK"][
            (df_pri_sales["YEAR"] <= 2023) & (df_pri_sales["MONTH"] == (i - 1))
        ].max()
    YW_predict_upper = df_pri_sales["YEARWEEK"][
        (df_pri_sales["YEAR"] == 2023) & (df_pri_sales["MONTH"] == i)
    ].max()
    YW_predict_lower = df_pri_sales["YEARWEEK"][
        (df_pri_sales["YEAR"] == 2023) & (df_pri_sales["MONTH"] == i)
    ].min()
    print('Month',i,'Range training:', lower, upper)
    print('         Range predict:',YW_predict_lower, YW_predict_upper)

# COMMAND ----------

RATIO_cate = [1, 4.54, 1.04, 1.06, 1.17, 1.02, 1.03, 1.1, 1.06, 1.18, 1.11, 1.05, 1]

# COMMAND ----------

df_accuracy_monthly2023 = pd.DataFrame(columns=["CATEGORY"])

for month_idx in range(1, 9):
    df_sec_clone = pd.DataFrame(columns=df_sec_sales.columns)
    df_pri_clone = pd.DataFrame(columns=df_pri_sales.columns)

    YW_predict_upper = df_pri_sales["YEARWEEK"][
        (df_pri_sales["YEAR"] == 2023) & (df_pri_sales["MONTH"] == month_idx)
    ].max()
    YW_predict_lower = df_pri_sales["YEARWEEK"][
        (df_pri_sales["YEAR"] == 2023) & (df_pri_sales["MONTH"] == month_idx)
    ].min()

    common_dp = df_consist_dp["DPNAME"][
        (df_consist_dp["YEARWEEK"] >= 202301) & (df_consist_dp["YEARWEEK"] <= 202335)
    ].unique()

    df_sec_clone = df_sec_sales[df_sec_sales["DPNAME"].isin(common_dp)]
    df_sec_clone = df_sec_clone[
        (df_sec_clone["YEARWEEK"] >= YW_predict_lower)
        & (df_sec_clone["YEARWEEK"] <= YW_predict_upper)
    ]

    df_pri_clone = df_pri_sales[df_pri_sales["DPNAME"].isin(common_dp)]
    df_pri_clone = df_pri_clone[
        (df_pri_clone["YEARWEEK"] >= YW_predict_lower)
        & (df_pri_clone["YEARWEEK"] <= YW_predict_upper)
    ]
    ### Phasing 2Y
    lower = df_pri_sales["YEARWEEK"][
        (df_pri_sales["YEAR"] >= 2020) & 
        (df_pri_sales["MONTH"] == month_idx)
    ].min()

    if month_idx == 1:
        upper = df_pri_sales["YEARWEEK"][df_pri_sales["YEAR"] < 2023].max()
    else:
        upper = df_pri_sales["YEARWEEK"][
            (df_pri_sales["YEAR"] <= 2023) & (df_pri_sales["MONTH"] == (month_idx - 1))
        ].max()

    df_pri_sales_phasing_2Y = df_pri_sales[
        (df_pri_sales["YEARWEEK"] >= lower) & (df_pri_sales["YEARWEEK"] <= upper)
    ]

    (
        df_ratio_WEEK_MONTH,
        df_ratio_WEEK_QUARTER,
        _,
    ) = create_ratio_3phase(df_pri_sales_phasing_2Y)

    df_convert = convert_data(
        df_sec_clone,
        df_ratio_WEEK_MONTH,
        df_ratio_WEEK_QUARTER,
        _,
        df_pri_clone,
    )

    df_accuracy_phase_2Y = pd.DataFrame.from_dict(
        [
            accuracy_check(key, df_group, actual_col, predict_col_arr, Ratio= 1)
            for ratio_idx, (key, df_group) in enumerate( df_convert.groupby("CATEGORY"))
        ]
    )

    df_accuracy_phase_2Y = df_accuracy_phase_2Y[
        [
            "CATEGORY",
            "Sum_actualsale",
            "Sum_predictsale_Sec2Pri_SALES_WEEK/MONTH",
            "Accuracy_Sec2Pri_SALES_WEEK/MONTH",
            "Error_Sec2Pri_SALES_WEEK/MONTH"
        ]
    ]

    df_accuracy_phase_2Y = df_accuracy_phase_2Y.rename(
        columns={
            "CATEGORY": "CATEGORY",
            "Sum_actualsale": "sum_actualsale_month_" + str(month_idx),
            "Sum_predictsale_Sec2Pri_SALES_WEEK/MONTH": "sum_predictsale_month_"
            + str(month_idx),
            "Accuracy_Sec2Pri_SALES_WEEK/MONTH": "accuracy_month_" + str(month_idx),
            "Error_Sec2Pri_SALES_WEEK/MONTH": "error_month_" + str(month_idx),
        }
    )

    df_accuracy_monthly2023 = df_accuracy_monthly2023.merge(
        df_accuracy_phase_2Y, on=["CATEGORY"], how="outer"
    )

df_accuracy_monthly2023

# COMMAND ----------

df_accuracy_monthly2023.replace([-np.inf, np.inf], 0, inplace = True)
df_accuracy_monthly2023.fillna(0, inplace = True)

df_accuracy_monthly2023[[
    'CATEGORY',
    'accuracy_month_1',
    'accuracy_month_2',
    'accuracy_month_3',
    'accuracy_month_4',
    'accuracy_month_5',
    'accuracy_month_6',
    'accuracy_month_7',
    'accuracy_month_8',
]]

# COMMAND ----------

df_accuracy_monthly2023.to_excel("/Workspace/Users/ng-minh-hoang.dat@unilever.com/Accuracy_Convert_Monthly_2023_phasing2Y.xlsx")

# COMMAND ----------

df_ratio_error = pd.DataFrame(columns=np.arange(0, 8))
list_cate = []

for key, df_group in tqdm(df_consist_dp.groupby("CATEGORY")):
    df_group = df_group[
        (df_group["YEARWEEK"] >= 202301) & (df_group["YEARWEEK"] <= 202335)
    ]
    list_cate.append(key)

    df_group["RATIO_MONTHLY"] = df_group["RATIO_MONTHLY"].replace(
        [np.inf, -np.inf], np.nan
    )

    ratio_error = (
        df_group.groupby(["YEAR", "MONTH"])
        .agg({"RATIO_MONTHLY": lambda x: x.mean(skipna=True)})
        .reset_index(drop=True)
    )

    df_ratio_error = pd.concat([df_ratio_error, ratio_error.T])


df_ratio_error["CATEGORY"] = list_cate
df_ratio_error = df_ratio_error.reset_index(drop=True)

for feature in df_ratio_error.columns:
    if feature != "CATEGORY":
        df_ratio_error = df_ratio_error.rename(
            columns={feature: "Month_" + str(feature + 1)}
        )
cols = df_ratio_error.columns.tolist()
cols = cols[-1:] + cols[:-1]
df_ratio_error = df_ratio_error[cols]

# COMMAND ----------

df_ratio_error

# COMMAND ----------

df_temp = df_consist_dp.copy()
df_temp = df_temp[(df_temp['YEARWEEK'] >= 202301) & (df_temp['YEARWEEK'] <= 202335)]
print(df_temp.shape)
print(df_temp[(df_temp['PRI_SALES'] > 0) & (df_temp['SEC_SALES'] == 0)].shape[0] / df_temp.shape[0])
print(df_temp[(df_temp['PRI_SALES'] == 0) & (df_temp['SEC_SALES'] == 0)].shape[0] / df_temp.shape[0])
print(df_temp[(df_temp['PRI_SALES'] == 0) & (df_temp['SEC_SALES'] > 0)].shape[0] / df_temp.shape[0])

# COMMAND ----------

df_consist_dp["WEEK COUNTS"] = (
    df_consist_dp.groupby(["DPNAME","YEAR", "MONTH"])["YEARWEEK"].transform("count").astype(int)
)
df_consist_dp[['YEAR','MONTH','WEEK COUNTS']][df_consist_dp['WEEK COUNTS'] == 5].drop_duplicates().sort_values(['YEAR','MONTH'])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Result analyze each DP

# COMMAND ----------

def accuracy_check_dp(key, df_group, actual_col, predict_col_arr, Ratio=1):
    # just change key CATEGORY to DPNAME
    performance = dict()
    sum_actualsale = df_group[actual_col].sum()

    performance = {"DPNAME": key, "Sum_actualsale": sum_actualsale}

    for predict_col in predict_col_arr:
        df_group[predict_col] = df_group[predict_col] * Ratio

        error = sum((df_group[actual_col] - df_group[predict_col]).abs())
        accuracy = 1 - error / df_group[actual_col].sum()
        sum_predictsale = df_group[predict_col].sum()

        performance["Sum_predictsale_" + predict_col] = sum_predictsale
        performance["Accuracy_" + predict_col] = accuracy
        performance["Error_" + predict_col] = error

    return performance


actual_col = "PRI_SALES"
predict_col_arr = [
    "Sec2Pri_SALES_WEEK/MONTH_normalized",
    "Sec2Pri_SALES_WEEK/MONTH",
    "Sec2Pri_SALES_WEEK/QUARTER_normalized",
    "Sec2Pri_SALES_WEEK/QUARTER",
]

# COMMAND ----------

with pd.ExcelWriter(
    "/Workspace/Users/ng-minh-hoang.dat@unilever.com/Accuracy_convert_DPNAME.xlsx",
) as writer:
    for month_idx in range(1, 9):
        df_sec_clone = pd.DataFrame(columns=df_sec_sales.columns)
        df_pri_clone = pd.DataFrame(columns=df_pri_sales.columns)

        YW_predict_upper = df_pri_sales["YEARWEEK"][
            (df_pri_sales["YEAR"] == 2023) & (df_pri_sales["MONTH"] == month_idx)
        ].max()
        YW_predict_lower = df_pri_sales["YEARWEEK"][
            (df_pri_sales["YEAR"] == 2023) & (df_pri_sales["MONTH"] == month_idx)
        ].min()

        common_dp = df_consist_dp["DPNAME"][
            (df_consist_dp["YEARWEEK"] >= 202301)
            & (df_consist_dp["YEARWEEK"] <= 202335)
        ].unique()

        df_sec_clone = df_sec_sales[df_sec_sales["DPNAME"].isin(common_dp)]
        df_sec_clone = df_sec_clone[
            (df_sec_clone["YEARWEEK"] >= YW_predict_lower)
            & (df_sec_clone["YEARWEEK"] <= YW_predict_upper)
        ]

        df_pri_clone = df_pri_sales[df_pri_sales["DPNAME"].isin(common_dp)]
        df_pri_clone = df_pri_clone[
            (df_pri_clone["YEARWEEK"] >= YW_predict_lower)
            & (df_pri_clone["YEARWEEK"] <= YW_predict_upper)
        ]
        ### Phasing 2Y
        lower = df_pri_sales["YEARWEEK"][
            (df_pri_sales["YEAR"] >= 2021) & (df_pri_sales["MONTH"] == month_idx)
        ].min()

        if month_idx == 1:
            upper = df_pri_sales["YEARWEEK"][df_pri_sales["YEAR"] < 2023].max()
        else:
            upper = df_pri_sales["YEARWEEK"][
                (df_pri_sales["YEAR"] <= 2023) & (df_pri_sales["MONTH"] == (month_idx - 1))
            ].max()

        df_pri_sales_phasing_2Y = df_pri_sales[
            (df_pri_sales["YEARWEEK"] >= lower) & (df_pri_sales["YEARWEEK"] <= upper)
        ]

        (
            df_ratio_WEEK_MONTH,
            df_ratio_WEEK_QUARTER,
            _,
        ) = create_ratio_3phase(df_pri_sales_phasing_2Y)

        df_convert = convert_data(
            df_sec_clone,
            df_ratio_WEEK_MONTH,
            df_ratio_WEEK_QUARTER,
            _,
            df_pri_clone,
        )

        df_accuracy_phase_2Y = pd.DataFrame.from_dict(
            [
                accuracy_check_dp(key, df_group, actual_col, predict_col_arr, Ratio=1)
                for key, df_group in df_convert.groupby("DPNAME")
            ]
        )

        df_accuracy_phase_2Y = df_accuracy_phase_2Y[
            [
                "DPNAME",
                "Sum_actualsale",
                "Sum_predictsale_Sec2Pri_SALES_WEEK/MONTH",
                "Accuracy_Sec2Pri_SALES_WEEK/MONTH",
                "Error_Sec2Pri_SALES_WEEK/MONTH",
            ]
        ]

        df_accuracy_phase_2Y = df_accuracy_phase_2Y.rename(
            columns={
                "DPNAME": "DPNAME",
                "Sum_actualsale": "sum_primary_sale",
                "Sum_predictsale_Sec2Pri_SALES_WEEK/MONTH": "sum_predict_sale",
                "Accuracy_Sec2Pri_SALES_WEEK/MONTH": "accuracy",
                "Error_Sec2Pri_SALES_WEEK/MONTH": "error_predict",
            }
        )

        df_convert["sum_secondary_sale"] = (
            df_convert.groupby(["DPNAME"])["SEC_SALES"].transform("sum").astype(float)
        )
        df_final = df_convert[["CATEGORY", "DPNAME", "sum_secondary_sale"]].merge(
            df_accuracy_phase_2Y, on=["DPNAME"], how="inner"
        )

        df_final["error_nature"] = (
            (df_final["sum_primary_sale"] - df_final["sum_secondary_sale"]).abs()
        ) / df_final["sum_primary_sale"]

        df_final = df_final.drop_duplicates()
        df_final = df_final.replace([-np.inf, np.inf], 0).fillna(0)

        df_final = df_final.sort_values(["CATEGORY", "DPNAME"]).reset_index(drop=True)

        df_final.to_excel(writer, sheet_name= "Month_" + str(month_idx), index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # check feasibility convert future baseline

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

df_pri_sales = df_pri_sales.sort_values(["DPNAME","YEARWEEK"]).reset_index(drop = True)
df_pri_sales

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
df_sec_sales

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

from datetime import datetime, timedelta
current_date = datetime.now().isocalendar()
current_yearweek = current_date[0] * 100 + current_date[1]

df_future_time = df_week_master[df_week_master["YEARWEEK"] >= current_yearweek]
display(df_future_time)

# COMMAND ----------

dict_future_time = {}
for year_idx in df_future_time["YEAR"].unique():
    dict_future_time[year_idx] = []
    for month_idx in df_future_time[df_future_time["YEAR"] == year_idx]["MONTH"].unique():
      dict_future_time[year_idx].append(month_idx)

dict_future_time

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## historical data

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

df_temp = df_convert[
    (df_convert["YEARWEEK"] <= current_yearweek) & (df_convert["YEARWEEK"] >= 202101)
]

pri_sale_na = df_temp[(df_temp["PRI_SALES"].isna())]
print(df_temp.shape)
print(pri_sale_na.shape)
print(pri_sale_na.shape[0] / df_temp.shape[0])

# COMMAND ----------

df_miss = pri_sale_na.groupby(["CATEGORY","WEEK/MONTH COUNT","WEEK/MONTH ORDER"], as_index=False).agg({'YEARWEEK':'count'})
df_full = df_temp.groupby(["CATEGORY","WEEK/MONTH COUNT","WEEK/MONTH ORDER"], as_index=False).agg({'YEARWEEK':'count'})

df_category = df_miss.merge(df_full, on =["CATEGORY","WEEK/MONTH COUNT","WEEK/MONTH ORDER"], how = "right")

df_category["PERCENTAGE NA"] = df_category["YEARWEEK_x"] / df_category["YEARWEEK_y"] * 100
df_category["PERCENTAGE NA"] = df_category["PERCENTAGE NA"].round(2)
df_category.columns = [
    "CATEGORY",
    "WEEK/MONTH COUNT",
    "WEEK/MONTH ORDER",
    "LENGTH PRI_SALES IS_NA",
    "LENGTH CONVERT DATA",
    "PERCENTAGE NA",
]

df_category

# COMMAND ----------

df_miss = pri_sale_na.groupby(["DPNAME","WEEK/MONTH COUNT","WEEK/MONTH ORDER"], as_index=False).agg({'YEARWEEK':'count'})
df_full = df_temp.groupby(["DPNAME","WEEK/MONTH COUNT","WEEK/MONTH ORDER"], as_index=False).agg({'YEARWEEK':'count'})

df_dpname = df_miss.merge(df_full, on =["DPNAME","WEEK/MONTH COUNT","WEEK/MONTH ORDER"], how = "right")

df_dpname["PERCENTAGE NA"] = df_dpname["YEARWEEK_x"] / df_dpname["YEARWEEK_y"] * 100
df_dpname["PERCENTAGE NA"] = df_dpname["PERCENTAGE NA"].round(2)
df_dpname.columns = [
    "DPNAME",
    "WEEK/MONTH COUNT",
    "WEEK/MONTH ORDER",
    "LENGTH PRI_SALES IS_NA",
    "LENGTH CONVERT DATA",
    "PERCENTAGE NA",
]

df_dpname

# COMMAND ----------

with pd.ExcelWriter(
    "/Workspace/Users/ng-minh-hoang.dat@unilever.com/Evaluate Convert Sec2Pri Rule-base/His_Pri_Sale_Na.xlsx"
) as writer:
    df_category.to_excel(writer, sheet_name = "CATEGORY", index = False)
    df_dpname.to_excel(writer, sheet_name = "DPNAME", index = False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## convert future baseline

# COMMAND ----------

for year_idx in dict_future_time.keys():
    for month_idx in dict_future_time[year_idx]:

        phasing_lower = df_convert["YEARWEEK"][
            (df_convert["YEAR"] >= (year_idx - 2)) & (df_convert["MONTH"] == month_idx)
        ].min()

        if month_idx == 1:
            phasing_upper = df_convert["YEARWEEK"][
                df_sec_sales["YEAR"] <= (year_idx - 1)
            ].max()
        else:
            phasing_upper = df_convert["YEARWEEK"][
                (df_convert["YEAR"] <= year_idx) & (df_convert["MONTH"] == (month_idx - 1))
            ].max()
            
        df_ratio_phasing = df_convert[
            (df_convert["YEARWEEK"] >= phasing_lower) & (df_convert["YEARWEEK"] <= phasing_upper)
        ]

        df_ratio_WEEK_MONTH = create_ratio_phasing(df_ratio_phasing, "FC_PRI_BASELINE_WEEKLY")

        df_convert_pattern = convert_data(
            df_convert[
                (df_convert["YEAR"] == year_idx) & (df_convert["MONTH"] == month_idx)
            ],
            df_ratio_WEEK_MONTH,
        )

        # df_convert[
        #     (df_convert["YEAR"] == year_idx) & (df_convert["MONTH"] == month_idx)
        # ].update(df_convert_pattern, overwrite = True)

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
        (df_convert["KEY"] == "NATIONWIDE|HNH|SUNLIGHT LEMON 1500G")
        & (df_convert["YEARWEEK"] >= current_yearweek)
    ]
)

# COMMAND ----------

fig = px.line(
    data_frame=df_convert[
        (df_convert["KEY"] == "NATIONWIDE|HNH|SUNLIGHT LEMON 1500G")
        & (df_convert["YEARWEEK"] >= current_yearweek)
    ],
    x="DATE",
    y=["PROPOSED_FC_WEEKLY", "FC_PRI_BASELINE_WEEKLY"],
)
fig.show()

# COMMAND ----------

pri_sale_na = df_convert[(df_convert["FC_PRI_BASELINE_WEEKLY"].isna()) & (df_convert["YEARWEEK"] > current_yearweek)]
print(df_convert[df_convert["YEARWEEK"] > current_yearweek].shape)
print(pri_sale_na.shape)
print(pri_sale_na.shape[0] / df_convert[df_convert["YEARWEEK"] > current_yearweek].shape[0])

# COMMAND ----------

df_percent_pri_na = pd.DataFrame()
for cate in df_convert[df_convert["YEARWEEK"] > current_yearweek]["CATEGORY"].unique():
    for count in df_convert[df_convert["YEARWEEK"] > current_yearweek][
        "WEEK/MONTH COUNT"
    ].unique():
        for order in df_convert[
            (df_convert["YEARWEEK"] > current_yearweek)
            & (df_convert["WEEK/MONTH COUNT"] == count)
        ]["WEEK/MONTH ORDER"].unique():
            record = [
                cate,
                count,
                order,
                pri_sale_na[
                    (pri_sale_na["CATEGORY"] == cate)
                    & (pri_sale_na["WEEK/MONTH ORDER"] == order)
                    & (pri_sale_na["WEEK/MONTH COUNT"] == count)
                ].shape[0],
                df_convert[
                    (df_convert["YEARWEEK"] > current_yearweek)
                    & (df_convert["CATEGORY"] == cate)
                    & (df_convert["WEEK/MONTH COUNT"] == count)
                    & (df_convert["WEEK/MONTH ORDER"] == order)
                ].shape[0],
                (
                    round(
                        pri_sale_na[
                            (pri_sale_na["CATEGORY"] == cate)
                            & (pri_sale_na["WEEK/MONTH ORDER"] == order)
                            & (pri_sale_na["WEEK/MONTH COUNT"] == count)
                        ].shape[0]
                        / df_convert[
                            (df_convert["YEARWEEK"] > current_yearweek)
                            & (df_convert["CATEGORY"] == cate)
                            & (df_convert["WEEK/MONTH COUNT"] == count)
                            & (df_convert["WEEK/MONTH ORDER"] == order)
                        ].shape[0]
                        * 100,
                        2,
                    )
                    if df_convert[
                        (df_convert["YEARWEEK"] > current_yearweek)
                        & (df_convert["CATEGORY"] == cate)
                        & (df_convert["WEEK/MONTH COUNT"] == count)
                        & (df_convert["WEEK/MONTH ORDER"] == order)
                    ].shape[0]
                    > 0
                    else 0
                ),
            ]
            df_percent_pri_na = df_percent_pri_na.append(
                pd.Series(record), ignore_index=True
            )

# COMMAND ----------

df_percent_pri_na.columns = [
    "CATEGORY",
    "WEEK/MONTH COUNT",
    "WEEK/MONTH ORDER",
    "LENGTH FC_PRI_BASELINE IS_NA",
    "LENGTH CONVERT DATA",
    "PERCENTAGE NA",
]
df_percent_pri_na = df_percent_pri_na.sort_values(["CATEGORY","WEEK/MONTH COUNT","WEEK/MONTH ORDER"])
df_percent_pri_na

# COMMAND ----------

# df_percent_pri_na.to_excel("/Workspace/Users/ng-minh-hoang.dat@unilever.com/Evaluate Convert Sec2Pri Rule-base/Future_Pri_Sale_Na.xlsx", index = False)

# COMMAND ----------


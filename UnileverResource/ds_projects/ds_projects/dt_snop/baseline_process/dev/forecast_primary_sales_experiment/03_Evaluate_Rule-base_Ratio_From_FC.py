# Databricks notebook source
import pandas as pd
import numpy as np

import plotly.express as px
import plotly.graph_objects as go

import warnings
warnings.filterwarnings("ignore")

from tqdm import tqdm

from datetime import timedelta, date


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Primary Sale

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
# MAGIC # Secondary Sale

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

df_sec_sales = df_sec_sales.merge(
    df_master_product[["DPNAME", "SEGMENT"]], on="DPNAME", how="left"
)

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
# MAGIC # Forecast Baseline Primary Sale

# COMMAND ----------

df_forecast = pd.read_excel("/Workspace/Users/ng-minh-hoang.dat@unilever.com/Forecast Primary Sales/SNOP_Forecast_Baseline_Primary.xlsx")
print(df_forecast.shape)
df_forecast.head(2)

# COMMAND ----------

df_master_product = df_master_product[['CATEGORY','DPNAME','SEGMENT']]
df_master_product

# COMMAND ----------

df_forecast = df_forecast.merge(df_master_product, on = ['CATEGORY','DPNAME'], how = 'left')
df_forecast.shape

# COMMAND ----------

df_forecast = df_forecast.drop_duplicates(subset =['KEY','YEARWEEK'], keep = 'first')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Data consistence DP

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

# df_consist_dp["RATIO_WEEKLY"] = (
#     df_consist_dp["SEC_SALES"] - df_consist_dp["PRI_SALES"]
# ) / df_consist_dp["PRI_SALES"]

# df_consist_dp["PRI_SALES_MONTHLY"] = (
#     df_consist_dp.groupby(["DPNAME", "YEAR", "MONTH"])["PRI_SALES"]
#     .transform("sum")
#     .astype(float)
# )

# df_consist_dp["SEC_SALES_MONTHLY"] = (
#     df_consist_dp.groupby(["DPNAME", "YEAR", "MONTH"])["SEC_SALES"]
#     .transform("sum")
#     .astype(float)
# )

# df_consist_dp["RATIO_MONTHLY"] = (
#     df_consist_dp["SEC_SALES_MONTHLY"] - df_consist_dp["PRI_SALES_MONTHLY"]
# ) / df_consist_dp["PRI_SALES_MONTHLY"]

# df_consist_dp["QUARTER"] = ((df_consist_dp["MONTH"] - 1) / 3).astype(int)

# df_consist_dp["PRI_SALES_QUARTERLY"] = (
#     df_consist_dp.groupby(["DPNAME", "YEAR", "QUARTER"])["PRI_SALES"]
#     .transform("sum")
#     .astype(float)
# )

# df_consist_dp["SEC_SALES_QUARTERLY"] = (
#     df_consist_dp.groupby(["DPNAME", "YEAR", "QUARTER"])["SEC_SALES"]
#     .transform("sum")
#     .astype(float)
# )

# df_consist_dp["RATIO_QUARTERLY"] = (
#     df_consist_dp["SEC_SALES_QUARTERLY"] - df_consist_dp["PRI_SALES_QUARTERLY"]
# ) / df_consist_dp["PRI_SALES_QUARTERLY"]

# df_consist_dp["MA_RATIO_WEEK/MONTH"] = (
#     df_consist_dp["RATIO_WEEKLY"].rolling(window=4).mean()
# )
# df_consist_dp["MA_RATIO_WEEK/QUARTER"] = (
#     df_consist_dp["RATIO_WEEKLY"].rolling(window=13).mean()
# )
# df_consist_dp["MA_RATIO_MONTH/QUARTER"] = (
#     df_consist_dp["RATIO_MONTHLY"].rolling(window=3).mean()
# )

# df_consist_dp

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

def create_ratio_3phase(df_phasing_ratio, ratio_var):
    ratio_WEEK_MONTH_arr = []
    ratio_WEEK_QUARTER_arr = []
    ratio_MONTH_QUARTER_arr = []
    df_phasing_ratio["QUARTER"] = ((df_phasing_ratio["MONTH"] -1) / 3).astype(int)

    # ********************************************************
    # Ratio Week per Month
    for key, df_group in tqdm(
        df_phasing_ratio.groupby(["BANNER", "REGION", "CATEGORY", "DPNAME"])
    ):
        df_group = df_group.sort_values('DATE')
        df_group[ratio_var] = df_group[ratio_var].fillna(0)

        df_group["WEEK/MONTH RATIO"] = df_group[ratio_var] / df_group.groupby(
            ["YEAR", "MONTH"]
        )[ratio_var].transform("sum")

        df_group["WEEK/MONTH COUNT"] = df_group.groupby(["YEAR", "MONTH"])[
            "YEARWEEK"
        ].transform("count")

        df_group["WEEK/MONTH ORDER"] = df_group.groupby(["YEAR", "MONTH"])[
            "YEARWEEK"
        ].transform("rank")

        # Contribution of selling day follow phase Week/Month
        df_group["SELLINGDAY_WEEK/MONTH RATIO"] = (
            df_group["WEEK/MONTH RATIO"] / df_group["WORKINGDAY"]
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

    # df_ratio_WEEK_MONTH["KEY"] = "NATIONWIDE|" + df_ratio_WEEK_MONTH["KEY"]


    # ********************************************************
    # Ratio Week per Quarter
    for key, df_group in tqdm(
        df_phasing_ratio.groupby(["BANNER", "REGION", "CATEGORY", "DPNAME"])
    ):
        df_group = df_group.sort_values('DATE')
        df_group[ratio_var] = df_group[ratio_var].fillna(0)

        df_group["WEEK/QUARTER RATIO"] = df_group[ratio_var] / df_group.groupby(
            ["YEAR", "QUARTER"]
        )[ratio_var].transform("sum")

        df_group["WEEK/QUARTER COUNT"] = df_group.groupby(["YEAR", "QUARTER"])[
            "YEARWEEK"
        ].transform("count")

        df_group["WEEK/QUARTER ORDER"] = df_group.groupby(["YEAR", "QUARTER"])[
            "YEARWEEK"
        ].transform("rank")

        # Contribution of selling day follow phase Week/Quarter
        df_group["SELLINGDAY_WEEK/QUARTER RATIO"] = (
            df_group["WEEK/QUARTER RATIO"] / df_group["WORKINGDAY"]
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

    return df_ratio_WEEK_MONTH, df_ratio_WEEK_QUARTER, _

# COMMAND ----------

def ratio_baseline_forecast(df_phasing_ratio, ratio_var):
    df_phasing_ratio = df_phasing_ratio[
        [
            "KEY",
            "YEARWEEK",
            "ACTUALSALE",
            "BANNER",
            "REGION",
            "CATEGORY",
            "SEGMENT",
            "DPNAME",
            "WORKINGDAY",
            "DATE",
            "YEAR",
            "MONTH",
            ratio_var,
        ]
    ]
    df_phasing_ratio["QUARTER"] = ((df_phasing_ratio["MONTH"] - 1) / 3).astype(int)

    df_phasing_ratio = df_phasing_ratio[
        (df_phasing_ratio["YEARWEEK"] >= 202301)
        & (df_phasing_ratio["YEARWEEK"] <= 202335)
    ]
    df_phasing_ratio = df_phasing_ratio.sort_values("DATE")
    df_phasing_ratio[ratio_var] = df_phasing_ratio[ratio_var].fillna(0)

    df_phasing_ratio["SELLINGDAY"] = df_phasing_ratio[ratio_var] / df_phasing_ratio["WORKINGDAY"]

    df_phasing_ratio["SELLINGDAY_WEEK/MONTH RATIO"] = (
        df_phasing_ratio["SELLINGDAY"] / df_phasing_ratio.groupby(["KEY","YEAR","MONTH"])[ratio_var].transform("sum")
    )

    df_phasing_ratio["SELLINGDAY_WEEK/QUARTER RATIO"] = (
        df_phasing_ratio["SELLINGDAY"] / df_phasing_ratio.groupby(["KEY","YEAR","QUARTER"])[ratio_var].transform("sum")
    )

    # just a temp col add _median to consistence with df_convert function above
    df_phasing_ratio = df_phasing_ratio.rename(columns = {
        "SELLINGDAY_WEEK/MONTH RATIO": "SELLINGDAY_WEEK/MONTH RATIO_median",
        "SELLINGDAY_WEEK/QUARTER RATIO": "SELLINGDAY_WEEK/QUARTER RATIO_median"
    })

    df_phasing_ratio = df_phasing_ratio[
        [
            "KEY",
            "YEARWEEK",
            "SELLINGDAY_WEEK/MONTH RATIO_median",
            "SELLINGDAY_WEEK/QUARTER RATIO_median",
        ]
    ]

    return df_phasing_ratio

# COMMAND ----------

def convert_data(
    df_sec_sales, df_pri_sales, df_forecast, mode_phasing_ratio, ratio_var, mode_groupby
):
    """
    mode_phasing_ratio: ALL to phasing ratio all data df_forecast and groupby week_count, week_order
    ratio_var: variable to calculate phasing ratio in df_forecast
    mode_groupby: values in ["SEGMENT", "CATEGORY"] else will take "DPNAME" to group phasing ratio covert
    """

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
    df_convert = df_convert[df_convert["YEARWEEK"] < 202338]
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
        df_convert.groupby(["KEY", "YEAR", "MONTH"])["SEC_SALES"]
        .transform("sum")
        .astype(float)
    )

    df_convert = df_convert.sort_values(["KEY", "DATE"]).reset_index(drop=True)

    # ****************************************************
    if mode_phasing_ratio == "ALL":
        df_ratio_WEEK_MONTH, df_ratio_WEEK_QUARTER, _ = create_ratio_3phase(
            df_forecast, ratio_var
        )

        df_convert = df_convert.merge(
            df_ratio_WEEK_MONTH,
            on=["WEEK/MONTH COUNT", "WEEK/MONTH ORDER", "KEY_NATIONWIDE"],
            how="left",
        )

        df_convert = df_convert.merge(
            df_ratio_WEEK_QUARTER,
            on=["WEEK/QUARTER COUNT", "WEEK/QUARTER ORDER", "KEY_NATIONWIDE"],
            how="left",
        )

        if mode_groupby in ["CATEGORY", "SEGMENT"]:
            ratio_WEEK_MONTH_segment = (
                df_convert.groupby([mode_groupby, "WEEK/MONTH COUNT", "WEEK/MONTH ORDER"])
                .agg({"SELLINGDAY_WEEK/MONTH RATIO_median": ["median"]})
                .reset_index()
            )

            ratio_WEEK_MONTH_segment.columns = [
                "_".join(col) for col in ratio_WEEK_MONTH_segment.columns
            ]

            ratio_WEEK_MONTH_segment.columns = [
                mode_groupby,
                "WEEK/MONTH COUNT",
                "WEEK/MONTH ORDER",
                "SELLINGDAY_WEEK/MONTH RATIO_median",
            ]
            df_convert = df_convert.drop("SELLINGDAY_WEEK/MONTH RATIO_median", axis=1)

            df_convert = df_convert.merge(
                ratio_WEEK_MONTH_segment,
                on=[mode_groupby, "WEEK/MONTH COUNT", "WEEK/MONTH ORDER"],
                how="left",
            )

            # **********************************************
            ratio_WEEK_QUARTER_segment = (
                df_convert.groupby(
                    [mode_groupby, "WEEK/QUARTER COUNT", "WEEK/QUARTER ORDER"]
                )
                .agg({"SELLINGDAY_WEEK/QUARTER RATIO_median": ["median"]})
                .reset_index()
            )

            ratio_WEEK_QUARTER_segment.columns = [
                "_".join(col) for col in ratio_WEEK_QUARTER_segment.columns
            ]

            ratio_WEEK_QUARTER_segment.columns = [
                mode_groupby,
                "WEEK/QUARTER COUNT",
                "WEEK/QUARTER ORDER",
                "SELLINGDAY_WEEK/QUARTER RATIO_median",
            ]
            df_convert = df_convert.drop("SELLINGDAY_WEEK/QUARTER RATIO_median", axis=1)

            df_convert = df_convert.merge(
                ratio_WEEK_QUARTER_segment,
                on=[mode_groupby, "WEEK/QUARTER COUNT", "WEEK/QUARTER ORDER"],
                how="left",
            )

    # backward 3 months =  current month + 2 months latest the same week count
    elif mode_phasing_ratio == "BACKWARD": 
        # last_3months = df_forecast["YEARWEEK"].max() - 13
        last_3months = df_forecast["DATE"].max() +  timedelta(days=-90)

        df_phasing_ratio = ratio_baseline_forecast(df_forecast, ratio_var)
        df_convert = df_convert.merge(
            df_phasing_ratio, on=["KEY", "YEARWEEK"], how="left"
        )
        # "KEY",
        # "YEARWEEK",
        # "SELLINGDAY_WEEK/MONTH RATIO_median",
        # "SELLINGDAY_WEEK/QUARTER RATIO_median",
        df_convert["SELLINGDAY_WEEK/MONTH RATIO_median"] = df_convert["SELLINGDAY_WEEK/MONTH RATIO_median"] / df_convert.groupby("KEY","WEEK/MONTH COUNT","WEEK/MONTH ORDER")

        if mode_groupby in ["CATEGORY", "SEGMENT"]:
            ratio_WEEK_MONTH_segment = (
                df_convert.groupby([mode_groupby, "WEEK/MONTH COUNT", "WEEK/MONTH ORDER"])
                .agg({"SELLINGDAY_WEEK/MONTH RATIO_median": ["median"]})
                .reset_index()
            )

            ratio_WEEK_MONTH_segment.columns = [
                "_".join(col) for col in ratio_WEEK_MONTH_segment.columns
            ]

            ratio_WEEK_MONTH_segment.columns = [
                mode_groupby,
                "WEEK/MONTH COUNT",
                "WEEK/MONTH ORDER",
                "SELLINGDAY_WEEK/MONTH RATIO_median",
            ]
            df_convert = df_convert.drop("SELLINGDAY_WEEK/MONTH RATIO_median", axis=1)

            df_convert = df_convert.merge(
                ratio_WEEK_MONTH_segment,
                on=[mode_groupby, "WEEK/MONTH COUNT", "WEEK/MONTH ORDER"],
                how="left",
            )

            # **********************************************
            ratio_WEEK_QUARTER_segment = (
                df_convert.groupby(
                    [mode_groupby, "WEEK/QUARTER COUNT", "WEEK/QUARTER ORDER"]
                )
                .agg({"SELLINGDAY_WEEK/QUARTER RATIO_median": ["median"]})
                .reset_index()
            )

            ratio_WEEK_QUARTER_segment.columns = [
                "_".join(col) for col in ratio_WEEK_QUARTER_segment.columns
            ]

            ratio_WEEK_QUARTER_segment.columns = [
                mode_groupby,
                "WEEK/QUARTER COUNT",
                "WEEK/QUARTER ORDER",
                "SELLINGDAY_WEEK/QUARTER RATIO_median",
            ]
            df_convert = df_convert.drop("SELLINGDAY_WEEK/QUARTER RATIO_median", axis=1)

            df_convert = df_convert.merge(
                ratio_WEEK_QUARTER_segment,
                on=[mode_groupby, "WEEK/QUARTER COUNT", "WEEK/QUARTER ORDER"],
                how="left",
            )

    else:  # phasing ratio for only its month
        df_phasing_ratio = ratio_baseline_forecast(df_forecast, ratio_var)
        df_convert = df_convert.merge(
            df_phasing_ratio, on=["KEY", "YEARWEEK"], how="left"
        )

        if mode_groupby in ["CATEGORY", "SEGMENT"]:
            ratio_WEEK_MONTH_segment = (
                df_convert.groupby(
                    [mode_groupby, "WEEK/MONTH COUNT", "WEEK/MONTH ORDER"]
                )
                .agg({"SELLINGDAY_WEEK/MONTH RATIO_median": ["median"]})
                .reset_index()
            )

            ratio_WEEK_MONTH_segment.columns = [
                "_".join(col) for col in ratio_WEEK_MONTH_segment.columns
            ]

            ratio_WEEK_MONTH_segment.columns = [
                mode_groupby,
                "WEEK/MONTH COUNT",
                "WEEK/MONTH ORDER",
                "SELLINGDAY_WEEK/MONTH RATIO_median",
            ]
            df_convert = df_convert.drop("SELLINGDAY_WEEK/MONTH RATIO_median", axis=1)

            df_convert = df_convert.merge(
                ratio_WEEK_MONTH_segment,
                on=[mode_groupby, "WEEK/MONTH COUNT", "WEEK/MONTH ORDER"],
                how="left",
            )

            # **********************************************
            ratio_WEEK_QUARTER_segment = (
                df_convert.groupby(
                    [mode_groupby, "WEEK/QUARTER COUNT", "WEEK/QUARTER ORDER"]
                )
                .agg({"SELLINGDAY_WEEK/QUARTER RATIO_median": ["median"]})
                .reset_index()
            )

            ratio_WEEK_QUARTER_segment.columns = [
                "_".join(col) for col in ratio_WEEK_QUARTER_segment.columns
            ]

            ratio_WEEK_QUARTER_segment.columns = [
                mode_groupby,
                "WEEK/QUARTER COUNT",
                "WEEK/QUARTER ORDER",
                "SELLINGDAY_WEEK/QUARTER RATIO_median",
            ]
            df_convert = df_convert.drop("SELLINGDAY_WEEK/QUARTER RATIO_median", axis=1)

            df_convert = df_convert.merge(
                ratio_WEEK_QUARTER_segment,
                on=[mode_groupby, "WEEK/QUARTER COUNT", "WEEK/QUARTER ORDER"],
                how="left",
            )

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
    ).astype(
        float
    )

    df_convert["RATIO_median_WEEK/QUARTER"] = (
        df_convert["SELLINGDAY_WEEK/QUARTER RATIO_median"] * df_convert["DTWORKINGDAY"]
    )
    df_convert["Normalized_RATIO_median_WEEK/QUARTER"] = df_convert[
        "RATIO_median_WEEK/QUARTER"
    ] / df_convert.groupby(["KEY_NATIONWIDE", "WEEK/QUARTER COUNT"])[
        "RATIO_median_WEEK/QUARTER"
    ].transform(
        lambda x: x.unique().sum()
    ).astype(
        float
    )

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
# MAGIC ### Ratio Agg weekly/month

# COMMAND ----------

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
df_convert = df_convert[df_convert["YEARWEEK"] < 202338]
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

df_convert = df_convert.sort_values(["KEY", "DATE"]).reset_index(drop=True)

df_phasing_ratio = df_forecast.copy()
df_phasing_ratio = df_phasing_ratio[
    (df_phasing_ratio["YEARWEEK"] >=202301) & (df_phasing_ratio["YEARWEEK"] <= 202335)
]
df_group = ratio_baseline_forecast(df_phasing_ratio, "BASELINE_FORECAST")

df_convert = df_convert.merge(df_group, on = ["KEY","YEARWEEK"])

# COMMAND ----------

temp = df_convert[df_convert["DPNAME"] == "COMFORT ELEGANT POUCH 3.6KG"]
temp = temp.
temp

# COMMAND ----------

t = temp[temp["YEARWEEK"] == 202335]
t["SELLINGDAY_WEEK/MONTH RATIO_median"]

# COMMAND ----------

temp.groupby(["KEY","WEEK/MONTH COUNT","WEEK/MONTH ORDER"])["SELLINGDAY_WEEK/MONTH RATIO_median"].unique().reset_index()

# COMMAND ----------

temp["new_rate"] = temp["SELLINGDAY_WEEK/MONTH RATIO_median"] / temp.groupby(["KEY","WEEK/MONTH COUNT","WEEK/MONTH ORDER"])

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
# MAGIC ### Phasing 8 months target

# COMMAND ----------

df_forecast = df_forecast[(df_forecast["YEARWEEK"])]

df_convert = convert_data(
    df_sec_clone,
    df_pri_clone,
    df_forecast,
    mode_phasing_ratio="ALL",
    ratio_var="BASELINE_FORECAST",
    mode_groupby="CATEGORY",
)

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
# MAGIC ### Phasing each months

# COMMAND ----------

df_convert = convert_data(df_sec_clone, df_pri_clone, df_forecast, mode_phasing_ratio = "", ratio_var = "BASELINE_FORECAST", mode_groupby = "CATEGORY")

# COMMAND ----------

df_accuracy_phase_1month = pd.DataFrame.from_dict(
    [
        accuracy_check(key, df_group, actual_col, predict_col_arr, Ratio = 1)
        for key, df_group in df_convert.groupby("CATEGORY")
    ]
)

df_accuracy_phase_1month.columns = [
    "CATEGORY",
    "Sum_actualsale",
    "Sum_predictsale_WEEK/MONTH_normalized_Phasing1month",
    "Accuracy_WEEK/MONTH_normalized_Phasing1month",
    "Error_WEEK/MONTH_normalized_Phasing1month",
    "Sum_predictsale_WEEK/MONTH_Phasing1month",
    "Accuracy_WEEK/MONTH_Phasing1month",
    "Error_WEEK/MONTH_Phasing1month",
    "Sum_predictsale_WEEK/QUARTER_normalized_Phasing1month",
    "Accuracy_WEEK/QUARTER_normalized_Phasing1month",
    "Error_WEEK/QUARTER_normalized_Phasing1month",
    "Sum_predictsale_WEEK/QUARTER_Phasing1month",
    "Accuracy_WEEK/QUARTER_Phasing1month",
    "Error_WEEK/QUARTER_Phasing1month"
]

# COMMAND ----------

df_accuracy_phase_1month

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Merge Phasing

# COMMAND ----------

df_accuracy_phase_1month["CATEGORY"] = df_accuracy_phase_1month["CATEGORY"].replace(" ","").astype(str)
df_accuracy_phase_all["CATEGORY"] = df_accuracy_phase_all["CATEGORY"].replace(" ","").astype(str)

df_accuracy_phase_1month["Sum_actualsale"] = df_accuracy_phase_1month["Sum_actualsale"].round(2).astype(float)
df_accuracy_phase_all["Sum_actualsale"] = df_accuracy_phase_all["Sum_actualsale"].round(2).astype(float)

# COMMAND ----------

df_accuracy_all_data = df_accuracy_phase_all.merge(
    df_accuracy_phase_1month, on=["CATEGORY", "Sum_actualsale"], how = "outer"
)
print(df_accuracy_all_data.shape)
df_accuracy_all_data = df_accuracy_all_data.fillna(0)
df_accuracy_all_data

# COMMAND ----------

df_accuracy_all_data.to_excel("/Workspace/Users/ng-minh-hoang.dat@unilever.com/Forecast Primary Sales/Accuracy_convert_baselineFC_8month2023_by_category.xlsx")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Evaluate Monthly 1/2023 - 8/2023

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Phasing all months

# COMMAND ----------

df_accuracy_monthly2023 = pd.DataFrame(columns=["CATEGORY"])

for month_idx in range(1, 9):
    df_sec_clone = pd.DataFrame(columns=df_sec_sales.columns)
    df_pri_clone = pd.DataFrame(columns=df_pri_sales.columns)

    common_dp = df_consist_dp["DPNAME"][
        (df_consist_dp["YEARWEEK"] >= 202301) & (df_consist_dp["YEARWEEK"] <= 202335)
    ].unique()

    df_sec_clone = df_sec_sales[df_sec_sales["DPNAME"].isin(common_dp)]
    df_sec_clone = df_sec_clone[
        (df_sec_clone["YEARWEEK"] >= 202301) & (df_sec_clone["YEARWEEK"] <= 202335)
    ]
    df_sec_clone = df_sec_clone[df_sec_clone["MONTH"] == month_idx]

    df_pri_clone = df_pri_sales[df_pri_sales["DPNAME"].isin(common_dp)]
    df_pri_clone = df_pri_clone[
        (df_pri_clone["YEARWEEK"] >= 202301) & (df_pri_clone["YEARWEEK"] <= 202335)
    ]
    df_pri_clone = df_pri_clone[df_pri_clone["MONTH"] == month_idx]

    df_phasing_ratio = df_forecast.copy()

    df_convert = convert_data(
        df_sec_clone,
        df_pri_clone,
        df_phasing_ratio,
        mode_phasing_ratio = "ALL",
        ratio_var= "BASELINE_FORECAST",
        mode_groupby = "CATEGORY"
    )

    df_accuracy_phase_2Y = pd.DataFrame.from_dict(
        [
            accuracy_check(key, df_group, actual_col, predict_col_arr, Ratio= 1)
            for key, df_group in df_convert.groupby("CATEGORY")
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

# COMMAND ----------

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

df_accuracy_monthly2023.to_excel("/Workspace/Users/ng-minh-hoang.dat@unilever.com/Forecast Primary Sales/Accuracy_Convert_baselineFC_Monthly2023_phasingall.xlsx")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Phasing each months

# COMMAND ----------

df_accuracy_monthly2023 = pd.DataFrame(columns=["CATEGORY"])

for month_idx in range(1, 9):
    df_sec_clone = pd.DataFrame(columns=df_sec_sales.columns)
    df_pri_clone = pd.DataFrame(columns=df_pri_sales.columns)

    common_dp = df_consist_dp["DPNAME"][
        (df_consist_dp["YEARWEEK"] >= 202301) & (df_consist_dp["YEARWEEK"] <= 202335)
    ].unique()

    df_sec_clone = df_sec_sales[df_sec_sales["DPNAME"].isin(common_dp)]
    df_sec_clone = df_sec_clone[
        (df_sec_clone["YEARWEEK"] >= 202301) & (df_sec_clone["YEARWEEK"] <= 202335)
    ]
    df_sec_clone = df_sec_clone[df_sec_clone["MONTH"] == month_idx]

    df_pri_clone = df_pri_sales[df_pri_sales["DPNAME"].isin(common_dp)]
    df_pri_clone = df_pri_clone[
        (df_pri_clone["YEARWEEK"] >= 202301) & (df_pri_clone["YEARWEEK"] <= 202335)
    ]
    df_pri_clone = df_pri_clone[df_pri_clone["MONTH"] == month_idx]

    df_phasing_ratio = df_forecast.copy()
    df_phasing_ratio = df_phasing_ratio[
        (df_phasing_ratio["YEARWEEK"] >= 202301) & (df_phasing_ratio["YEARWEEK"] <= 202335)
    ]
    df_phasing_ratio = df_phasing_ratio[df_phasing_ratio["MONTH"] == month_idx]

    df_convert = convert_data(
        df_sec_clone,
        df_pri_clone,
        df_phasing_ratio,
        mode_phasing_ratio = "",
        ratio_var= "BASELINE_FORECAST",
        mode_groupby = "CATEGORY"
    )

    df_accuracy_phase_2Y = pd.DataFrame.from_dict(
        [
            accuracy_check(key, df_group, actual_col, predict_col_arr, Ratio= 1)
            for key, df_group in df_convert.groupby("CATEGORY")
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

# COMMAND ----------

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

df_accuracy_monthly2023.to_excel("/Workspace/Users/ng-minh-hoang.dat@unilever.com/Forecast Primary Sales/Accuracy_Convert_baselineFC_Monthly2023_phasing1month.xlsx")

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
    "/Workspace/Users/ng-minh-hoang.dat@unilever.com/Forecast Primary Sales/Accuracy_convert_baselineFC_DPNAME.xlsx"
) as writer:
    for month_idx in range(1, 9):
        df_sec_clone = pd.DataFrame(columns=df_sec_sales.columns)
        df_pri_clone = pd.DataFrame(columns=df_pri_sales.columns)

        common_dp = df_consist_dp["DPNAME"][
            (df_consist_dp["YEARWEEK"] >= 202301) & (df_consist_dp["YEARWEEK"] <= 202335)
        ].unique()

        df_sec_clone = df_sec_sales[df_sec_sales["DPNAME"].isin(common_dp)]
        df_sec_clone = df_sec_clone[
            (df_sec_clone["YEARWEEK"] >= 202301) & (df_sec_clone["YEARWEEK"] <= 202335)
        ]
        df_sec_clone = df_sec_clone[df_sec_clone["MONTH"] == month_idx]

        df_pri_clone = df_pri_sales[df_pri_sales["DPNAME"].isin(common_dp)]
        df_pri_clone = df_pri_clone[
            (df_pri_clone["YEARWEEK"] >= 202301) & (df_pri_clone["YEARWEEK"] <= 202335)
        ]
        df_pri_clone = df_pri_clone[df_pri_clone["MONTH"] == month_idx]

        df_phasing_ratio = df_forecast.copy()
        df_phasing_ratio = df_phasing_ratio[
            (df_phasing_ratio["YEARWEEK"] >= 202301) & (df_phasing_ratio["YEARWEEK"] <= 202335)
        ]
        df_phasing_ratio = df_phasing_ratio[df_phasing_ratio["MONTH"] == month_idx]

        df_convert = convert_data(
            df_sec_clone,
            df_pri_clone,
            df_phasing_ratio,
            mode_phasing_ratio = "",
            ratio_var= "BASELINE_FORECAST",
            mode_groupby = "CATEGORY"
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

        stats_df_convert = df_convert.copy()
        stats_df_convert["PRI_DAILY_SALES"] = stats_df_convert["PRI_SALES"] / stats_df_convert["DTWORKINGDAY"]
        stats_df_convert["SEC_DAILY_SALES"] = stats_df_convert["SEC_SALES"] / stats_df_convert["DTWORKINGDAY"]
        stats_df_convert["YEARWEEK_MIN"] = stats_df_convert["YEARWEEK"]
        stats_df_convert["YEARWEEK_MAX"] = stats_df_convert["YEARWEEK"]
        stats_df_convert["PRI_DAILY_SALES_MIN"] = stats_df_convert["PRI_DAILY_SALES"]
        stats_df_convert["PRI_DAILY_SALES_MAX"] = stats_df_convert["PRI_DAILY_SALES"]
        stats_df_convert["PRI_DAILY_SALES_AVG"] = stats_df_convert["PRI_DAILY_SALES"]

        stats_df_convert["SEC_DAILY_SALES_MIN"] = stats_df_convert["SEC_DAILY_SALES"]
        stats_df_convert["SEC_DAILY_SALES_MAX"] = stats_df_convert["SEC_DAILY_SALES"]
        stats_df_convert["SEC_DAILY_SALES_AVG"] = stats_df_convert["SEC_DAILY_SALES"]

        stats_df = stats_df_convert.groupby("DPNAME").agg({
            "YEARWEEK" : "count",
            "YEARWEEK_MIN" : "min",
            "YEARWEEK_MAX" : "max",
            "SEC_DAILY_SALES_MIN" : "min",
            "SEC_DAILY_SALES_AVG" : "mean",
            "SEC_DAILY_SALES_MAX" : "max",
            "PRI_DAILY_SALES_MIN" : "min",
            "PRI_DAILY_SALES_AVG" : "mean",
            "PRI_DAILY_SALES_MAX" : "max",
        }).reset_index()
        stats_df = stats_df.rename(columns = {"YEARWEEK": "YEARWEEK_COUNT"})

        df_final = df_final.merge(stats_df, on = "DPNAME", how = "left")

        df_final = df_final.drop_duplicates()
        df_final = df_final.replace([-np.inf, np.inf], 0).fillna(0)

        df_final = df_final.sort_values(["CATEGORY", "DPNAME"]).reset_index(drop=True)

        df_final.to_excel(writer, sheet_name= "Month_" + str(month_idx), index=False)

# COMMAND ----------

stats_df_convert = df_convert.copy()
stats_df_convert["PRI_DAILY_SALES"] = stats_df_convert["PRI_SALES"] / stats_df_convert["DTWORKINGDAY"]
stats_df_convert["SEC_DAILY_SALES"] = stats_df_convert["SEC_SALES"] / stats_df_convert["DTWORKINGDAY"]
stats_df_convert["YEARWEEK_MIN"] = stats_df_convert["YEARWEEK"]
stats_df_convert["YEARWEEK_MAX"] = stats_df_convert["YEARWEEK"]
stats_df_convert["PRI_DAILY_SALES_MIN"] = stats_df_convert["PRI_DAILY_SALES"]
stats_df_convert["PRI_DAILY_SALES_MAX"] = stats_df_convert["PRI_DAILY_SALES"]
stats_df_convert["PRI_DAILY_SALES_AVG"] = stats_df_convert["PRI_DAILY_SALES"]

stats_df_convert["SEC_DAILY_SALES_MIN"] = stats_df_convert["SEC_DAILY_SALES"]
stats_df_convert["SEC_DAILY_SALES_MAX"] = stats_df_convert["SEC_DAILY_SALES"]
stats_df_convert["SEC_DAILY_SALES_AVG"] = stats_df_convert["SEC_DAILY_SALES"]

stats_df = stats_df_convert.groupby("DPNAME").agg({
    "YEARWEEK" : "count",
    "YEARWEEK_MIN" : "min",
    "YEARWEEK_MAX" : "max",
    "SEC_DAILY_SALES_MIN" : "min",
    "SEC_DAILY_SALES_AVG" : "mean",
    "SEC_DAILY_SALES_MAX" : "max",
    "PRI_DAILY_SALES_MIN" : "min",
    "PRI_DAILY_SALES_AVG" : "mean",
    "PRI_DAILY_SALES_MAX" : "max",
}).reset_index()

# COMMAND ----------

stats_df
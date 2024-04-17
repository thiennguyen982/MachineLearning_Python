# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Load data

# COMMAND ----------

# MAGIC %run "/Users/ng-minh-hoang.dat@unilever.com/EnvironmentSetup_clone"

# COMMAND ----------

import pandas as pd
import numpy as np

import plotly.express as px
import plotly.graph_objects as go
import seaborn as sns
import matplotlib.pyplot as plt

import datetime

from tqdm import tqdm

import warnings
warnings.filterwarnings("ignore")

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
    engine = "openpyxl"
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

df_pri_sales_by_code = (
    df_pri_sales.groupby(["YEARWEEK", "BANNER", "REGION", "CATEGORY", "DPNAME","MATERIAL"])[
        ["PCS", "TON", "CS"]
    ]
    .sum()
    .reset_index()
)

df_pri_sales_by_code["ACTUALSALE"] = df_pri_sales_by_code["TON"]
df_pri_sales_by_code["ACTUALSALE"].loc[
    (df_pri_sales_by_code["CATEGORY"].isin(["SKINCARE", "IC", "DEO"]))
] = df_pri_sales_by_code["CS"]
df_pri_sales_by_code["ACTUALSALE"].loc[(df_pri_sales_by_code["CATEGORY"].isin(["TBRUSH"]))] = (
    df_pri_sales_by_code["PCS"] / 1000
)

df_pri_sales_by_code["YEARWEEK"] = df_pri_sales_by_code["YEARWEEK"].astype(int)

df_pri_sales_by_code["DATE"] = pd.to_datetime(df_pri_sales_by_code["YEARWEEK"].astype(str) + "-1", format="%G%V-%w")

df_pri_sales_by_code = df_pri_sales_by_code.fillna(0)
df_pri_sales_by_code = df_pri_sales_by_code.sort_values(["DPNAME","MATERIAL","YEARWEEK"]).reset_index(drop = True)

print(df_pri_sales_by_code.shape)
df_pri_sales_by_code.head(3)

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
# MAGIC ## Secondary Sale by code

# COMMAND ----------

df_sec_sales_by_code = pd.read_parquet(
    "/dbfs/mnt/adls/SAP_HANA_DATASET/RAW_DATA/SEC_SALES_BANNER_WEEKLY_PARQUET"
)

df_sec_sales_by_code = df_sec_sales_by_code[
    df_sec_sales_by_code["BANNER"].isin(
        ["DT HCME", "DT MEKONG DELTA", "DT North", "DT CENTRAL"]
    )
]
print(df_sec_sales_by_code.shape)
df_sec_sales_by_code.head(2)

# COMMAND ----------

df_sec_sales_by_code = df_sec_sales_by_code.rename(
    columns={
        "MATERIAL": "MATERIAL",
        "PCS": "PCS",
        "BANNER": "REGION",
        "GSV": "GSV",
        "YEARWEEK": "YEARWEEK",
    }
)
df_sec_sales_by_code["MATERIAL"] = df_sec_sales_by_code["MATERIAL"].astype(int)
df_sec_sales_by_code["YEARWEEK"] = df_sec_sales_by_code["YEARWEEK"].astype(int)
df_sec_sales_by_code["GSV"] = df_sec_sales_by_code["GSV"].astype(float)
df_sec_sales_by_code["PCS"] = df_sec_sales_by_code["PCS"].astype(float)

df_sec_sales_by_code = df_sec_sales_by_code.merge(df_master_product, on="MATERIAL")
df_sec_sales_by_code["KG"] = df_sec_sales_by_code["PCS"] * df_sec_sales_by_code["KG/PCS"]
df_sec_sales_by_code["TON"] = df_sec_sales_by_code["KG"] / 1000
df_sec_sales_by_code["CS"] = df_sec_sales_by_code["PCS"] / df_sec_sales_by_code["PCS/CS"]

# COMMAND ----------

df_sec_sales_by_code["ACTUALSALE"] = df_sec_sales_by_code["TON"]
df_sec_sales_by_code["ACTUALSALE"].loc[
    (df_sec_sales_by_code["CATEGORY"].isin(["SKINCARE", "IC", "DEO"]))
] = df_sec_sales_by_code["CS"]
df_sec_sales_by_code["ACTUALSALE"].loc[(df_sec_sales_by_code["CATEGORY"].isin(["TBRUSH"]))] = (
    df_sec_sales_by_code["PCS"] / 1000
)

df_sec_sales_by_code = (
    df_sec_sales_by_code.groupby(["CATEGORY", "DPNAME", "YEARWEEK", "MATERIAL"])[
        ["ACTUALSALE", "KG", "CS", "GSV"]
    ]
    .sum()
    .reset_index()
)

df_sec_sales_by_code["YEARWEEK"] = df_sec_sales_by_code["YEARWEEK"].astype(int)

df_sec_sales_by_code["DATE"] = pd.to_datetime(df_sec_sales_by_code["YEARWEEK"].astype(str) + "-1", format="%G%V-%w")

# df_sec_sales_by_code = df_sec_sales_by_code.merge(df_calendar_workingday, on="YEARWEEK")

# df_sec_sales_by_code = df_sec_sales_by_code.merge(df_week_master, on = "YEARWEEK")

# df_sec_sales_by_code["QUARTER"] = ((df_sec_sales_by_code["MONTH"] - 1) / 3).astype(int) + 1

# COMMAND ----------

df_sec_sales_by_code = df_sec_sales_by_code.fillna(0)
df_sec_sales_by_code = df_sec_sales_by_code.sort_values(
    ["CATEGORY", "DPNAME", "MATERIAL", "YEARWEEK"]
).reset_index(drop=True)

print(df_sec_sales_by_code.shape)
df_sec_sales_by_code.head(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Promotion data

# COMMAND ----------

file_path = [
    "/dbfs/mnt/adls/NMHDAT_SNOP/DT/Promotion/LE_PROMO_2022.csv",
    "/dbfs/mnt/adls/NMHDAT_SNOP/DT/Promotion/LE_PROMO_2023.csv"
]
df_tpr = pd.concat([pd.read_csv(f) for f in file_path], ignore_index=True)

# COMMAND ----------

df_business_promotion = read_excel_folder(
    folder_path="/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/LANDING-PROMO/",
    sheet_name=None,
)
df_business_promotion.head(3)

# COMMAND ----------

df_business_promotion.describe(include = "all")

# COMMAND ----------

df_holiday = pd.read_csv("/dbfs/mnt/adls/BHX_FC/FBFC/DATA/CALENDAR.csv")
df_holiday.head(2)

# COMMAND ----------

df_holiday.describe(include = "all")

# COMMAND ----------

df_banded = pd.read_csv("/dbfs/mnt/adls/NMHDAT_SNOP/DT/Promotion/promotion_by_code.csv")
print(df_banded.shape)
df_banded.head(2)

# COMMAND ----------

df_banded = df_banded[["Start Date Promotion", "End Date Promotion","Material","Short Mechanic"]]
df_banded.columns = ["START_DATE_PROMOTION","END_DATE_PROMOTION","MATERIAL","SHORT_MECHANIC"]

df_banded["START_DATE_PROMOTION"] = pd.to_datetime(df_banded["START_DATE_PROMOTION"])
df_banded["END_DATE_PROMOTION"] = pd.to_datetime(df_banded["END_DATE_PROMOTION"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Convert Sec2Pri Baseline with ratio_phasing Weekly/Month 2 years latest

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

df_convert = df_sec_sales.copy()
df_convert = df_convert.rename(columns={"ACTUALSALE": "SEC_SALES", "BASELINE": "SEC_BASELINE"})

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
    how="outer",
    on=["CATEGORY", "DPNAME", "YEARWEEK"],
)

df_convert["DATE"] = pd.to_datetime(df_convert["YEARWEEK"].astype(str) + "-1", format="%G%V-%w")

df_convert = df_convert.rename(columns={"ACTUALSALE": "PRI_SALES"})

df_convert = df_convert.sort_values(["KEY", "YEARWEEK"]).reset_index(drop=True)

df_convert["FC_PRI_BASELINE"] = 0
df_convert["FC_PRI_BASELINE_WEEKLY"] = 0
df_convert["SELLINGDAY_WEEK/MONTH RATIO_median"] = 0

df_convert = df_convert[df_convert["YEARWEEK"] <= df_pri_sales["YEARWEEK"].max()]

# COMMAND ----------

dict_promotion_time = {}

for year_idx in df_pri_sales["YEAR"].unique():
    if year_idx >= 2020:
        dict_promotion_time[year_idx] = []
        for month_idx in df_convert[
            (df_convert["YEAR"] == year_idx)
            & (df_convert["YEARWEEK"] <= df_pri_sales["YEARWEEK"].max())
        ]["MONTH"].unique():
            dict_promotion_time[year_idx].append(int(month_idx))

        dict_promotion_time[year_idx] = sorted(dict_promotion_time[year_idx])
dict_promotion_time

# COMMAND ----------

from dateutil.relativedelta import relativedelta

for year_idx in dict_promotion_time.keys():
    for month_idx in dict_promotion_time[year_idx]:

        start_date = datetime.datetime(year = year_idx, month = month_idx, day = 1) - relativedelta(months= 24)
        to_date = datetime.datetime(year = year_idx, month = month_idx, day = 28) - relativedelta(months= 1)
        df_ratio_phasing = df_convert[
            (df_convert["DATE"] >= start_date)
            & (df_convert["DATE"] <= to_date)
        ]

        df_ratio_WEEK_MONTH = create_ratio_phasing(df_ratio_phasing, "PRI_SALES")

        df_convert_pattern = convert_data(
            df_convert[
                (df_convert["YEAR"] == year_idx) & (df_convert["MONTH"] == month_idx)
            ],
            df_ratio_WEEK_MONTH,
            input_var = "SEC_BASELINE",
            output_var = "FC_PRI_BASELINE_WEEKLY"
        )

        df_convert_pattern = df_convert_pattern.reset_index(drop=True)  
        indices = df_convert[(df_convert["YEAR"] == year_idx) & (df_convert["MONTH"] == month_idx)].index 
        positions = df_convert.index.get_indexer(indices) 
        df_convert.iloc[positions] = df_convert_pattern

# COMMAND ----------

df_convert = df_convert.rename(columns = {"SELLINGDAY_WEEK/MONTH RATIO_median": "RATIO_median"})
df_convert = df_convert.sort_values(["KEY","YEARWEEK"]).reset_index(drop = True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Main product TPR data

# COMMAND ----------

df_product_tpr = df_pri_sales[["BANNER","REGION","CATEGORY","DPNAME","YEARWEEK","ACTUALSALE"]].copy()
df_product_tpr = df_product_tpr.rename(columns = {"ACTUALSALE" : "PRI_SALES"})

df_product_tpr = df_product_tpr.merge(df_sec_sales[["BANNER","CATEGORY","DPNAME","YEARWEEK","ACTUALSALE","BASELINE"]], on = ["BANNER","CATEGORY","DPNAME","YEARWEEK"], how = "outer")

df_product_tpr = df_product_tpr.rename(columns = {"ACTUALSALE" : "SEC_SALES", "BASELINE":"SEC_BASELINE"})

df_product_tpr = df_product_tpr[(df_product_tpr["YEARWEEK"] <= 202352)]

df_product_tpr["DATE"] = pd.to_datetime(df_product_tpr["YEARWEEK"].astype(str) + "-1", format = "%G%V-%w")

df_product_tpr = df_product_tpr.merge(df_calendar_workingday, on="YEARWEEK")

df_product_tpr = df_product_tpr.merge(df_week_master, on = "YEARWEEK")

df_product_tpr["QUARTER"] = ((df_product_tpr["MONTH"] - 1) / 3).astype(int) + 1

df_product_tpr["WEEK_MONTH_COUNT"] = (
        df_product_tpr.groupby(["DPNAME", "YEAR", "MONTH"])["YEARWEEK"].transform("count").astype(int)
)
df_product_tpr["WEEK_OF_MONTH"] = (df_product_tpr["DATE"].dt.day - 1) // 7 + 1
df_product_tpr["WEEK_OF_QUARTER"] = (df_product_tpr["YEARWEEK"] % 100 - 1) % 13 + 1 
df_product_tpr["WEEK_OF_YEAR"] = df_product_tpr["YEARWEEK"] % 100
df_product_tpr["MONTH_OF_QUARTER"] = (df_product_tpr["MONTH"] -1) % 4 + 1

print(df_product_tpr.shape)
df_product_tpr.head(2)

# COMMAND ----------

df_convert_temp = df_convert[
    [
        "CATEGORY",
        "DPNAME",
        "YEARWEEK",
        # "SEC_SALES",
        # "PRI_SALES",
        "FC_PRI_BASELINE_WEEKLY",
        "RATIO_median"
        # "SEC_BASELINE",
    ]
]

df_convert_temp = df_convert_temp.rename(
    columns={
        "PRI_SALES": "PRI_SALES_DP",
        "SEC_SALES": "SEC_SALES_DP",
        "SEC_BASELINE": "SEC_BASELINE_WEEKLY",
        "FC_PRI_BASELINE_WEEKLY": "PRI_BASELINE"
    }
)
print(df_convert_temp.shape)
df_convert_temp.head(2)

# COMMAND ----------

df_product_tpr = df_product_tpr.merge(
    df_convert_temp,
    on=["CATEGORY", "DPNAME", "YEARWEEK"],
    how="left",
)

df_product_tpr["UOM_PRODUCT"] = "TON"
df_product_tpr["UOM_PRODUCT"][df_product_tpr["CATEGORY"].isin(["TBRUSH"])] = "KPCS"
df_product_tpr["UOM_PRODUCT"][df_product_tpr["CATEGORY"].isin(["IC","DEO","SKINCARE"])] = "CS"

# COMMAND ----------

df_business_promotion = df_business_promotion.groupby(["CATEGORY","DPNAME","YEARWEEK"])["ABNORMAL"].min().reset_index()
df_business_promotion["YEARWEEK"] = df_business_promotion["YEARWEEK"].astype(int)
print(df_business_promotion["ABNORMAL"].value_counts())

df_product_tpr = df_product_tpr.merge(df_business_promotion, on = ["CATEGORY","DPNAME","YEARWEEK"], how = "left")

# COMMAND ----------

df_holiday["YEARWEEK"] = df_holiday["YEARWEEK"].astype(int)

df_product_tpr = df_product_tpr.merge(
    df_holiday[
        [
            "YEARWEEK",
            "HOLIDAYNAME",
            "SPECIALEVENTNAME",
            "MONTHENDFACTOR",
            "QUARTERTYPE",
            "DAYTYPE_CLOSING",
            "DAYTYPE_OPENING",
            "DAYTYPE_NORMAL",
        ]
    ],
    on="YEARWEEK",
    how="left",
)

# COMMAND ----------

print(
    df_product_tpr[df_product_tpr["DPNAME"].str.contains("DELISTED")].shape[0]
    / df_product_tpr.shape[0]
)
df_product_tpr = df_product_tpr[
    (~df_product_tpr["DPNAME"].str.contains("DELISTED"))
]

df_product_tpr = df_product_tpr[(df_product_tpr["PRI_SALES"] >= 0) & (df_product_tpr["YEARWEEK"] >= 202001)]
df_product_tpr = df_product_tpr.drop(["BANNER","REGION"], axis = 1)

df_product_tpr = df_product_tpr.sort_values(["CATEGORY","DPNAME","YEARWEEK"]).reset_index(drop = True).fillna(0)
df_product_tpr.shape

# COMMAND ----------

df_backup = df_product_tpr.copy()
df_product_tpr.describe(include = "all")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Timeseries features

# COMMAND ----------

df_temp = df_product_tpr[["CATEGORY","DPNAME","DATE","PRI_SALES"]].copy()

df_explode_date = pd.DataFrame(
    data={
        "DPNAME": df_product_tpr["DPNAME"].unique(),
        "DATE_RANGE": [
            pd.date_range(
                start=df_product_tpr[df_product_tpr["DPNAME"] == dpname]["DATE"].min(),
                end=df_product_tpr[df_product_tpr["DPNAME"] == dpname]["DATE"].max(),
                freq="W-MON",
            )
            for dpname in df_product_tpr["DPNAME"].unique()
        ],
    }
)
df_explode_date = df_explode_date.explode("DATE_RANGE")
df_explode_date.columns = ["DPNAME","DATE"]

# COMMAND ----------

df_temp = df_temp.merge(df_explode_date, on =["DPNAME", "DATE"], how = "right")
df_temp = df_temp.fillna(0)

# COMMAND ----------

# Lag
for lag_number in [52, 53, 54, 55, 59, 63]:
    df_temp[f"lag_{lag_number}"] = df_temp.groupby(["CATEGORY","DPNAME"])["PRI_SALES"].shift(lag_number)

# lag then moving
# Moving avg, median, min, max
for lag_number in [52, 56]:
    for moving_number in [2, 3, 4, 8, 12]:
        df_temp[f"lag_{lag_number}_moving_mean_{moving_number}"] = df_temp.groupby(["CATEGORY","DPNAME"])[
            "PRI_SALES"
        ].apply(lambda x: x.shift(lag_number).rolling(window=moving_number).mean())
        df_temp[f"lag_{lag_number}_moving_std_{moving_number}"] = df_temp.groupby(["CATEGORY","DPNAME"])[
            "PRI_SALES"
        ].apply(lambda x: x.shift(lag_number).rolling(window=moving_number).std())
        df_temp[f"lag_{lag_number}_moving_median_{moving_number}"] = df_temp.groupby(["CATEGORY","DPNAME"])[
            "PRI_SALES"
        ].apply(lambda x: x.shift(lag_number).rolling(window=moving_number).median())
        df_temp[f"lag_{lag_number}_moving_min_{moving_number}"] = df_temp.groupby(["CATEGORY","DPNAME"])[
            "PRI_SALES"
        ].apply(lambda x: x.shift(lag_number).rolling(window=moving_number).min())
        df_temp[f"lag_{lag_number}_moving_max_{moving_number}"] = df_temp.groupby(["CATEGORY","DPNAME"])[
            "PRI_SALES"
        ].apply(lambda x: x.shift(lag_number).rolling(window=moving_number).max())

df_temp = df_temp.fillna(0)
df_temp.shape 

# COMMAND ----------

display(df_temp[df_temp["DPNAME"] == "OMO RED 6000 GR"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Base model

# COMMAND ----------

!pip install --upgrade pip
!pip install catboost

# COMMAND ----------

from sklearn import tree
from sklearn.tree import DecisionTreeRegressor, export_graphviz
from sklearn.ensemble import RandomForestRegressor, AdaBoostRegressor, ExtraTreesRegressor
from xgboost import XGBRegressor, plot_tree
from catboost import CatBoostRegressor
from lightgbm import LGBMRegressor

from sklearn.metrics import (
    mean_absolute_error as MAE,
    mean_squared_error as MSE,
    mean_absolute_percentage_error as MAPE,
)

# from sklearn.pipeline import Pipeline
# from sklearn.model_selection import GridSearchCV, KFold
# from sklearn.feature_selection import RFE
# from sklearn.inspection import permutation_importance
# import shap

# COMMAND ----------

def accuracy_check(key, df_group, actual_col, predict_col_arr):
    df_group[actual_col] = df_group[actual_col].fillna(0)

    performance = dict()
    sum_actualsale = df_group[actual_col].sum()

    performance = {"CATEGORY": key, "Sum_actual": sum_actualsale}
    performance["Mean_actual"] = df_group[actual_col].mean()
    performance["Std_actual"] = df_group[actual_col].std()
    
    for predict_col in predict_col_arr:
        # df_group[predict_col] = df_group[predict_col].fillna(0)
        # df_group[predict_col] = df_group[predict_col].replace([-np.inf, np.inf], 0)

        error = sum((df_group[actual_col] - df_group[predict_col]).abs())
        accuracy = 1 - error / df_group[actual_col].sum()
        sum_predictsale = df_group[predict_col].sum()

        performance["Sum_predictsale_" + predict_col] = sum_predictsale
        performance["FA_" + predict_col] = accuracy
        performance["Error_" + predict_col] = error
        performance["MAE_" + predict_col] = MAE(df_group[actual_col], df_group[predict_col])
        performance["RMSE_" + predict_col] = MSE(df_group[actual_col], df_group[predict_col], squared = False)

    return performance

# COMMAND ----------

df_model = df_product_tpr.copy()
df_model = df_model.merge(df_temp.drop("PRI_SALES", axis = 1), on = ["CATEGORY","DPNAME", "DATE"], how = "left")

# COMMAND ----------

display(df_model.corr()[["PRI_SALES"]].sort_values(by = "PRI_SALES", ascending = False).reset_index())

# COMMAND ----------

df_model.head(2)

# COMMAND ----------

# train from 202001 to 202252 
# test from 202301 to 202352
X_train = df_model[(df_model["YEARWEEK"] <= 202252) & (df_model["YEARWEEK"] >= 202001)].drop(
    [
        "DATE",
        "PRI_SALES",
        "SEC_SALES",
    ],
    axis=1,
)
Y_train = df_model[(df_model["YEARWEEK"] <= 202252)  & (df_model["YEARWEEK"] >= 202001)]["PRI_SALES"]

X_test = df_model[df_model["YEARWEEK"] >= 202301].drop(
    [
        "DATE",
        "PRI_SALES",
        "SEC_SALES",
    ],
    axis=1,
)
Y_test = df_model[df_model["YEARWEEK"] >= 202301]["PRI_SALES"]

X_train.shape, Y_train.shape, X_test.shape, Y_test.shape

# COMMAND ----------

categorical_cols = X_train.select_dtypes(include=[object]).columns.values
print(categorical_cols)

X_train[X_train.columns.difference(categorical_cols)] = X_train[X_train.columns.difference(categorical_cols)].fillna(0)
X_test[X_test.columns.difference(categorical_cols)] = X_test[X_test.columns.difference(categorical_cols)].fillna(0)

X_train[categorical_cols] =X_train[categorical_cols].astype("category")
X_test[categorical_cols] =X_test[categorical_cols].astype("category") 
# X_train = X_train.drop(categorical_cols, axis = 1)
# X_test = X_test.drop(categorical_cols, axis = 1)

# COMMAND ----------

model = XGBRegressor(enable_categorical = True, tree_method = "hist")
model.fit(X_train, Y_train)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Evaluate 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### overall

# COMMAND ----------

y_pred = model.predict(X_test)
df_evaluate = X_test.copy()
df_evaluate["ACTUAL_TOTAL"] = Y_test
df_evaluate["PREDICTED_TOTAL"] = y_pred

df_evaluate["ACTUAL_UPLIFT"] = df_evaluate["ACTUAL_TOTAL"] - df_evaluate["PRI_BASELINE"]
df_evaluate["PREDICTED_UPLIFT"] = df_evaluate["PREDICTED_TOTAL"] - df_evaluate["PRI_BASELINE"]

accuracy_check("total", df_evaluate, "ACTUAL_TOTAL", ["PREDICTED_TOTAL"])

# COMMAND ----------

df_evaluate[["ACTUAL_TOTAL","PREDICTED_TOTAL","PRI_BASELINE","ACTUAL_UPLIFT","PREDICTED_UPLIFT","YEARWEEK"]].describe()

# COMMAND ----------

df_temp = df_evaluate.groupby("YEARWEEK")[["ACTUAL_TOTAL", "PREDICTED_TOTAL","ACTUAL_UPLIFT","PREDICTED_UPLIFT"]].sum().reset_index()
df_temp["YEARWEEK"] = df_temp["YEARWEEK"].astype(str)

print((df_temp["ACTUAL_TOTAL"].sum() - df_temp["PREDICTED_TOTAL"].sum()) / df_temp["ACTUAL_TOTAL"].sum())
px.line(df_temp, x = "YEARWEEK", y = ["ACTUAL_TOTAL","PREDICTED_TOTAL","ACTUAL_UPLIFT","PREDICTED_UPLIFT"])

# COMMAND ----------

df_agg_total = df_evaluate.groupby(["YEARWEEK"])[["ACTUAL_TOTAL","PREDICTED_TOTAL"]].sum().reset_index()
df_agg_total["FA"] =  1 - (df_agg_total["ACTUAL_TOTAL"] - df_agg_total["PREDICTED_TOTAL"]).abs() / df_agg_total["ACTUAL_TOTAL"]
df_agg_total["FA"] = df_agg_total["FA"].replace([-np.inf, np.inf, 0]).fillna(0)
display(df_agg_total)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Cate, bin

# COMMAND ----------

display(pd.DataFrame.from_dict(
        [
            accuracy_check(key, df_group, "ACTUAL_TOTAL", ["PREDICTED_TOTAL"])
            for key, df_group in  df_evaluate.groupby("UOM_PRODUCT")
        ]
))

# COMMAND ----------

display(pd.DataFrame.from_dict(
        [
            accuracy_check(key, df_group, "ACTUAL_TOTAL", ["PREDICTED_TOTAL"])
            for key, df_group in  df_evaluate.groupby("CATEGORY")
        ]
))

# COMMAND ----------

df_agg_cate = df_evaluate.groupby(["CATEGORY","YEARWEEK"])[["ACTUAL_TOTAL","PREDICTED_TOTAL"]].sum().reset_index()
# df_agg_cate = df_evaluate.copy()
df_agg_cate["FA"] =  1 - (df_agg_cate["ACTUAL_TOTAL"] - df_agg_cate["PREDICTED_TOTAL"]).abs() / df_agg_cate["ACTUAL_TOTAL"]
df_agg_cate["FA"] = df_agg_cate["FA"].replace([-np.inf, np.inf, 0]).fillna(0)
display(df_agg_cate)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### DP

# COMMAND ----------

df_evaluate["FA"] =  1 - (df_evaluate["ACTUAL_TOTAL"] - df_evaluate["PREDICTED_TOTAL"]).abs() / df_evaluate["ACTUAL_TOTAL"]
df_evaluate["FA"] = df_evaluate["FA"].replace([-np.inf, np.inf], 0).fillna(0)
df_evaluate["FA"][df_evaluate["FA"] < 0] = 0

# COMMAND ----------

display(df_evaluate[df_evaluate["DPNAME"] == "OMO RED 6000 GR"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### something

# COMMAND ----------

feature_importance = model.feature_importances_[:15]
sorted_idx = np.argsort(feature_importance)
fig = plt.figure(figsize=(12, 6))
plt.barh(range(len(sorted_idx)), feature_importance[sorted_idx], align='center')
plt.yticks(range(len(sorted_idx)), np.array(X_test.columns)[sorted_idx])
plt.title('Feature Importance')

# COMMAND ----------

# df_export = df_product_tpr.merge(df_evaluate[["CATEGORY","DPNAME","YEARWEEK", "ACTUAL_UPLIFT","PREDICTED_UPLIFT"]], on = ["CATEGORY","DPNAME","YEARWEEK"], how = "left")
# df_export.to_csv("/dbfs/mnt/adls/NMHDAT_SNOP/test_eval_tpr.csv", index = False)

# COMMAND ----------

for dpname in [
    "OMO LIQUID MATIC CFT SS (POU) 3.7KG",
    "OMO RED 6000 GR",
    "SUNLIGHT LEMON 3600G",
]:
    temp_plot = df_evaluate[(df_evaluate["DPNAME"] == dpname)]
    temp_plot["YEARWEEK"] = temp_plot["YEARWEEK"].astype(str)
    print("error",(temp_plot["ACTUAL_TOTAL"] - temp_plot["PREDICTED_TOTAL"]).abs().sum())
    print("FA",1- ( temp_plot["ACTUAL_TOTAL"] - temp_plot["PREDICTED_TOTAL"]).abs().sum() / temp_plot["ACTUAL_TOTAL"].sum())
    fig = go.Figure(
        data=[
            go.Scatter(
                x=temp_plot["YEARWEEK"],
                y=temp_plot["ACTUAL_TOTAL"],
                name="actual uplift",
                marker={"color": "green"},
            ),
            go.Scatter(
                x=temp_plot["YEARWEEK"],
                y=temp_plot["PREDICTED_TOTAL"],
                name="predicted uplift",
                connectgaps=False,
                marker={"color": "red"},
                mode="lines+markers",
            ),
        ]
    )
    fig.update_layout(title=dpname, hovermode= "x unified")

    fig.show()

# COMMAND ----------

perm_importance = permutation_importance(model, X_test, Y_test, n_repeats=10, random_state=1066)
sorted_idx = perm_importance.importances_mean.argsort()
fig = plt.figure(figsize=(12, 6))
plt.barh(range(len(sorted_idx)), perm_importance.importances_mean[sorted_idx], align='center')
plt.yticks(range(len(sorted_idx)), np.array(X_test.columns)[sorted_idx])
plt.title('Permutation Importance')

# COMMAND ----------

# explainer = shap.Explainer(model)
# shap_values = explainer(X_test)
# shap_importance = shap_values.abs.mean(0).values
# sorted_idx = shap_importance.argsort()
# fig = plt.figure(figsize=(12, 6))
# plt.barh(range(len(sorted_idx)), shap_importance[sorted_idx], align='center')
# plt.yticks(range(len(sorted_idx)), np.array(X_test.columns)[sorted_idx])
# plt.title('SHAP Importance')
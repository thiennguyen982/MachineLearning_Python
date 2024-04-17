# Databricks notebook source
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

def read_excel_file(file_path, sheet_name, skiprows=0):
    print(f"{file_path} - Sheet {sheet_name}")
    if sheet_name != None:
        df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=skiprows)
    else:
        df = pd.read_excel(file_path, skiprows=skiprows)
    return df


def read_excel_folder(folder_path, sheet_name, skiprows=0):
    folder_items = os.listdir(folder_path)
    tasks = [
        read_excel_file(
            folder_path + "/" + file_item, sheet_name, skiprows
        )
        for file_item in folder_items
    ]
    result = pd.concat(tasks)
    return result

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

# MAGIC %md
# MAGIC
# MAGIC create dataframe aggregated primary sales data to level SKU weekly

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

# MAGIC %md
# MAGIC create dataframe aggregated level DPNAME weekly

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

# MAGIC %md
# MAGIC
# MAGIC load secondary sales from service of DT SNOP Baseline Generator

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
# MAGIC ## Promotion TPR

# COMMAND ----------

df_business_promotion = read_excel_folder(
    folder_path="/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/LANDING-PROMO/",
    sheet_name=None,
)
df_business_promotion.head(3)

# COMMAND ----------

df_holiday = pd.read_csv("/dbfs/mnt/adls/BHX_FC/FBFC/DATA/CALENDAR.csv")
df_holiday.head(2)

# COMMAND ----------

df_holiday.describe(include = "all")

# COMMAND ----------

file_path = [
    "/dbfs/mnt/adls/NMHDAT_SNOP/DT/Promotion/LE_PROMO_2022.csv",
    "/dbfs/mnt/adls/NMHDAT_SNOP/DT/Promotion/LE_PROMO_2023.csv"
]
df_tpr = pd.concat([pd.read_csv(f) for f in file_path], ignore_index=True)

# COMMAND ----------

df_tpr = df_tpr.dropna(subset = ["DPNAME"])
df_tpr = df_tpr[~df_tpr["DPNAME"].isin(["UNKOWN","Not Assigned"])]
df_tpr["BUY_ITEM"] = df_tpr["BUY_ITEM"].astype(int)
df_tpr = df_tpr[df_tpr["REGION"].isin(["DT HCME","DT MEKONG DELTA","DT NORTH","DT CENTRAL"])]
# df_tpr = df_tpr[df_tpr["REGION"] != "UNKNOWN"] # REGION have deal_status = de-activated & CATEGORY = HNH, same DEAL ID but different DPNAME

df_tpr["START_DATE"] = pd.to_datetime(df_tpr["START_DATE"], format = "%Y%m%d")
df_tpr["END_DATE"] = np.where(df_tpr["STOP_DATE"] == 0, df_tpr["ORIG_END_DATE"], df_tpr["STOP_DATE"])
df_tpr["END_DATE"] = pd.to_datetime(df_tpr["END_DATE"], format="%Y%m%d")
df_tpr["ORIG_END_DATE"] = pd.to_datetime(df_tpr["ORIG_END_DATE"], format = "%Y%m%d")
df_tpr = df_tpr.drop_duplicates()

# COMMAND ----------

print(df_tpr.shape)
df_tpr.head(3)

# COMMAND ----------

plt.pie(df_tpr["REGION"].value_counts(), labels=df_tpr["REGION"].value_counts().index, autopct='%.2f%%')

# COMMAND ----------

display(df_tpr[df_tpr["DEAL_DESCRIPTION"].isnull()])

# COMMAND ----------

df_tpr["START_DATE"].min(), df_tpr["START_DATE"].max(), df_tpr["END_DATE"].min(), df_tpr["END_DATE"].max()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Promotion banded

# COMMAND ----------

# MAGIC %md
# MAGIC comment below is code for load primary sales data from HANA and transformation to calculate total sales of SKU have banded promotion
# MAGIC
# MAGIC But this is old version to exclude total sales of banded, the transformation of banded replace by loading result of service rule-based banded promotion and exclude uplift banded

# COMMAND ----------

# df_banded = pd.read_csv("/dbfs/mnt/adls/NMHDAT_SNOP/DT/Promotion/promotion_by_code.csv")
# print(df_banded.shape)
# df_banded.head(2)

# COMMAND ----------

# df_banded = df_banded[["Start Date Promotion", "End Date Promotion","Material","Short Mechanic"]]
# df_banded.columns = ["START_DATE_PROMOTION","END_DATE_PROMOTION","MATERIAL","SHORT_MECHANIC"]

# df_banded["START_DATE_PROMOTION"] = pd.to_datetime(df_banded["START_DATE_PROMOTION"])
# df_banded["END_DATE_PROMOTION"] = pd.to_datetime(df_banded["END_DATE_PROMOTION"])

# COMMAND ----------

# df_mapping_banded = pd.read_excel("/dbfs/mnt/adls/NMHDAT_SNOP/DT/Promotion/SALE_BOM_NOV_15.xlsx")
# print(df_mapping_banded.shape)
# df_mapping_banded.head(2)

# COMMAND ----------

# df_mapping_banded = df_mapping_banded[df_mapping_banded["Banded Material"].isin(df_banded["MATERIAL"].unique())].drop_duplicates()
# df_mapping_banded = df_mapping_banded[df_mapping_banded["Plant"].isin(["V101","V102","V103","V104","V105","V106"])]

# df_mapping_banded = df_mapping_banded.drop_duplicates(subset = ["Banded Material","Item Material"])

# df_mapping_banded = df_mapping_banded[
#     (
#         df_mapping_banded["Banded Material"]
#         .astype(int)
#         .isin(df_pri_sales_by_code["MATERIAL"].astype(int).unique())
#     )
#     & (
#         df_mapping_banded["Item Material"].isin(
#             df_pri_sales_by_code["MATERIAL"].astype(int).unique()
#         )
#     )
# ]

# COMMAND ----------

# # Source Primary Sales HANA

# from pyspark.sql import functions as F

# NUMBER_OF_PARTITIONS = sc.defaultParallelism * 2

# DF_HANA_PRI_SALES = spark.read.csv(
#     "dbfs:/mnt/adls/Prod_UDL/TechDebt/InternalSources/U2K2BW/OpenHubFileDestination/PrimarySales/Vietnam/Processed",
#     sep="|",
#     header=True,
# )
# DF_HANA_CUSTOMER_MASTER = spark.read.format("delta").load(
#     "dbfs:/mnt/adls/Prod_UDL/TechDebt/InternalSources/U2K2BW/OpenHubFileDestination/CustomerMaster/SouthEastAsiaAustralasia/Processed_Parquet"
# )

# DF_HANA_CUSTOMER_MASTER = (
#     DF_HANA_CUSTOMER_MASTER.select("CUST_SALES", "COUNTRY", "ZCUSTNAME4", "ZCUSTNAME16")
#     .filter(DF_HANA_CUSTOMER_MASTER.COUNTRY == "VN")
#     .dropDuplicates()
# )

# DF_HANA_PRI_SALES = DF_HANA_PRI_SALES.join(DF_HANA_CUSTOMER_MASTER).where(
#     (DF_HANA_PRI_SALES.CUSTOMER == DF_HANA_CUSTOMER_MASTER.CUST_SALES)
# )

# DF_HANA_PRI_SALES = DF_HANA_PRI_SALES.filter(F.col('MATERIAL').cast("Integer").isin(df_mapping_banded["Banded Material"].astype(int).unique().tolist()))

# DF_HANA_PRI_SALES = DF_HANA_PRI_SALES.withColumn("G_QABSMG", F.when(F.col("G_QABSMG") > 0, F.col("G_QABSMG")).otherwise(0))

# DF_HANA_PRI_SALES = DF_HANA_PRI_SALES.groupBy(
#     "ZCUSTNAME4", "ZCUSTNAME16", "CALDAY", "CALWEEK", "MATERIAL"
# ).agg(
#     F.sum("G_QABSMG").alias("PCS"),
#     F.sum("G_AVV010").alias("GSV"),
# )

# rename_header = {
#     "CALDAY": "DATE",
#     "CALWEEK": "YEARWEEK",
#     "ZCUSTNAME4": "BANNER",
#     "ZCUSTNAME16": "REGION",
#     "MATERIAL": "MATERIAL",
#     "PCS": "PCS",
#     "GSV": "GSV",
# }
# for key, value in rename_header.items():
#     DF_HANA_PRI_SALES = DF_HANA_PRI_SALES.withColumnRenamed(key, value)

# DF_HANA_PRI_SALES_WEEKLY = DF_HANA_PRI_SALES.groupBy(
#     "YEARWEEK", "BANNER", "REGION", "MATERIAL"
# ).agg(F.sum("PCS").alias("PCS"), F.sum("GSV").alias("GSV"))

# COMMAND ----------

# df_pri_hana_banded = DF_HANA_PRI_SALES_WEEKLY.toPandas()

# df_pri_hana_banded = df_pri_hana_banded.dropna()
# df_pri_hana_banded["MATERIAL"] = df_pri_hana_banded["MATERIAL"].astype(int)

# df_pri_hana_banded = df_pri_hana_banded[
#     df_pri_hana_banded["BANNER"].isin(
#         ["DT HCME", "DT MEKONG DELTA", "DT North", "DT CENTRAL"]
#     )
# ]
# df_pri_hana_banded = df_pri_hana_banded.groupby(["YEARWEEK", "MATERIAL"])["PCS"].sum().reset_index()
# df_pri_hana_banded["REGION"] = "NATIONWIDE"
# df_pri_hana_banded["BANNER"] = "NATIONWIDE"

# df_pri_hana_banded = df_pri_hana_banded.merge(df_master_product, on="MATERIAL")

# df_pri_hana_banded["TON"] = df_pri_hana_banded["PCS"] * df_pri_hana_banded["KG/PCS"] / 1000
# df_pri_hana_banded["CS"] = df_pri_hana_banded["PCS"] / df_pri_hana_banded["PCS/CS"]

# COMMAND ----------

# df_pri_hana_banded = (
#     df_pri_hana_banded.groupby(["YEARWEEK", "BANNER", "REGION", "CATEGORY", "DPNAME","MATERIAL"])[
#         ["PCS", "TON", "CS"]
#     ]
#     .sum()
#     .reset_index()
# )

# df_pri_hana_banded["ACTUALSALE"] = df_pri_hana_banded["TON"]
# df_pri_hana_banded["ACTUALSALE"].loc[
#     (df_pri_hana_banded["CATEGORY"].isin(["SKINCARE", "IC", "DEO"]))
# ] = df_pri_hana_banded["CS"]
# df_pri_hana_banded["ACTUALSALE"].loc[(df_pri_hana_banded["CATEGORY"].isin(["TBRUSH"]))] = (
#     df_pri_hana_banded["PCS"] / 1000
# )

# df_pri_hana_banded["YEARWEEK"] = df_pri_hana_banded["YEARWEEK"].astype(int)
# df_pri_hana_banded = df_pri_hana_banded.rename(columns = {"ACTUALSALE":"BANDED_SALES_SKU"})
# df_pri_hana_banded = df_pri_hana_banded.fillna(0)
# df_pri_hana_banded = df_pri_hana_banded.sort_values(["DPNAME","MATERIAL","YEARWEEK"]).reset_index(drop = True)

# print(df_pri_hana_banded.shape)
# df_pri_hana_banded.head(3)

# COMMAND ----------

# df_pri_sales_exclude_banded = df_pri_sales_by_code.merge(
#     df_pri_hana_banded[
#         ["CATEGORY", "DPNAME", "MATERIAL", "YEARWEEK", "BANDED_SALES_SKU"]
#     ],
#     on=["CATEGORY", "DPNAME", "MATERIAL", "YEARWEEK"],
#     how="left",
# )

# df_pri_sales_exclude_banded = df_pri_sales_exclude_banded.merge(
#     df_master_product[["MATERIAL","SKU_TYPE"]], on=["MATERIAL"], how = "left"
# )


# df_pri_sales_exclude_banded["ACTUALSALE_TRANSFORM"] = np.where(
#     df_pri_sales_exclude_banded["BANDED_SALES_SKU"].isnull(),
#     df_pri_sales_exclude_banded["ACTUALSALE"],
#     df_pri_sales_exclude_banded["ACTUALSALE"]
#     - df_pri_sales_exclude_banded["BANDED_SALES_SKU"],
# )

# df_pri_sales_exclude_banded["ACTUALSALE_TRANSFORM"] = np.where(
#     (df_pri_sales_exclude_banded["SKU_TYPE"] != "NORMAL")
#     & (df_pri_sales_exclude_banded["DATE"] < df_banded["START_DATE_PROMOTION"].min()),
#     0,
#     df_pri_sales_exclude_banded["ACTUALSALE_TRANSFORM"],
# )

# COMMAND ----------

# df_pri_sales_exclude_banded = (
#     df_pri_sales_exclude_banded.groupby(
#         ["BANNER", "REGION", "CATEGORY", "DPNAME", "YEARWEEK"]
#     )[["ACTUALSALE", "ACTUALSALE_TRANSFORM", "BANDED_SALES_SKU"]]
#     .sum()
#     .reset_index()
# )

# df_pri_sales_exclude_banded = df_pri_sales_exclude_banded.rename(
#     columns={
#         "ACTUALSALE_TRANSFORM": "PRI_SALES_WITHOUT_BANDED",
#         "BANDED_SALES_SKU": "BANDED_SALES_DP",
#     }
# )
# df_pri_sales_exclude_banded.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC load result from rule-base banded promotion service and calculate uplift banded to exclude in total sales

# COMMAND ----------

df_uplift_banded = read_excel_folder("/dbfs/mnt/adls/ds_vn/service/dt_snop/promotion_banded/Master_Data/",sheet_name="DATA")
print(df_uplift_banded.shape)
df_uplift_banded.head(3)

# COMMAND ----------

df_uplift_banded = df_uplift_banded[df_uplift_banded["CONTRIBUTE_MATERIAL"].between(-2, 2)]

df_uplift_banded["UPLIFT_BANDED"] = df_uplift_banded["PRELOAD_VOLUME"] + df_uplift_banded["POSTLOAD_VOLUME"]

df_uplift_banded["BANDED_COUNT"] = 1

# COMMAND ----------

df_uplift_banded.describe()

# COMMAND ----------

df_uplift_banded = df_uplift_banded.groupby(["CATEGORY","DPNAME","YEARWEEK"])[["UPLIFT_BANDED","BANDED_COUNT"]].sum().reset_index()
display(df_uplift_banded)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Convert Sec2Pri Baseline

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

# sec baseline train before 202326 to predict 202327-202352
df_sec_baseline_pred = pd.read_csv("/dbfs/mnt/adls/NMHDAT_SNOP/DT/FC_BASELINE_SECONDARY/FC_BL_TRAIN_TO202326.csv")
print(df_sec_baseline_pred.shape)
df_sec_baseline_pred.describe()

# COMMAND ----------

df_convert = df_sec_sales.copy()
df_convert = df_convert.rename(columns={"ACTUALSALE": "SEC_SALES", "BASELINE": "SEC_BASELINE"})

df_convert = df_convert.merge(df_sec_baseline_pred[["KEY","YEARWEEK","BL_Sec_weekly"]], on = ["KEY","YEARWEEK"], how = "left")

df_convert["SEC_BASELINE"][df_convert["YEARWEEK"].between(202327, 202352)] = df_convert["BL_Sec_weekly"][df_convert["YEARWEEK"].between(202327, 202352)].fillna(0)

df_convert = df_convert.reset_index(drop = True)

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
# MAGIC # Preprocessing TPR data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select TPR data follow SKU product

# COMMAND ----------

# Load pri & sec sales data level SKU

df_promotion = df_pri_sales_by_code.copy()
df_promotion = df_promotion.rename(columns={"ACTUALSALE": "PRI_SALES"})

df_promotion = df_promotion.merge(
    df_sec_sales_by_code[["CATEGORY", "DPNAME", "YEARWEEK", "MATERIAL", "ACTUALSALE"]],
    on=["CATEGORY", "DPNAME", "MATERIAL", "YEARWEEK"],
    how="outer",
)

df_promotion["BANNER"] = "NATIONWIDE"
df_promotion = df_promotion.rename(columns={"ACTUALSALE": "SEC_SALES"})
df_promotion = df_promotion[~df_promotion["DPNAME"].str.contains("DELISTED|DELETED")]

df_promotion = df_promotion[
    (df_promotion["DATE"] >= df_tpr["START_DATE"].min()) & (df_promotion["DATE"] <= df_tpr["END_DATE"].max())
    # & (df_promotion["MATERIAL"] >= 6e7)
]
df_promotion = df_promotion.sort_values(["BANNER","CATEGORY","DPNAME","YEARWEEK","MATERIAL"]).reset_index(drop = True)
df_promotion["MATERIAL"] = df_promotion["MATERIAL"].astype(int)

df_promotion["DATE"] = pd.to_datetime(df_promotion["YEARWEEK"].astype(str) + "-1", format = "%G%V-%w")
df_promotion["YEARWEEK"] = df_promotion["YEARWEEK"].astype(int)
df_promotion = df_promotion.merge(df_calendar_workingday, on="YEARWEEK")

df_promotion = df_promotion.merge(df_week_master, on = "YEARWEEK")

df_promotion["QUARTER"] = ((df_promotion["MONTH"] - 1) / 3).astype(int) + 1

print(df_promotion.shape)
df_promotion.head(2)

# COMMAND ----------

df_convert_temp = df_convert[
    [
        "CATEGORY",
        "DPNAME",
        "YEARWEEK",
        "SEC_SALES",
        "PRI_SALES",
        "FC_PRI_BASELINE_WEEKLY",
        "SEC_BASELINE"
    ]
]

df_convert_temp = df_convert_temp.rename(
    columns={
        "PRI_SALES": "PRI_SALES_DP",
        "SEC_SALES": "SEC_SALES_DP",
        "SEC_BASELINE": "SEC_BASELINE_WEEKLY",
        "FC_PRI_BASELINE_WEEKLY": "PRI_BASELINE_WEEKLY"
    }
)
print(df_convert_temp.shape)

df_promotion = df_promotion.merge(
    df_convert_temp,
    on=["CATEGORY", "DPNAME", "YEARWEEK"],
    how="left",
)
df_promotion.shape

# COMMAND ----------

# MAGIC %md
# MAGIC cleaning tpr data and explode datetime to weekly frequency
# MAGIC

# COMMAND ----------

df_temp = df_tpr.copy()
df_temp = df_temp[df_temp["BUY_ITEM"].isin(df_promotion["MATERIAL"].unique())]
df_temp = df_temp[~df_temp["DEAL_STATUS"].isin(["De-Activated","Stopped"])]
df_temp = df_temp[df_temp["END_DATE"] >= df_temp["START_DATE"]]
df_temp = df_temp.drop_duplicates()
print(df_temp.shape)
# df_temp = df_temp[df_temp["BUY_ITEM"] >= 6e7]

# COMMAND ----------

df_explode =spark.createDataFrame(df_temp)
df_explode = df_explode.withColumn("EXPLODED_DATE",F.explode(F.expr("sequence(START_DATE, END_DATE, interval 7 day)")))
print(df_explode.count())

# COMMAND ----------

df_temp = df_explode.toPandas()
df_temp = df_temp.rename(
    columns={"BUY_ITEM": "MATERIAL"}
)
df_temp["MATERIAL"] = df_temp["MATERIAL"].astype(int)

df_temp["YEARWEEK"] = (
    df_temp["EXPLODED_DATE"].dt.isocalendar()["year"] * 100
    + df_temp["EXPLODED_DATE"].dt.isocalendar()["week"]
).astype(int)

df_temp.head(3)

# COMMAND ----------

df_promotion = df_promotion.drop(["REGION"], axis = 1).merge(
    df_temp.drop(["DPNAME","CATEGORY"], axis = 1),
    on=["YEARWEEK", "MATERIAL"],
    how="left",
).sort_values(["CATEGORY","DPNAME","YEARWEEK","MATERIAL","DEAL_ID", "START_DATE", "END_DATE"]).reset_index(drop = True)

df_promotion.shape

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature selection, generation

# COMMAND ----------

df_promotion = df_promotion.drop(
    [
        "PCS",
        "CS",
        "TON",
        "PH_LEVEL",
        "BANDED",
        "EXPLODED_DATE",
    ],
    axis=1,
)

df_promotion = df_promotion.sort_values(
    ["CATEGORY", "DPNAME", "YEARWEEK", "MATERIAL", "START_DATE", "END_DATE"]
).reset_index(drop=True)

df_promotion["TOTAL_UPLIFT"] = (
    df_promotion["PRI_SALES_DP"] - df_promotion["PRI_BASELINE_WEEKLY"]
)
df_promotion["DURATION_TPR_DAILY"] = (
    df_promotion["END_DATE"] - df_promotion["START_DATE"]
) / np.timedelta64(1, "D")

df_promotion[
    ["INVESTMENT_TYPE", "SCHEME_ID", "REGION_DEAL_ID", "PROMO_SEGMENT"]
] = np.nan
df_promotion["INVESTMENT_TYPE"][df_promotion["DEAL_ID"].notnull()] = (
    df_promotion["DEAL_ID"].astype(str).str[:2]
)
df_promotion["SCHEME_ID"][df_promotion["DEAL_ID"].notnull()] = (
    df_promotion["DEAL_ID"].astype(str).str[2:10]
)
df_promotion["REGION_DEAL_ID"][df_promotion["DEAL_ID"].notnull()] = (
    df_promotion["DEAL_ID"].astype(str).str[10:12]
)
df_promotion["PROMO_SEGMENT"][df_promotion["DEAL_ID"].notnull()] = (
    df_promotion["DEAL_ID"].astype(str).str[12:]
)

# COMMAND ----------

df_promotion.describe()

# COMMAND ----------

df_promotion[["MECHANIC_TYPE","BUY_TYPE","MIN_BUY_UOM"]].value_counts(sort=False, normalize = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Promotion UOM & Price

# COMMAND ----------

# ## reading in LE article master
# filepath = '/mnt/adls/promo_analytics/Landing/mapping_files/le_article_master.csv'
# spark.read.format("csv").options(delimiter="|", header="True").load(filepath).createOrReplaceTempView("article_master")

df_uom = pd.read_csv("/dbfs/mnt/adls/NMHDAT_SNOP/DT/Promotion/promotion_uom_price.csv")
df_uom = df_uom.drop("Unnamed: 0", axis = 1)
print(df_uom.shape)
print(df_uom.nunique())
df_uom.head(3)

# COMMAND ----------

df_uom = df_uom[~df_uom["product_code"].str.contains("G|P")]
df_uom["product_code"] = df_uom["product_code"].astype(int)

# COMMAND ----------

df_donthave_uom = df_promotion[
    (~df_promotion["MATERIAL"].isin(df_uom["product_code"].unique()))
    & (df_promotion["DEAL_ID"].notnull())
]
print(df_donthave_uom.shape[0] / df_promotion.shape[0])
df_donthave_uom[["CATEGORY"]].value_counts(normalize=True)

# COMMAND ----------

# take uom & price data for product
df_uom = df_uom[["product_code","price_per_su","price_per_pc","price_per_cs"]]
df_promotion = df_promotion.merge(df_uom, left_on = "MATERIAL", right_on = "product_code", how = "left")

# take uom & price data for gift
df_uom.columns = ["GIFT_CODE","price_gift_per_su","price_gift_per_pc","price_gift_per_cs"]
df_promotion = df_promotion.merge(df_uom, on = "GIFT_CODE", how = "left")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### BENEFIT TPR features

# COMMAND ----------

# CAN IMPROVE BY GROUPBY MIN_BUY_UOM
# Or transform all output MIN_BUY_UOM to VND but dont have price_per_ton
# Can add feature cost = min_buy - benefit_tpr (make sense for mechanic GIFT)

df_promotion["BENEFIT_TPR"] = np.where(
    df_promotion["MECHANIC_TYPE"] == "PERCENTAGE",
    np.where(
        df_promotion["MIN_BUY_UOM"] == "VND",
        # consider when MIN_BUY 1VND: take price_per_uom * PCT_DISCOUNT, only DEO & Savoury have but DEO can calculate by price_per_cs, Savoury dont have infor price_per_ton
        df_promotion["PCT_DISCOUNT"] * df_promotion["MIN_BUY"] / 100,
        np.where(
            df_promotion["MIN_BUY_UOM"] == "SU",
            df_promotion["PCT_DISCOUNT"] * df_promotion["MIN_BUY"] * df_promotion["price_per_su"] / 100,
            np.where(
                df_promotion["MIN_BUY_UOM"] == "CS",
                df_promotion["PCT_DISCOUNT"] * df_promotion["MIN_BUY"] * df_promotion["price_per_cs"] / 100,
                df_promotion["PCT_DISCOUNT"] * df_promotion["MIN_BUY"] * df_promotion["price_per_pc"] / 100,
            ),
        ),
    ),
    np.where(
        df_promotion["MECHANIC_TYPE"] == "VALUE",
        df_promotion["VAL_DISCOUNT"],
        np.where(
            df_promotion["MECHANIC_TYPE"].isin(["U GIFT", "NON-U GIFT"]),
            np.where(
                df_promotion["GIFT_UOM"] == "SU",
                df_promotion["GIFT_QTY"] * df_promotion["price_gift_per_su"],
                np.where(
                    df_promotion["GIFT_UOM"] == "CS",
                    df_promotion["GIFT_QTY"] * df_promotion["price_gift_per_cs"],
                    df_promotion["GIFT_QTY"] * df_promotion["price_gift_per_pc"],
                ),
            ),
            0,
        ),
    ),
)


df_promotion["BENEFIT_TPR_PER_MIN_BUY"] = np.where(
    df_promotion["MECHANIC_TYPE"] == "PERCENTAGE",
    np.where(
        df_promotion["MIN_BUY_UOM"] == "VND",
        df_promotion["PCT_DISCOUNT"] / df_promotion["MIN_BUY"] * 1000,
        np.where(
            df_promotion["MIN_BUY_UOM"] == "SU",
            df_promotion["PCT_DISCOUNT"] / df_promotion["MIN_BUY"],
            np.where(
                df_promotion["MIN_BUY_UOM"] == "CS",
                df_promotion["PCT_DISCOUNT"] / df_promotion["MIN_BUY"],
                df_promotion["PCT_DISCOUNT"] / df_promotion["MIN_BUY"],
            ),
        ),
    ),
    np.where(
        df_promotion["MECHANIC_TYPE"] == "VALUE",
        df_promotion["VAL_DISCOUNT"] / df_promotion["MIN_BUY"],
        np.where(
            df_promotion["MECHANIC_TYPE"].isin(["U GIFT", "NON-U GIFT"]),
            np.where(
                df_promotion["MIN_BUY_UOM"] == "VND",
                df_promotion["GIFT_QTY"] / df_promotion["MIN_BUY"] * 1000 * np.where(
                    df_promotion["GIFT_UOM"] == "SU",
                    df_promotion["price_gift_per_su"],
                    np.where(
                        df_promotion["GIFT_UOM"] == "CS",
                        df_promotion["price_gift_per_cs"],
                        df_promotion["price_gift_per_pc"],
                    ),
                ),
                np.where( # else MIN_BUY_UOM is SU or CS
                    df_promotion["GIFT_UOM"] == "SU",
                    df_promotion["GIFT_QTY"] * df_promotion["price_gift_per_su"] / df_promotion["MIN_BUY"],
                    np.where(
                        df_promotion["GIFT_UOM"] == "CS",
                        df_promotion["GIFT_QTY"] * df_promotion["price_gift_per_cs"] / df_promotion["MIN_BUY"],
                        df_promotion["GIFT_QTY"] * df_promotion["price_gift_per_pc"] / df_promotion["MIN_BUY"],
                    ),
                ),
            ),
            0,
        ),
    ),
)

# COMMAND ----------

display(df_promotion[(df_promotion["MIN_BUY"] == 1) & (df_promotion["MIN_BUY_UOM"] == "VND")])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Datetime Features

# COMMAND ----------

# MAGIC %md
# MAGIC Note: if preprocessing features for future data, need to replace END_DATE by ORIN_END_DATE which is end date planning

# COMMAND ----------

df_promotion["START_YEARWEEK"] = df_promotion["START_DATE"].dt.isocalendar()["year"] * 100 + df_promotion["START_DATE"].dt.isocalendar()["week"]

df_promotion["END_YEARWEEK"] = df_promotion["END_DATE"].dt.isocalendar()["year"] * 100 + df_promotion["END_DATE"].dt.isocalendar()["week"]

df_promotion[["START_YEARWEEK","END_YEARWEEK"]] = df_promotion[["START_YEARWEEK","END_YEARWEEK"]].fillna(0)

# COMMAND ----------

df_promotion["START_END_SAME_WEEK"] = np.where(
    (df_promotion["START_YEARWEEK"] == df_promotion["END_YEARWEEK"]) & (df_promotion["START_YEARWEEK"] > 0), 1, 0,
)

df_promotion["START_CURRENT_WEEK"] = np.where(
    df_promotion["START_YEARWEEK"] == df_promotion["YEARWEEK"], 1, 0
)
df_promotion["END_CURRENT_WEEK"] = np.where(df_promotion["END_YEARWEEK"] == df_promotion["YEARWEEK"], 1, 0)

df_promotion["START_FIRST_WEEK_OF_MONTH"] = np.where(
    (((df_promotion["START_DATE"].dt.day - 1) // 7 + 1) == 1)
    & (df_promotion["START_YEARWEEK"] == df_promotion["YEARWEEK"]), 
    1, 
    0
)
df_promotion["START_LAST_WEEK_OF_MONTH"] = np.where(
    (((df_promotion["START_DATE"].dt.day - 1) // 7 + 1) > 3) 
    & (df_promotion["START_YEARWEEK"] == df_promotion["YEARWEEK"]),
    1, 0
)

# use ORIG_END_DATE in the future
df_promotion["END_FIRST_WEEK_OF_MONTH"] = np.where(
    (((df_promotion["END_DATE"].dt.day - 1) // 7 + 1) == 1) 
    & (df_promotion["END_YEARWEEK"] == df_promotion["YEARWEEK"])
    , 1, 0
)
df_promotion["END_LAST_WEEK_OF_MONTH"] = np.where(
    (((df_promotion["END_DATE"].dt.day - 1) // 7 + 1) > 3) 
    & (df_promotion["END_YEARWEEK"] == df_promotion["YEARWEEK"])
    , 1, 0
)

df_promotion["START_FIRST_WEEK_OF_QUARTER"] = np.where(
    ((df_promotion["START_YEARWEEK"] % 100 % 13) == 1)
    & (df_promotion["START_YEARWEEK"] == df_promotion["YEARWEEK"]), 
    1, 0
)
df_promotion["START_LAST_WEEK_OF_QUARTER"] = np.where(
    ((df_promotion["START_YEARWEEK"] % 100 % 13) == 0)
    & (df_promotion["START_YEARWEEK"] == df_promotion["YEARWEEK"]), 
    1, 0
)

df_promotion["END_FIRST_WEEK_OF_QUARTER"] = np.where(
    ((df_promotion["END_YEARWEEK"] % 100 % 13) == 1)
    & (df_promotion["END_YEARWEEK"] == df_promotion["YEARWEEK"]),
    1, 0
)
df_promotion["END_LAST_WEEK_OF_QUARTER"] = np.where(
    ((df_promotion["END_YEARWEEK"] % 100 % 13) == 0)
    & (df_promotion["END_YEARWEEK"] == df_promotion["YEARWEEK"]), 
    1, 0
)

df_promotion["DURATION_TPR_1_WEEK"] = np.where(df_promotion["DURATION_TPR_DAILY"] < 8, 1, 0)
df_promotion["DURATION_TPR_2_WEEK"] = np.where(
    (df_promotion["DURATION_TPR_DAILY"] < 15) & (df_promotion["DURATION_TPR_DAILY"] > 7), 1, 0
)
df_promotion["DURATION_TPR_UNDER_1_MONTH"] = np.where(
    (df_promotion["DURATION_TPR_DAILY"] < 31), 1, 0
)
df_promotion["DURATION_TPR_OVER_1_MONTH"] = np.where(
    df_promotion["DURATION_TPR_DAILY"] > 30, 1, 0
)
# add feature count distinct month TPR pass through


# COMMAND ----------

list_feature = ["START_NEXT_WEEK", "START_NEXT_2_WEEK", "START_LAST_WEEK", "START_LAST_2_WEEK", "END_NEXT_WEEK", "END_NEXT_2_WEEK", "END_LAST_WEEK", "END_LAST_2_WEEK"]

df_promotion[list_feature] = 0

df_promotion.loc[((df_promotion["DATE"] - df_promotion["START_DATE"]) / np.timedelta64(1, "W")).between(-1, 0, inclusive = "left"),"START_NEXT_WEEK"] = 1
df_promotion.loc[((df_promotion["DATE"] - df_promotion["START_DATE"]) / np.timedelta64(1, "W")).between(-2, -1, inclusive = "left"),"START_NEXT_2_WEEK"] = 1
df_promotion.loc[((df_promotion["DATE"] - df_promotion["START_DATE"]) / np.timedelta64(1, "W")).between(0, 1, inclusive = "right"),"START_LAST_WEEK"] = 1
df_promotion.loc[((df_promotion["DATE"] - df_promotion["START_DATE"]) / np.timedelta64(1, "W")).between(1, 2, inclusive = "right"),"START_LAST_2_WEEK"] = 1

df_promotion.loc[((df_promotion["DATE"] - df_promotion["END_DATE"]) / np.timedelta64(1, "W")).between(-1, 0, inclusive = "left"),"END_NEXT_WEEK"] = 1
df_promotion.loc[((df_promotion["DATE"] - df_promotion["END_DATE"]) / np.timedelta64(1, "W")).between(-2, -1, inclusive = "left"),"END_NEXT_2_WEEK"] = 1
df_promotion.loc[((df_promotion["DATE"] - df_promotion["END_DATE"]) / np.timedelta64(1, "W")).between(0, 1, inclusive = "right"),"END_LAST_WEEK"] = 1
df_promotion.loc[((df_promotion["DATE"] - df_promotion["END_DATE"]) / np.timedelta64(1, "W")).between(1, 2, inclusive = "right"),"END_LAST_2_WEEK"] = 1

for feature in list_feature:
    df_promotion[feature] = df_promotion.groupby(["CATEGORY","DPNAME","YEARWEEK","MATERIAL","SCHEME_ID","PROMO_SEGMENT"])[feature].transform("sum")
    df_promotion[feature] = df_promotion[feature].fillna(0)

# COMMAND ----------

print(df_promotion[df_promotion.duplicated(subset = ["CATEGORY", "DPNAME", "YEARWEEK", "MATERIAL", "SCHEME_ID" ,"PROMO_SEGMENT"])].shape[0]/df_promotion.shape[0])

df_promotion = df_promotion.drop_duplicates(subset = ["CATEGORY","DPNAME","YEARWEEK","MATERIAL","SCHEME_ID","PROMO_SEGMENT"])

# COMMAND ----------

display(df_promotion[(df_promotion["DPNAME"] == "OMO RED 6000 GR") & (df_promotion["YEARWEEK"] == 202331)])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## De-duplicates TPR features

# COMMAND ----------

def sum_str_values(series):
    result = [val for val in series if val is not None]
    return result

# COMMAND ----------

df_tpr_dp = pd.pivot_table(
    df_promotion,
    index=["CATEGORY", 
           "DPNAME",
           "YEARWEEK", 
        #    "MATERIAL", 
        #    "SCHEME_ID", 
        #    "PROMO_SEGMENT"
    ],
    columns=["MECHANIC_TYPE"],
    values=[
        "DEAL_TYPE",
        "MIN_BUY",
        "MIN_BUY_UOM",
        "VAL_DISCOUNT",
        "PCT_DISCOUNT",
        "GIFT_QTY",
        "GIFT_UOM",
        "BENEFIT_TPR",
        "BENEFIT_TPR_PER_MIN_BUY",
        "DURATION_TPR_DAILY",
        "DURATION_TPR_1_WEEK",
        "DURATION_TPR_2_WEEK",
        "DURATION_TPR_UNDER_1_MONTH",
        "DURATION_TPR_OVER_1_MONTH",
        "START_END_SAME_WEEK",
        "START_CURRENT_WEEK",
        "END_CURRENT_WEEK",
        "START_FIRST_WEEK_OF_MONTH",
        "START_LAST_WEEK_OF_MONTH",
        "END_FIRST_WEEK_OF_MONTH",
        "END_LAST_WEEK_OF_MONTH",
        "START_FIRST_WEEK_OF_QUARTER",
        "START_LAST_WEEK_OF_QUARTER",
        "END_FIRST_WEEK_OF_QUARTER",
        "END_LAST_WEEK_OF_QUARTER",
        "START_NEXT_WEEK",
        "START_NEXT_2_WEEK",
        "START_LAST_WEEK",
        "START_LAST_2_WEEK",
        "END_NEXT_WEEK",
        "END_NEXT_2_WEEK",
        "END_LAST_WEEK",
        "END_LAST_2_WEEK",
    ],
    aggfunc={
        "DEAL_TYPE": ["count", sum_str_values],
        "MIN_BUY": ["min", "mean", "median", "max"],
        "MIN_BUY_UOM": sum_str_values,
        "VAL_DISCOUNT": "median",  # no need other statistics cause VALUE DISCOUNT is not duplicate
        "PCT_DISCOUNT": ["min", "mean", "median", "max"],
        "GIFT_QTY": ["min", "median", "max"],
        "GIFT_UOM": sum_str_values,
        "BENEFIT_TPR": ["min", "mean", "median", "max", "sum"],
        "BENEFIT_TPR_PER_MIN_BUY": ["min", "median", "mean", "max"],
        "DURATION_TPR_DAILY": ["min", "mean", "median", "max"],
        # cannot sum cause all same DEAL_ID are duplicate by requirements or gift_code => take max
        "START_END_SAME_WEEK": "sum",
        "START_CURRENT_WEEK": "sum",
        "END_CURRENT_WEEK": "sum",
        "DURATION_TPR_1_WEEK": "sum",
        "DURATION_TPR_2_WEEK": "sum",
        "DURATION_TPR_UNDER_1_MONTH": "sum",
        "DURATION_TPR_OVER_1_MONTH": "sum",
        "START_FIRST_WEEK_OF_MONTH": "sum",
        "START_LAST_WEEK_OF_MONTH": "sum",
        "END_FIRST_WEEK_OF_MONTH": "sum",
        "END_LAST_WEEK_OF_MONTH": "sum",
        "START_FIRST_WEEK_OF_QUARTER": "sum",
        "START_LAST_WEEK_OF_QUARTER": "sum",
        "END_FIRST_WEEK_OF_QUARTER": "sum",
        "END_LAST_WEEK_OF_QUARTER": "sum",
        "START_NEXT_WEEK": "sum",
        "START_NEXT_2_WEEK": "sum",
        "START_LAST_WEEK": "sum",
        "START_LAST_2_WEEK": "sum",
        "END_NEXT_WEEK": "sum",
        "END_NEXT_2_WEEK": "sum",
        "END_LAST_WEEK": "sum",
        "END_LAST_2_WEEK": "sum",
    },
    # fill_value = 0,
)
df_tpr_dp

# COMMAND ----------

df_tpr_dp = df_tpr_dp.reset_index()
df_tpr_dp.columns = ["_".join(col) for col in df_tpr_dp.columns]
df_tpr_dp = df_tpr_dp.rename(
    columns={
        "CATEGORY__": "CATEGORY",
        "DPNAME__": "DPNAME",
        "YEARWEEK__": "YEARWEEK",
        # "MATERIAL__": "MATERIAL",
        # "SCHEME_ID__": "SCHEME_ID",
        # "PROMO_SEGMENT__": "PROMO_SEGMENT",
    }
)

# COMMAND ----------

# Encoding categorical features
for mechanic in ["NON-U GIFT", "U GIFT", "PERCENTAGE", "VALUE"]:
    for deal_type in df_promotion["DEAL_TYPE"].unique():
        if isinstance(deal_type, str):
            df_tpr_dp[f"DEAL_TYPE_{deal_type}_count_in_{mechanic}"] = df_tpr_dp[
                f"DEAL_TYPE_sum_str_values_{mechanic}"
            ].str.count(deal_type)
            df_tpr_dp[f"DEAL_TYPE_{deal_type}_count_in_{mechanic}"] = df_tpr_dp[
                f"DEAL_TYPE_{deal_type}_count_in_{mechanic}"
            ].fillna(0)

for mechanic in ["NON-U GIFT", "U GIFT"]:
    for uom in ["SU", "CS"]:
        df_tpr_dp[f"GIFT_UOM_{uom}_count_in_{mechanic}"] = df_tpr_dp[
            f"GIFT_UOM_sum_str_values_{mechanic}"
        ].str.count(uom)
        df_tpr_dp[f"GIFT_UOM_{uom}_count_in_{mechanic}"] = df_tpr_dp[
            f"GIFT_UOM_{uom}_count_in_{mechanic}"
        ].fillna(0)

for mechanic in ["NON-U GIFT", "U GIFT", "PERCENTAGE", "VALUE"]:
    for uom in ["VND", "SU", "CS"]:
        df_tpr_dp[f"MIN_BUY_UOM_{uom}_count_in_{mechanic}"] = df_tpr_dp[
            f"MIN_BUY_UOM_sum_str_values_{mechanic}"
        ].str.count(uom)
        df_tpr_dp[f"MIN_BUY_UOM_{uom}_count_in_{mechanic}"] = df_tpr_dp[
            f"MIN_BUY_UOM_{uom}_count_in_{mechanic}"
        ].fillna(0)

df_tpr_dp = df_tpr_dp.drop(
    [
        "DEAL_TYPE_sum_str_values_NON-U GIFT",
        "DEAL_TYPE_sum_str_values_PERCENTAGE",
        "DEAL_TYPE_sum_str_values_U GIFT",
        "DEAL_TYPE_sum_str_values_VALUE",
        "GIFT_UOM_sum_str_values_NON-U GIFT",
        "GIFT_UOM_sum_str_values_PERCENTAGE",
        "GIFT_UOM_sum_str_values_U GIFT",
        "GIFT_UOM_sum_str_values_VALUE",
        "MIN_BUY_UOM_sum_str_values_NON-U GIFT",
        "MIN_BUY_UOM_sum_str_values_PERCENTAGE",
        "MIN_BUY_UOM_sum_str_values_U GIFT",
        "MIN_BUY_UOM_sum_str_values_VALUE",
    ],
    axis=1,
)

check_col_zero = df_tpr_dp.max() == 0
check_col_zero = check_col_zero[check_col_zero == True].index
df_tpr_dp = df_tpr_dp.drop(check_col_zero, axis=1)
print(check_col_zero)
df_tpr_dp.shape

# COMMAND ----------

df_tpr_dp.describe(include = "all")

# COMMAND ----------

# MAGIC %md
# MAGIC Below comment is an old version aggregation from SCHEME_ID - SKU - DPNAME not like current (above) version from SCHEME_ID to DPNAME.
# MAGIC
# MAGIC It can use for feature engineering from business sense in each level aggregation which discussed in next step to create NET_TPR feature: https://bnlwe-p-56728-ia-01-unilevercom-vsts.visualstudio.com/Vietnam%20DnA/_wiki/wikis/Vietnam-DnA.wiki/3393/Methodologies

# COMMAND ----------

# Agg DPNAME|YEARWEEK|SKU|SCHEME_ID|PROMO_SEGMENT

# df_temp = pd.pivot_table(
#     df_promotion,
#     index=["CATEGORY", 
#            "DPNAME",
#            "YEARWEEK", 
#            "MATERIAL", 
#            "SCHEME_ID", 
#            "PROMO_SEGMENT"
#     ],
#     columns=["MECHANIC_TYPE"],
#     values=[
#         "DEAL_TYPE",
#         "MIN_BUY",
#         "MIN_BUY_UOM",
#         "VAL_DISCOUNT",
#         "PCT_DISCOUNT",
#         "GIFT_QTY",
#         "GIFT_UOM",
#         "BENEFIT_TPR",
#         "BENEFIT_TPR_PER_MIN_BUY",
#         "DURATION_TPR_DAILY",
#         "DURATION_TPR_1_WEEK",
#         "DURATION_TPR_2_WEEK",
#         "DURATION_TPR_UNDER_1_MONTH",
#         "DURATION_TPR_OVER_1_MONTH",
#         "START_END_SAME_WEEK",
#         "START_CURRENT_WEEK",
#         "END_CURRENT_WEEK",
#         "START_FIRST_WEEK_OF_MONTH",
#         "START_LAST_WEEK_OF_MONTH",
#         "END_FIRST_WEEK_OF_MONTH",
#         "END_LAST_WEEK_OF_MONTH",
#         "START_FIRST_WEEK_OF_QUARTER",
#         "START_LAST_WEEK_OF_QUARTER",
#         "END_FIRST_WEEK_OF_QUARTER",
#         "END_LAST_WEEK_OF_QUARTER",
#         "START_NEXT_WEEK",
#         "START_NEXT_2_WEEK",
#         "START_LAST_WEEK",
#         "START_LAST_2_WEEK",
#         "END_NEXT_WEEK",
#         "END_NEXT_2_WEEK",
#         "END_LAST_WEEK",
#         "END_LAST_2_WEEK",
#     ],
#     aggfunc={
#         "DEAL_TYPE": ["count", sum_str_values],
#         "MIN_BUY": ["min", "mean", "median", "max"],
#         "MIN_BUY_UOM": sum_str_values,
#         "VAL_DISCOUNT": "median",  # no need other statistics cause VALUE DISCOUNT is not duplicate
#         "PCT_DISCOUNT": ["min", "mean", "median", "max"],
#         "GIFT_QTY": ["min", "median", "max"],
#         "GIFT_UOM": sum_str_values,
#         "BENEFIT_TPR": ["min", "mean", "median", "max", "sum"],
#         "BENEFIT_TPR_PER_MIN_BUY": ["min", "median", "mean", "max"],
#         "DURATION_TPR_DAILY": ["min", "mean", "median", "max"],
#         # cannot sum cause all same DEAL_ID are duplicate by requirements or gift_code => take max
#         "START_END_SAME_WEEK": "sum",
#         "START_CURRENT_WEEK": "sum",
#         "END_CURRENT_WEEK": "sum",
#         "DURATION_TPR_1_WEEK": "sum",
#         "DURATION_TPR_2_WEEK": "sum",
#         "DURATION_TPR_UNDER_1_MONTH": "sum",
#         "DURATION_TPR_OVER_1_MONTH": "sum",
#         "START_FIRST_WEEK_OF_MONTH": "sum",
#         "START_LAST_WEEK_OF_MONTH": "sum",
#         "END_FIRST_WEEK_OF_MONTH": "sum",
#         "END_LAST_WEEK_OF_MONTH": "sum",
#         "START_FIRST_WEEK_OF_QUARTER": "sum",
#         "START_LAST_WEEK_OF_QUARTER": "sum",
#         "END_FIRST_WEEK_OF_QUARTER": "sum",
#         "END_LAST_WEEK_OF_QUARTER": "sum",
#         "START_NEXT_WEEK": "sum",
#         "START_NEXT_2_WEEK": "sum",
#         "START_LAST_WEEK": "sum",
#         "START_LAST_2_WEEK": "sum",
#         "END_NEXT_WEEK": "sum",
#         "END_NEXT_2_WEEK": "sum",
#         "END_LAST_WEEK": "sum",
#         "END_LAST_2_WEEK": "sum",
#     },
#     # fill_value = 0,
# )

# df_temp = df_temp.reset_index()
# df_temp.columns = ["_".join(col) for col in df_temp.columns]
# df_temp = df_temp.rename(
#     columns={
#         "CATEGORY__": "CATEGORY",
#         "DPNAME__": "DPNAME",
#         "YEARWEEK__": "YEARWEEK",
#         "MATERIAL__": "MATERIAL",
#         "SCHEME_ID__": "SCHEME_ID",
#         "PROMO_SEGMENT__": "PROMO_SEGMENT",
#     }
# )

# COMMAND ----------

## Agg DP|YEARWEEK|SKU

# df_temp[df_temp.select_dtypes(include=[object]).columns.drop(["CATEGORY","DPNAME","SCHEME_ID","PROMO_SEGMENT"])] = df_temp[df_temp.select_dtypes(include=[object]).columns.drop(["CATEGORY","DPNAME","SCHEME_ID","PROMO_SEGMENT"])].fillna("NA")
#agg replace "unique" by " ".join than encode = .str.count()

# t = (
#     df_temp.groupby(["CATEGORY", "DPNAME", "YEARWEEK", "MATERIAL"])
#     .agg(
#         {
#             "BENEFIT_TPR_max_NON-U GIFT": "max",
#             "BENEFIT_TPR_max_PERCENTAGE": "max",
#             "BENEFIT_TPR_max_U GIFT": "max",
#             "BENEFIT_TPR_max_VALUE": "max",
#             "BENEFIT_TPR_mean_NON-U GIFT": "mean",
#             "BENEFIT_TPR_mean_PERCENTAGE": "mean",
#             "BENEFIT_TPR_mean_U GIFT": "mean",
#             "BENEFIT_TPR_mean_VALUE": "mean",
#             "BENEFIT_TPR_median_NON-U GIFT": "median",
#             "BENEFIT_TPR_median_PERCENTAGE": "median",
#             "BENEFIT_TPR_median_U GIFT": "median",
#             "BENEFIT_TPR_median_VALUE": "median",
#             "BENEFIT_TPR_min_NON-U GIFT": "min",
#             "BENEFIT_TPR_min_PERCENTAGE": "min",
#             "BENEFIT_TPR_min_U GIFT": "min",
#             "BENEFIT_TPR_min_VALUE": "min",
#             "BENEFIT_TPR_sum_NON-U GIFT": "sum",
#             "BENEFIT_TPR_sum_PERCENTAGE": "sum",
#             "BENEFIT_TPR_sum_U GIFT": "sum",
#             "BENEFIT_TPR_sum_VALUE": "sum",
#             "BENEFIT_TPR_PER_MIN_BUY_max_NON-U GIFT": "max",
#             "BENEFIT_TPR_PER_MIN_BUY_max_PERCENTAGE": "max",
#             "BENEFIT_TPR_PER_MIN_BUY_max_U GIFT": "max",
#             "BENEFIT_TPR_PER_MIN_BUY_max_VALUE": "max",
#             "BENEFIT_TPR_PER_MIN_BUY_mean_NON-U GIFT": "mean",
#             "BENEFIT_TPR_PER_MIN_BUY_mean_PERCENTAGE": "mean",
#             "BENEFIT_TPR_PER_MIN_BUY_mean_U GIFT": "mean",
#             "BENEFIT_TPR_PER_MIN_BUY_mean_VALUE": "mean",
#             "BENEFIT_TPR_PER_MIN_BUY_median_NON-U GIFT": "median",
#             "BENEFIT_TPR_PER_MIN_BUY_median_PERCENTAGE": "median",
#             "BENEFIT_TPR_PER_MIN_BUY_median_U GIFT": "median",
#             "BENEFIT_TPR_PER_MIN_BUY_median_VALUE": "median",
#             "BENEFIT_TPR_PER_MIN_BUY_min_NON-U GIFT": "min",
#             "BENEFIT_TPR_PER_MIN_BUY_min_PERCENTAGE": "min",
#             "BENEFIT_TPR_PER_MIN_BUY_min_U GIFT": "min",
#             "BENEFIT_TPR_PER_MIN_BUY_min_VALUE": "min",
#             "DEAL_TYPE_count_NON-U GIFT": "sum",
#             "DEAL_TYPE_count_PERCENTAGE": "sum",
#             "DEAL_TYPE_count_U GIFT": "sum",
#             "DEAL_TYPE_count_VALUE": "sum",
#             "DEAL_TYPE_sum_NON-U GIFT": "unique",
#             "DEAL_TYPE_sum_PERCENTAGE": "unique",
#             "DEAL_TYPE_sum_U GIFT": "unique",
#             "DEAL_TYPE_sum_VALUE": "unique",
#             "DURATION_TPR_1_WEEK_sum_NON-U GIFT": "sum",
#             "DURATION_TPR_1_WEEK_sum_PERCENTAGE": "sum",
#             "DURATION_TPR_1_WEEK_sum_U GIFT": "sum",
#             "DURATION_TPR_1_WEEK_sum_VALUE": "sum",
#             "DURATION_TPR_2_WEEK_sum_NON-U GIFT": "sum",
#             "DURATION_TPR_2_WEEK_sum_PERCENTAGE": "sum",
#             "DURATION_TPR_2_WEEK_sum_U GIFT": "sum",
#             "DURATION_TPR_2_WEEK_sum_VALUE": "sum",
#             "DURATION_TPR_DAILY_max_NON-U GIFT": "max",
#             "DURATION_TPR_DAILY_max_PERCENTAGE": "max",
#             "DURATION_TPR_DAILY_max_U GIFT": "max",
#             "DURATION_TPR_DAILY_max_VALUE": "max",
#             "DURATION_TPR_DAILY_mean_NON-U GIFT": "mean",
#             "DURATION_TPR_DAILY_mean_PERCENTAGE": "mean",
#             "DURATION_TPR_DAILY_mean_U GIFT": "mean",
#             "DURATION_TPR_DAILY_mean_VALUE": "mean",
#             "DURATION_TPR_DAILY_median_NON-U GIFT": "median",
#             "DURATION_TPR_DAILY_median_PERCENTAGE": "median",
#             "DURATION_TPR_DAILY_median_U GIFT": "median",
#             "DURATION_TPR_DAILY_median_VALUE": "median",
#             "DURATION_TPR_DAILY_min_NON-U GIFT": "min",
#             "DURATION_TPR_DAILY_min_PERCENTAGE": "min",
#             "DURATION_TPR_DAILY_min_U GIFT": "min",
#             "DURATION_TPR_DAILY_min_VALUE": "min",
#             "DURATION_TPR_OVER_1_MONTH_sum_NON-U GIFT": "sum",
#             "DURATION_TPR_OVER_1_MONTH_sum_PERCENTAGE": "sum",
#             "DURATION_TPR_OVER_1_MONTH_sum_U GIFT": "sum",
#             "DURATION_TPR_OVER_1_MONTH_sum_VALUE": "sum",
#             "DURATION_TPR_UNDER_1_MONTH_sum_NON-U GIFT": "sum",
#             "DURATION_TPR_UNDER_1_MONTH_sum_PERCENTAGE": "sum",
#             "DURATION_TPR_UNDER_1_MONTH_sum_U GIFT": "sum",
#             "DURATION_TPR_UNDER_1_MONTH_sum_VALUE": "sum",
#             "END_CURRENT_WEEK_sum_NON-U GIFT": "sum",
#             "END_CURRENT_WEEK_sum_PERCENTAGE": "sum",
#             "END_CURRENT_WEEK_sum_U GIFT": "sum",
#             "END_CURRENT_WEEK_sum_VALUE": "sum",
#             "END_FIRST_WEEK_OF_MONTH_sum_NON-U GIFT": "sum",
#             "END_FIRST_WEEK_OF_MONTH_sum_PERCENTAGE": "sum",
#             "END_FIRST_WEEK_OF_MONTH_sum_U GIFT": "sum",
#             "END_FIRST_WEEK_OF_MONTH_sum_VALUE": "sum",
#             "END_FIRST_WEEK_OF_QUARTER_sum_NON-U GIFT": "sum",
#             "END_FIRST_WEEK_OF_QUARTER_sum_PERCENTAGE": "sum",
#             "END_FIRST_WEEK_OF_QUARTER_sum_U GIFT": "sum",
#             "END_FIRST_WEEK_OF_QUARTER_sum_VALUE": "sum",
#             "END_LAST_WEEK_OF_MONTH_sum_NON-U GIFT": "sum",
#             "END_LAST_WEEK_OF_MONTH_sum_PERCENTAGE": "sum",
#             "END_LAST_WEEK_OF_MONTH_sum_U GIFT": "sum",
#             "END_LAST_WEEK_OF_MONTH_sum_VALUE": "sum",
#             "END_LAST_WEEK_OF_QUARTER_sum_NON-U GIFT": "sum",
#             "END_LAST_WEEK_OF_QUARTER_sum_PERCENTAGE": "sum",
#             "END_LAST_WEEK_OF_QUARTER_sum_U GIFT": "sum",
#             "END_LAST_WEEK_OF_QUARTER_sum_VALUE": "sum",
#             "END_LAST_2_WEEK_max_NON-U GIFT": "sum",
#             "END_LAST_2_WEEK_max_PERCENTAGE": "sum",
#             "END_LAST_2_WEEK_max_U GIFT": "sum",
#             "END_LAST_2_WEEK_max_VALUE": "sum",
#             "END_LAST_WEEK_max_NON-U GIFT": "sum",
#             "END_LAST_WEEK_max_PERCENTAGE": "sum",
#             "END_LAST_WEEK_max_U GIFT": "sum",
#             "END_LAST_WEEK_max_VALUE": "sum",
#             "END_NEXT_2_WEEK_max_NON-U GIFT": "sum",
#             "END_NEXT_2_WEEK_max_PERCENTAGE": "sum",
#             "END_NEXT_2_WEEK_max_U GIFT": "sum",
#             "END_NEXT_2_WEEK_max_VALUE": "sum",
#             "END_NEXT_WEEK_max_NON-U GIFT": "sum",
#             "END_NEXT_WEEK_max_PERCENTAGE": "sum",
#             "END_NEXT_WEEK_max_U GIFT": "sum",
#             "END_NEXT_WEEK_max_VALUE": "sum",
#             "GIFT_QTY_max_NON-U GIFT": "max",
#             "GIFT_QTY_max_U GIFT": "max",
#             "GIFT_QTY_min_NON-U GIFT": "min",
#             "GIFT_QTY_min_U GIFT": "min",
#             "GIFT_UOM_sum_NON-U GIFT": "unique",
#             "GIFT_UOM_sum_PERCENTAGE": "unique",
#             "GIFT_UOM_sum_U GIFT": "unique",
#             "GIFT_UOM_sum_VALUE": "unique",
#             "MIN_BUY_max_NON-U GIFT": "max",
#             "MIN_BUY_max_PERCENTAGE": "max",
#             "MIN_BUY_max_U GIFT": "max",
#             "MIN_BUY_max_VALUE": "max",
#             "MIN_BUY_median_NON-U GIFT": "median",
#             "MIN_BUY_median_PERCENTAGE": "median",
#             "MIN_BUY_median_U GIFT": "median",
#             "MIN_BUY_median_VALUE": "median",
#             "MIN_BUY_mean_NON-U GIFT": "mean",
#             "MIN_BUY_mean_PERCENTAGE": "mean",
#             "MIN_BUY_mean_U GIFT": "mean",
#             "MIN_BUY_mean_VALUE": "mean",
#             "MIN_BUY_min_NON-U GIFT": "min",
#             "MIN_BUY_min_PERCENTAGE": "min",
#             "MIN_BUY_min_U GIFT": "min",
#             "MIN_BUY_min_VALUE": "min",
#             "MIN_BUY_UOM_sum_NON-U GIFT": "unique",
#             "MIN_BUY_UOM_sum_PERCENTAGE": "unique",
#             "MIN_BUY_UOM_sum_U GIFT": "unique",
#             "MIN_BUY_UOM_sum_VALUE": "unique",
#             "PCT_DISCOUNT_max_PERCENTAGE": "max",
#             "PCT_DISCOUNT_median_PERCENTAGE": "median",
#             "PCT_DISCOUNT_mean_PERCENTAGE": "mean",
#             "PCT_DISCOUNT_min_PERCENTAGE": "min",
#             "START_CURRENT_WEEK_sum_NON-U GIFT": "sum",
#             "START_CURRENT_WEEK_sum_PERCENTAGE": "sum",
#             "START_CURRENT_WEEK_sum_U GIFT": "sum",
#             "START_CURRENT_WEEK_sum_VALUE": "sum",
#             "START_END_SAME_WEEK_sum_NON-U GIFT": "sum",
#             "START_END_SAME_WEEK_sum_PERCENTAGE": "sum",
#             "START_END_SAME_WEEK_sum_U GIFT": "sum",
#             "START_END_SAME_WEEK_sum_VALUE": "sum",
#             "START_FIRST_WEEK_OF_MONTH_sum_NON-U GIFT": "sum",
#             "START_FIRST_WEEK_OF_MONTH_sum_PERCENTAGE": "sum",
#             "START_FIRST_WEEK_OF_MONTH_sum_U GIFT": "sum",
#             "START_FIRST_WEEK_OF_MONTH_sum_VALUE": "sum",
#             "START_FIRST_WEEK_OF_QUARTER_sum_NON-U GIFT": "sum",
#             "START_FIRST_WEEK_OF_QUARTER_sum_PERCENTAGE": "sum",
#             "START_FIRST_WEEK_OF_QUARTER_sum_U GIFT": "sum",
#             "START_FIRST_WEEK_OF_QUARTER_sum_VALUE": "sum",
#             "START_LAST_WEEK_OF_MONTH_sum_NON-U GIFT": "sum",
#             "START_LAST_WEEK_OF_MONTH_sum_PERCENTAGE": "sum",
#             "START_LAST_WEEK_OF_MONTH_sum_U GIFT": "sum",
#             "START_LAST_WEEK_OF_MONTH_sum_VALUE": "sum",
#             "START_LAST_WEEK_OF_QUARTER_sum_NON-U GIFT": "sum",
#             "START_LAST_WEEK_OF_QUARTER_sum_PERCENTAGE": "sum",
#             "START_LAST_WEEK_OF_QUARTER_sum_U GIFT": "sum",
#             "START_LAST_WEEK_OF_QUARTER_sum_VALUE": "sum",
#             "START_LAST_2_WEEK_max_NON-U GIFT": "sum",
#             "START_LAST_2_WEEK_max_PERCENTAGE": "sum",
#             "START_LAST_2_WEEK_max_U GIFT": "sum",
#             "START_LAST_2_WEEK_max_VALUE": "sum",
#             "START_LAST_WEEK_max_NON-U GIFT": "sum",
#             "START_LAST_WEEK_max_PERCENTAGE": "sum",
#             "START_LAST_WEEK_max_U GIFT": "sum",
#             "START_LAST_WEEK_max_VALUE": "sum",
#             "START_NEXT_2_WEEK_max_NON-U GIFT": "sum",
#             "START_NEXT_2_WEEK_max_PERCENTAGE": "sum",
#             "START_NEXT_2_WEEK_max_U GIFT": "sum",
#             "START_NEXT_2_WEEK_max_VALUE": "sum",
#             "START_NEXT_WEEK_max_NON-U GIFT": "sum",
#             "START_NEXT_WEEK_max_PERCENTAGE": "sum",
#             "START_NEXT_WEEK_max_U GIFT": "sum",
#             "START_NEXT_WEEK_max_VALUE": "sum",
#             # "VAL_DISCOUNT_median_VALUE": ["min", "median", "max"],
#             "VAL_DISCOUNT_median_VALUE": "median",
#         }
#     )
#     .reset_index()
# )

# COMMAND ----------

# # Encoding categorical features

# for mechanic in ["NON-U GIFT", "U GIFT", "PERCENTAGE", "VALUE"]:
#     for deal_type in df_promotion["DEAL_TYPE"].unique():
#         # t[f"DEAL_TYPE_{deal_type}_MECHANIC_{mechanic}"] = t[f"DEAL_TYPE_sum_{mechanic}"].str.count(deal_type)
#         t[f"DEAL_TYPE_{deal_type}_MECHANIC_{mechanic}"] = t[f"DEAL_TYPE_sum_{mechanic}"].apply(lambda x: x.tolist().count(deal_type))
#         t[f"DEAL_TYPE_{deal_type}_MECHANIC_{mechanic}"] = t[f"DEAL_TYPE_{deal_type}_MECHANIC_{mechanic}"].fillna(0)

# for mechanic in ["NON-U GIFT", "U GIFT"]:
#     for uom in ["SU", "CS"]:
#         t[f"GIFT_UOM_{uom}_{mechanic}"] = t[f"GIFT_UOM_sum_{mechanic}"].apply(lambda x: x.tolist().count(uom))
#         t[f"GIFT_UOM_{uom}_{mechanic}"] = t[f"GIFT_UOM_{uom}_{mechanic}"].fillna(0)

# for mechanic in ["NON-U GIFT", "U GIFT", "PERCENTAGE", "VALUE"]:
#     for uom in ["VND", "SU", "CS"]:
#         t[f"MIN_BUY_UOM_{uom}_{mechanic}"] = t[f"MIN_BUY_UOM_sum_{mechanic}"].apply(lambda x: x.tolist().count(uom))
#         t[f"MIN_BUY_UOM_{uom}_{mechanic}"] = t[f"MIN_BUY_UOM_{uom}_{mechanic}"].fillna(0)

# t = t.drop(
#     [
#         "DEAL_TYPE_sum_NON-U GIFT",
#         "DEAL_TYPE_sum_PERCENTAGE",
#         "DEAL_TYPE_sum_U GIFT",
#         "DEAL_TYPE_sum_VALUE",
#         "GIFT_UOM_sum_NON-U GIFT",
#         "GIFT_UOM_sum_PERCENTAGE",
#         "GIFT_UOM_sum_U GIFT",
#         "GIFT_UOM_sum_VALUE",
#         "MIN_BUY_UOM_sum_NON-U GIFT",
#         "MIN_BUY_UOM_sum_PERCENTAGE",
#         "MIN_BUY_UOM_sum_U GIFT",
#         "MIN_BUY_UOM_sum_VALUE",
#     ],
#     axis=1,
# )

# COMMAND ----------

# ## agg DP|YEARWEEK

# df_tpr_dp = (
#     t.groupby(["CATEGORY", "DPNAME", "YEARWEEK"])
#     .agg(
#         {
#             "BENEFIT_TPR_max_NON-U GIFT": "max",
#             "BENEFIT_TPR_max_PERCENTAGE": "max",
#             "BENEFIT_TPR_max_U GIFT": "max",
#             "BENEFIT_TPR_max_VALUE": "max",
#             "BENEFIT_TPR_mean_NON-U GIFT": "mean",
#             "BENEFIT_TPR_mean_PERCENTAGE": "mean",
#             "BENEFIT_TPR_mean_U GIFT": "mean",
#             "BENEFIT_TPR_mean_VALUE": "mean",
#             "BENEFIT_TPR_median_NON-U GIFT": "median",
#             "BENEFIT_TPR_median_PERCENTAGE": "median",
#             "BENEFIT_TPR_median_U GIFT": "median",
#             "BENEFIT_TPR_median_VALUE": "median",
#             "BENEFIT_TPR_min_NON-U GIFT": "min",
#             "BENEFIT_TPR_min_PERCENTAGE": "min",
#             "BENEFIT_TPR_min_U GIFT": "min",
#             "BENEFIT_TPR_min_VALUE": "min",
#             "BENEFIT_TPR_sum_NON-U GIFT": "sum",
#             "BENEFIT_TPR_sum_PERCENTAGE": "sum",
#             "BENEFIT_TPR_sum_U GIFT": "sum",
#             "BENEFIT_TPR_sum_VALUE": "sum",
            
#             "BENEFIT_TPR_PER_MIN_BUY_max_NON-U GIFT": "max",
#             "BENEFIT_TPR_PER_MIN_BUY_max_PERCENTAGE": "max",
#             "BENEFIT_TPR_PER_MIN_BUY_max_U GIFT": "max",
#             "BENEFIT_TPR_PER_MIN_BUY_max_VALUE": "max",
#             "BENEFIT_TPR_PER_MIN_BUY_mean_NON-U GIFT": "mean",
#             "BENEFIT_TPR_PER_MIN_BUY_mean_PERCENTAGE": "mean",
#             "BENEFIT_TPR_PER_MIN_BUY_mean_U GIFT": "mean",
#             "BENEFIT_TPR_PER_MIN_BUY_mean_VALUE": "mean",
#             "BENEFIT_TPR_PER_MIN_BUY_median_NON-U GIFT": "median",
#             "BENEFIT_TPR_PER_MIN_BUY_median_PERCENTAGE": "median",
#             "BENEFIT_TPR_PER_MIN_BUY_median_U GIFT": "median",
#             "BENEFIT_TPR_PER_MIN_BUY_median_VALUE": "median",
#             "BENEFIT_TPR_PER_MIN_BUY_min_NON-U GIFT": "min",
#             "BENEFIT_TPR_PER_MIN_BUY_min_PERCENTAGE": "min",
#             "BENEFIT_TPR_PER_MIN_BUY_min_U GIFT": "min",
#             "BENEFIT_TPR_PER_MIN_BUY_min_VALUE": "min",
            
#             "DEAL_TYPE_count_NON-U GIFT": "sum",
#             "DEAL_TYPE_count_PERCENTAGE": "sum",
#             "DEAL_TYPE_count_U GIFT": "sum",
#             "DEAL_TYPE_count_VALUE": "sum",
            
#             "DURATION_TPR_1_WEEK_sum_NON-U GIFT": "sum",
#             "DURATION_TPR_1_WEEK_sum_PERCENTAGE": "sum",
#             "DURATION_TPR_1_WEEK_sum_U GIFT": "sum",
#             "DURATION_TPR_1_WEEK_sum_VALUE": "sum",
#             "DURATION_TPR_2_WEEK_sum_NON-U GIFT": "sum",
#             "DURATION_TPR_2_WEEK_sum_PERCENTAGE": "sum",
#             "DURATION_TPR_2_WEEK_sum_U GIFT": "sum",
#             "DURATION_TPR_2_WEEK_sum_VALUE": "sum",
            
#             "DURATION_TPR_DAILY_max_NON-U GIFT": "max",
#             "DURATION_TPR_DAILY_max_PERCENTAGE": "max",
#             "DURATION_TPR_DAILY_max_U GIFT": "max",
#             "DURATION_TPR_DAILY_max_VALUE": "max",
#             "DURATION_TPR_DAILY_mean_NON-U GIFT": "mean",
#             "DURATION_TPR_DAILY_mean_PERCENTAGE": "mean",
#             "DURATION_TPR_DAILY_mean_U GIFT": "mean",
#             "DURATION_TPR_DAILY_mean_VALUE": "mean",
#             "DURATION_TPR_DAILY_median_NON-U GIFT": "median",
#             "DURATION_TPR_DAILY_median_PERCENTAGE": "median",
#             "DURATION_TPR_DAILY_median_U GIFT": "median",
#             "DURATION_TPR_DAILY_median_VALUE": "median",
#             "DURATION_TPR_DAILY_min_NON-U GIFT": "min",
#             "DURATION_TPR_DAILY_min_PERCENTAGE": "min",
#             "DURATION_TPR_DAILY_min_U GIFT": "min",
#             "DURATION_TPR_DAILY_min_VALUE": "min",
            
#             "DURATION_TPR_OVER_1_MONTH_sum_NON-U GIFT": "sum",
#             "DURATION_TPR_OVER_1_MONTH_sum_PERCENTAGE": "sum",
#             "DURATION_TPR_OVER_1_MONTH_sum_U GIFT": "sum",
#             "DURATION_TPR_OVER_1_MONTH_sum_VALUE": "sum",
#             "DURATION_TPR_UNDER_1_MONTH_sum_NON-U GIFT": "sum",
#             "DURATION_TPR_UNDER_1_MONTH_sum_PERCENTAGE": "sum",
#             "DURATION_TPR_UNDER_1_MONTH_sum_U GIFT": "sum",
#             "DURATION_TPR_UNDER_1_MONTH_sum_VALUE": "sum",
            
#             "END_CURRENT_WEEK_sum_NON-U GIFT": "sum",
#             "END_CURRENT_WEEK_sum_PERCENTAGE": "sum",
#             "END_CURRENT_WEEK_sum_U GIFT": "sum",
#             "END_CURRENT_WEEK_sum_VALUE": "sum",
            
#             "END_FIRST_WEEK_OF_MONTH_sum_NON-U GIFT": "sum",
#             "END_FIRST_WEEK_OF_MONTH_sum_PERCENTAGE": "sum",
#             "END_FIRST_WEEK_OF_MONTH_sum_U GIFT": "sum",
#             "END_FIRST_WEEK_OF_MONTH_sum_VALUE": "sum",
#             "END_FIRST_WEEK_OF_QUARTER_sum_NON-U GIFT": "sum",
#             "END_FIRST_WEEK_OF_QUARTER_sum_PERCENTAGE": "sum",
#             "END_FIRST_WEEK_OF_QUARTER_sum_U GIFT": "sum",
#             "END_FIRST_WEEK_OF_QUARTER_sum_VALUE": "sum",
            
#             "END_LAST_WEEK_OF_MONTH_sum_NON-U GIFT": "sum",
#             "END_LAST_WEEK_OF_MONTH_sum_PERCENTAGE": "sum",
#             "END_LAST_WEEK_OF_MONTH_sum_U GIFT": "sum",
#             "END_LAST_WEEK_OF_MONTH_sum_VALUE": "sum",
#             "END_LAST_WEEK_OF_QUARTER_sum_NON-U GIFT": "sum",
#             "END_LAST_WEEK_OF_QUARTER_sum_PERCENTAGE": "sum",
#             "END_LAST_WEEK_OF_QUARTER_sum_U GIFT": "sum",
#             "END_LAST_WEEK_OF_QUARTER_sum_VALUE": "sum",
            
#             "END_LAST_2_WEEK_max_NON-U GIFT": "sum",
#             "END_LAST_2_WEEK_max_PERCENTAGE": "sum",
#             "END_LAST_2_WEEK_max_U GIFT": "sum",
#             "END_LAST_2_WEEK_max_VALUE": "sum",
#             "END_LAST_WEEK_max_NON-U GIFT": "sum",
#             "END_LAST_WEEK_max_PERCENTAGE": "sum",
#             "END_LAST_WEEK_max_U GIFT": "sum",
#             "END_LAST_WEEK_max_VALUE": "sum",
            
#             "END_NEXT_2_WEEK_max_NON-U GIFT": "sum",
#             "END_NEXT_2_WEEK_max_PERCENTAGE": "sum",
#             "END_NEXT_2_WEEK_max_U GIFT": "sum",
#             "END_NEXT_2_WEEK_max_VALUE": "sum",
#             "END_NEXT_WEEK_max_NON-U GIFT": "sum",
#             "END_NEXT_WEEK_max_PERCENTAGE": "sum",
#             "END_NEXT_WEEK_max_U GIFT": "sum",
#             "END_NEXT_WEEK_max_VALUE": "sum",
            
#             "GIFT_QTY_max_NON-U GIFT": "max",
#             "GIFT_QTY_max_U GIFT": "max",
#             "GIFT_QTY_min_NON-U GIFT": "min",
#             "GIFT_QTY_min_U GIFT": "min",
            
#             "MIN_BUY_max_NON-U GIFT": "max",
#             "MIN_BUY_max_PERCENTAGE": "max",
#             "MIN_BUY_max_U GIFT": "max",
#             "MIN_BUY_max_VALUE": "max",
#             "MIN_BUY_median_NON-U GIFT": "median",
#             "MIN_BUY_median_PERCENTAGE": "median",
#             "MIN_BUY_median_U GIFT": "median",
#             "MIN_BUY_median_VALUE": "median",
#             "MIN_BUY_mean_NON-U GIFT" : "mean", 
#             "MIN_BUY_mean_PERCENTAGE" : "mean",
#             "MIN_BUY_mean_U GIFT" : "mean", 
#             "MIN_BUY_mean_VALUE" : "mean",
#             "MIN_BUY_min_NON-U GIFT": "min",
#             "MIN_BUY_min_PERCENTAGE": "min",
#             "MIN_BUY_min_U GIFT": "min",
#             "MIN_BUY_min_VALUE": "min",
            
#             "PCT_DISCOUNT_max_PERCENTAGE": "max",
#             "PCT_DISCOUNT_mean_PERCENTAGE": "mean",
#             "PCT_DISCOUNT_median_PERCENTAGE": "median",
#             "PCT_DISCOUNT_min_PERCENTAGE": "min",
            
#             "START_CURRENT_WEEK_sum_NON-U GIFT": "sum",
#             "START_CURRENT_WEEK_sum_PERCENTAGE": "sum",
#             "START_CURRENT_WEEK_sum_U GIFT": "sum",
#             "START_CURRENT_WEEK_sum_VALUE": "sum",
#             "START_END_SAME_WEEK_sum_NON-U GIFT": "sum",
#             "START_END_SAME_WEEK_sum_PERCENTAGE": "sum",
#             "START_END_SAME_WEEK_sum_U GIFT": "sum",
#             "START_END_SAME_WEEK_sum_VALUE": "sum",
            
#             "START_FIRST_WEEK_OF_MONTH_sum_NON-U GIFT": "sum",
#             "START_FIRST_WEEK_OF_MONTH_sum_PERCENTAGE": "sum",
#             "START_FIRST_WEEK_OF_MONTH_sum_U GIFT": "sum",
#             "START_FIRST_WEEK_OF_MONTH_sum_VALUE": "sum",
#             "START_FIRST_WEEK_OF_QUARTER_sum_NON-U GIFT": "sum",
#             "START_FIRST_WEEK_OF_QUARTER_sum_PERCENTAGE": "sum",
#             "START_FIRST_WEEK_OF_QUARTER_sum_U GIFT": "sum",
#             "START_FIRST_WEEK_OF_QUARTER_sum_VALUE": "sum",
            
#             "START_LAST_WEEK_OF_MONTH_sum_NON-U GIFT": "sum",
#             "START_LAST_WEEK_OF_MONTH_sum_PERCENTAGE": "sum",
#             "START_LAST_WEEK_OF_MONTH_sum_U GIFT": "sum",
#             "START_LAST_WEEK_OF_MONTH_sum_VALUE": "sum",
#             "START_LAST_WEEK_OF_QUARTER_sum_NON-U GIFT": "sum",
#             "START_LAST_WEEK_OF_QUARTER_sum_PERCENTAGE": "sum",
#             "START_LAST_WEEK_OF_QUARTER_sum_U GIFT": "sum",
#             "START_LAST_WEEK_OF_QUARTER_sum_VALUE": "sum",
            
#             "START_LAST_2_WEEK_max_NON-U GIFT": "sum",
#             "START_LAST_2_WEEK_max_PERCENTAGE": "sum",
#             "START_LAST_2_WEEK_max_U GIFT": "sum",
#             "START_LAST_2_WEEK_max_VALUE": "sum",
#             "START_LAST_WEEK_max_NON-U GIFT": "sum",
#             "START_LAST_WEEK_max_PERCENTAGE": "sum",
#             "START_LAST_WEEK_max_U GIFT": "sum",
#             "START_LAST_WEEK_max_VALUE": "sum",
            
#             "START_NEXT_2_WEEK_max_NON-U GIFT": "sum",
#             "START_NEXT_2_WEEK_max_PERCENTAGE": "sum",
#             "START_NEXT_2_WEEK_max_U GIFT": "sum",
#             "START_NEXT_2_WEEK_max_VALUE": "sum",
#             "START_NEXT_WEEK_max_NON-U GIFT": "sum",
#             "START_NEXT_WEEK_max_PERCENTAGE": "sum",
#             "START_NEXT_WEEK_max_U GIFT": "sum",
#             "START_NEXT_WEEK_max_VALUE": "sum",
            
#             "VAL_DISCOUNT_median_VALUE": "median",
#             "DEAL_TYPE_LINDIS_MECHANIC_NON-U GIFT": "sum",
#             "DEAL_TYPE_BASKET_MECHANIC_NON-U GIFT": "sum",
#             "DEAL_TYPE_BBFREE_MECHANIC_NON-U GIFT": "sum",
#             "DEAL_TYPE_SETDIS_MECHANIC_NON-U GIFT": "sum",
#             "DEAL_TYPE_CPNBAS_MECHANIC_NON-U GIFT": "sum",
#             "DEAL_TYPE_LINDIS_MECHANIC_U GIFT": "sum",
#             "DEAL_TYPE_BASKET_MECHANIC_U GIFT": "sum",
#             "DEAL_TYPE_BBFREE_MECHANIC_U GIFT": "sum",
#             "DEAL_TYPE_SETDIS_MECHANIC_U GIFT": "sum",
#             "DEAL_TYPE_CPNBAS_MECHANIC_U GIFT": "sum",
#             "DEAL_TYPE_LINDIS_MECHANIC_PERCENTAGE": "sum",
#             "DEAL_TYPE_BASKET_MECHANIC_PERCENTAGE": "sum",
#             "DEAL_TYPE_BBFREE_MECHANIC_PERCENTAGE": "sum",
#             "DEAL_TYPE_SETDIS_MECHANIC_PERCENTAGE": "sum",
#             "DEAL_TYPE_CPNBAS_MECHANIC_PERCENTAGE": "sum",
#             "DEAL_TYPE_LINDIS_MECHANIC_VALUE": "sum",
#             "DEAL_TYPE_BASKET_MECHANIC_VALUE": "sum",
#             "DEAL_TYPE_BBFREE_MECHANIC_VALUE": "sum",
#             "DEAL_TYPE_SETDIS_MECHANIC_VALUE": "sum",
#             "DEAL_TYPE_CPNBAS_MECHANIC_VALUE": "sum",
            
#             "GIFT_UOM_SU_NON-U GIFT": "sum",
#             "GIFT_UOM_CS_NON-U GIFT": "sum",
#             "GIFT_UOM_SU_U GIFT": "sum",
#             "GIFT_UOM_CS_U GIFT": "sum",
            
#             "MIN_BUY_UOM_VND_NON-U GIFT": "sum",
#             "MIN_BUY_UOM_SU_NON-U GIFT": "sum",
#             "MIN_BUY_UOM_CS_NON-U GIFT": "sum",
#             "MIN_BUY_UOM_VND_U GIFT": "sum",
#             "MIN_BUY_UOM_SU_U GIFT": "sum",
#             "MIN_BUY_UOM_CS_U GIFT": "sum",
#             "MIN_BUY_UOM_VND_PERCENTAGE": "sum",
#             "MIN_BUY_UOM_SU_PERCENTAGE": "sum",
#             "MIN_BUY_UOM_CS_PERCENTAGE": "sum",
#             "MIN_BUY_UOM_VND_VALUE": "sum",
#             "MIN_BUY_UOM_SU_VALUE": "sum",
#             "MIN_BUY_UOM_CS_VALUE": "sum",
#         }
#     )
#     .reset_index()
# )

# check_col_zero = df_tpr_dp.max() == 0
# check_col_zero = check_col_zero[check_col_zero == True].index
# print(check_col_zero)

# df_tpr_dp = df_tpr_dp.drop(check_col_zero, axis = 1)
# df_tpr_dp.shape

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Main product TPR data

# COMMAND ----------

# df_product_tpr = df_pri_sales_by_code.copy()
# df_product_tpr = df_product_tpr.rename(columns={"ACTUALSALE": "PRI_SALES"})

# df_product_tpr = df_product_tpr.merge(
#     df_sec_sales_by_code[["CATEGORY", "DPNAME", "YEARWEEK", "MATERIAL", "ACTUALSALE"]],
#     on=["CATEGORY", "DPNAME", "MATERIAL", "YEARWEEK"],
#     how="outer",
# )

# df_product_tpr["BANNER"] = "NATIONWIDE"
# df_product_tpr["REGION"] = "NATIONWIDE"

# df_product_tpr = df_product_tpr.rename(columns={"ACTUALSALE": "SEC_SALES"})

# df_product_tpr = df_product_tpr[
#     & (df_product_tpr["YEARWEEK"] <= 202352)
#     # & (df_promotion["MATERIAL"] >= 6e7)
# ]

# df_product_tpr = df_product_tpr.groupby(["BANNER","REGION","CATEGORY","DPNAME","YEARWEEK"])[["PRI_SALES","SEC_SALES"]].sum().reset_index()

# print(df_product_tpr.shape)
# df_product_tpr.head(2)

# COMMAND ----------

df_product_tpr = df_pri_sales[
    ["BANNER", "REGION", "CATEGORY", "DPNAME", "YEARWEEK", "ACTUALSALE"]
].copy()
df_product_tpr = df_product_tpr.rename(columns={"ACTUALSALE": "PRI_SALES"})

df_product_tpr = df_product_tpr.merge(
    df_sec_sales[
        ["BANNER", "CATEGORY", "DPNAME", "YEARWEEK", "ACTUALSALE", "BASELINE"]
    ],
    on=["BANNER", "CATEGORY", "DPNAME", "YEARWEEK"],
    how="outer",
)

df_product_tpr = df_product_tpr.rename(
    columns={"ACTUALSALE": "SEC_SALES", "BASELINE": "SEC_BASELINE_ORG"}
)

df_product_tpr = df_product_tpr[
    (
        df_product_tpr["YEARWEEK"].between(
            df_promotion["YEARWEEK"].min(), df_promotion["YEARWEEK"].max()
        )
    )
]

df_product_tpr["DATE"] = pd.to_datetime(
    df_product_tpr["YEARWEEK"].astype(str) + "-1", format="%G%V-%w"
)

df_product_tpr = df_product_tpr.merge(df_calendar_workingday, on="YEARWEEK")

df_product_tpr = df_product_tpr.merge(df_week_master, on="YEARWEEK")

df_product_tpr["QUARTER"] = ((df_product_tpr["MONTH"] - 1) / 3).astype(int) + 1

df_product_tpr["WEEK_MONTH_COUNT"] = (
    df_product_tpr.groupby(["DPNAME", "YEAR", "MONTH"])["YEARWEEK"]
    .transform("count")
    .astype(int)
)
df_product_tpr["WEEK_OF_MONTH"] = (df_product_tpr["DATE"].dt.day - 1) // 7 + 1
df_product_tpr["WEEK_OF_QUARTER"] = (df_product_tpr["YEARWEEK"] % 100 - 1) % 13 + 1
df_product_tpr["WEEK_OF_YEAR"] = df_product_tpr["YEARWEEK"] % 100
df_product_tpr["MONTH_OF_QUARTER"] = (df_product_tpr["MONTH"] - 1) % 4 + 1

print(df_product_tpr.shape)
df_product_tpr.head(2)

# COMMAND ----------

df_convert_temp = df_convert[
    [
        "CATEGORY",
        "DPNAME",
        "YEARWEEK",
        "FC_PRI_BASELINE_WEEKLY",
        "SEC_BASELINE",
    ]
]

df_convert_temp = df_convert_temp.rename(
    columns={
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

df_business_promotion = df_business_promotion.groupby(["CATEGORY","DPNAME","YEARWEEK"])["ABNORMAL"].apply(lambda x: max(map(str, x)) if (x.all() in["N", "Y"]) else None).reset_index()
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

# df_pri_sales_exclude_banded = df_pri_sales_exclude_banded.rename(
#     columns={"PRI_SALES_WITHOUT_BANDED": "PRI_SALES_OLD_TRANSFORM"}
# )
# df_product_tpr = df_product_tpr.merge(
#     df_pri_sales_exclude_banded[
#         [
#             "CATEGORY",
#             "DPNAME",
#             "YEARWEEK",
#             "PRI_SALES_OLD_TRANSFORM",
#         ]
#     ],
#     on=["CATEGORY", "DPNAME", "YEARWEEK"],
#     how="left",
# )

# COMMAND ----------

df_product_tpr = df_product_tpr.merge(
    df_uplift_banded[
        [
            "CATEGORY",
            "DPNAME",
            "YEARWEEK",
            "UPLIFT_BANDED",
            "BANDED_COUNT",
        ]
    ],
    on=["CATEGORY", "DPNAME", "YEARWEEK"],
    how="left",
)
df_product_tpr["UPLIFT_BANDED"] = df_product_tpr["UPLIFT_BANDED"].fillna(0)
df_product_tpr["PRI_SALES_WITHOUT_BANDED"] = df_product_tpr["PRI_SALES"] - df_product_tpr["UPLIFT_BANDED"]

df_product_tpr = df_product_tpr.sort_values(["CATEGORY","DPNAME","YEARWEEK"]).reset_index(drop = True).fillna(0)
df_product_tpr.shape

# COMMAND ----------

df_product_tpr = df_product_tpr.merge(
    df_tpr_dp, 
    on = ["CATEGORY","DPNAME","YEARWEEK"],
    how = "left"
)

# COMMAND ----------

df_product_tpr["PROMOTION_COUNT"] = df_product_tpr[
    [
        "DEAL_TYPE_count_NON-U GIFT",
        "DEAL_TYPE_count_PERCENTAGE",
        "DEAL_TYPE_count_U GIFT",
        "DEAL_TYPE_count_VALUE",
    ]
].sum(axis = 1)

# COMMAND ----------

print(
    df_product_tpr[df_product_tpr["DPNAME"].str.contains("DELISTED|DELETED")].shape[0]
    / df_product_tpr.shape[0]
)
df_product_tpr = df_product_tpr[
    (~df_product_tpr["DPNAME"].str.contains("DELISTED|DELETED"))
    # & (df_product_tpr["YEARWEEK"] >= 202001)
]

df_product_tpr = df_product_tpr[df_product_tpr["PRI_SALES_WITHOUT_BANDED"] >= 0].sort_values(["CATEGORY","DPNAME","YEARWEEK"]).reset_index(drop = True)

# COMMAND ----------

df_product_tpr.describe()

# COMMAND ----------


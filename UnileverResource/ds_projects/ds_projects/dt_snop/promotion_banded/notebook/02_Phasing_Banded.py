# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Load data

# COMMAND ----------

# MAGIC %run "/Repos/lai-trung-minh.duc@unilever.com/SC_DT_Forecast_Project/EnvironmentSetup"

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
# MAGIC
# MAGIC ## Promotion data

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

df_mapping_banded = pd.read_excel("/dbfs/mnt/adls/NMHDAT_SNOP/DT/Promotion/SALE_BOM_NOV_15.xlsx")
print(df_mapping_banded.shape)
df_mapping_banded.head(2)

# COMMAND ----------

df_mapping_banded = df_mapping_banded[df_mapping_banded["Banded Material"].isin(df_banded["MATERIAL"].unique())].drop_duplicates()
df_mapping_banded = df_mapping_banded[df_mapping_banded["Plant"].isin(["V101","V102","V103","V104","V105","V106"])]

# COMMAND ----------

df_mapping_banded = df_mapping_banded.drop_duplicates(subset = ["Banded Material","Item Material"])

df_mapping_banded = df_mapping_banded[
    (
        df_mapping_banded["Banded Material"]
        .astype(int)
        .isin(df_pri_sales_by_code["MATERIAL"].astype(int).unique())
    )
    & (
        df_mapping_banded["Item Material"].isin(
            df_pri_sales_by_code["MATERIAL"].astype(int).unique()
        )
    )
]

# COMMAND ----------

df_mapping_banded.groupby("Banded Material").agg({"Item Material":"nunique",}).sort_values("Item Material",ascending = False).reset_index()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Source Primary sales HANA

# COMMAND ----------

from pyspark.sql import functions as F

NUMBER_OF_PARTITIONS = sc.defaultParallelism * 2

DF_HANA_PRI_SALES = spark.read.csv(
    "dbfs:/mnt/adls/Prod_UDL/TechDebt/InternalSources/U2K2BW/OpenHubFileDestination/PrimarySales/Vietnam/Processed",
    sep="|",
    header=True,
)
DF_HANA_CUSTOMER_MASTER = spark.read.format("delta").load(
    "dbfs:/mnt/adls/Prod_UDL/TechDebt/InternalSources/U2K2BW/OpenHubFileDestination/CustomerMaster/SouthEastAsiaAustralasia/Processed_Parquet"
)

# COMMAND ----------

DF_HANA_CUSTOMER_MASTER = (
    DF_HANA_CUSTOMER_MASTER.select("CUST_SALES", "COUNTRY", "ZCUSTNAME4", "ZCUSTNAME16")
    .filter(DF_HANA_CUSTOMER_MASTER.COUNTRY == "VN")
    .dropDuplicates()
)

# COMMAND ----------

DF_HANA_PRI_SALES = DF_HANA_PRI_SALES.join(DF_HANA_CUSTOMER_MASTER).where(
    (DF_HANA_PRI_SALES.CUSTOMER == DF_HANA_CUSTOMER_MASTER.CUST_SALES)
)

# COMMAND ----------

DF_HANA_PRI_SALES = DF_HANA_PRI_SALES.filter(F.col('MATERIAL').cast("Integer").isin(df_mapping_banded["Banded Material"].astype(int).unique().tolist()))

# COMMAND ----------

DF_HANA_PRI_SALES = DF_HANA_PRI_SALES.withColumn("G_QABSMG", F.when(F.col("G_QABSMG") > 0, F.col("G_QABSMG")).otherwise(0))

# COMMAND ----------

# DF_HANA_PRI_SALES.select("PLANT").where(DF_HANA_PRI_SALES.ZCUSTNAME4.isin(["DT HCME", "DT MEKONG DELTA", "DT North", "DT CENTRAL"])).distinct().collect()

# COMMAND ----------

# df_before_groupby["MATERIAL"] = df_before_groupby["MATERIAL"].astype(int)
# df_before_groupby["CALWEEK"] = df_before_groupby["CALWEEK"].astype(int)

# df_before_groupby["G_QABSMG"][df_before_groupby["G_QABSMG"].str[-1] == "-"] = "-" + df_before_groupby["G_QABSMG"].str[:-1]

# df_before_groupby["G_AVV010"][df_before_groupby["G_AVV010"].str[-1] == "-"] = "-" + df_before_groupby["G_AVV010"].str[:-1]

# COMMAND ----------

DF_HANA_PRI_SALES = DF_HANA_PRI_SALES.groupBy(
    "ZCUSTNAME4", "ZCUSTNAME16", "CALDAY", "CALWEEK", "MATERIAL"
).agg(
    F.sum("G_QABSMG").alias("PCS"),
    F.sum("G_AVV010").alias("GSV"),
)

# COMMAND ----------

rename_header = {
    "CALDAY": "DATE",
    "CALWEEK": "YEARWEEK",
    "ZCUSTNAME4": "BANNER",
    "ZCUSTNAME16": "REGION",
    "MATERIAL": "MATERIAL",
    "PCS": "PCS",
    "GSV": "GSV",
}
for key, value in rename_header.items():
    DF_HANA_PRI_SALES = DF_HANA_PRI_SALES.withColumnRenamed(key, value)

# COMMAND ----------

DF_HANA_PRI_SALES_WEEKLY = DF_HANA_PRI_SALES.groupBy(
    "YEARWEEK", "BANNER", "REGION", "MATERIAL"
).agg(F.sum("PCS").alias("PCS"), F.sum("GSV").alias("GSV"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Mapping Banded Sales

# COMMAND ----------

df_pri_hana_banded = DF_HANA_PRI_SALES_WEEKLY.toPandas()

# COMMAND ----------

display(df_pri_sales_by_code[df_pri_sales_by_code["MATERIAL"] == 68418575])

# COMMAND ----------

df_pri_hana_banded = df_pri_hana_banded.dropna()
df_pri_hana_banded["MATERIAL"] = df_pri_hana_banded["MATERIAL"].astype(int)

df_pri_hana_banded = df_pri_hana_banded[
    df_pri_hana_banded["BANNER"].isin(
        ["DT HCME", "DT MEKONG DELTA", "DT North", "DT CENTRAL"]
    )
]
df_pri_hana_banded = df_pri_hana_banded.groupby(["YEARWEEK", "MATERIAL"])["PCS"].sum().reset_index()
df_pri_hana_banded["REGION"] = "NATIONWIDE"
df_pri_hana_banded["BANNER"] = "NATIONWIDE"

# COMMAND ----------

df_pri_hana_banded = df_pri_hana_banded.merge(df_master_product, on="MATERIAL")

df_pri_hana_banded["TON"] = df_pri_hana_banded["PCS"] * df_pri_hana_banded["KG/PCS"] / 1000
df_pri_hana_banded["CS"] = df_pri_hana_banded["PCS"] / df_pri_hana_banded["PCS/CS"]

# COMMAND ----------

df_pri_hana_banded.head(3)

# COMMAND ----------

df_pri_hana_banded = (
    df_pri_hana_banded.groupby(["YEARWEEK", "BANNER", "REGION", "CATEGORY", "DPNAME","MATERIAL"])[
        ["PCS", "TON", "CS"]
    ]
    .sum()
    .reset_index()
)

df_pri_hana_banded["ACTUALSALE"] = df_pri_hana_banded["TON"]
df_pri_hana_banded["ACTUALSALE"].loc[
    (df_pri_hana_banded["CATEGORY"].isin(["SKINCARE", "IC", "DEO"]))
] = df_pri_hana_banded["CS"]
df_pri_hana_banded["ACTUALSALE"].loc[(df_pri_hana_banded["CATEGORY"].isin(["TBRUSH"]))] = (
    df_pri_hana_banded["PCS"] / 1000
)

df_pri_hana_banded["YEARWEEK"] = df_pri_hana_banded["YEARWEEK"].astype(int)

df_pri_hana_banded["DATE"] = pd.to_datetime(df_pri_hana_banded["YEARWEEK"].astype(str) + "-1", format="%G%V-%w")

df_pri_hana_banded = df_pri_hana_banded.fillna(0)
df_pri_hana_banded = df_pri_hana_banded.sort_values(["DPNAME","MATERIAL","YEARWEEK"]).reset_index(drop = True)

print(df_pri_hana_banded.shape)
df_pri_hana_banded.head(3)

# COMMAND ----------

df_pri_hana_banded = df_pri_hana_banded.rename(columns = {"ACTUALSALE":"BANDED_SALES"})

# COMMAND ----------

df_pri_sales_by_code = df_pri_sales_by_code.merge(
    df_pri_hana_banded.drop(["PCS", "TON", "CS", "DATE", "BANNER", "REGION"], axis=1),
    on=["CATEGORY", "DPNAME", "MATERIAL", "YEARWEEK"],
    how="left",
)

df_pri_sales_by_code["ACTUALSALE_TRANSFORM"] = df_pri_sales_by_code["ACTUALSALE"]

df_pri_sales_by_code.loc[
    df_pri_sales_by_code["BANDED_SALES"].notnull(), "ACTUALSALE_TRANSFORM"
] = df_pri_sales_by_code.loc[
    df_pri_sales_by_code["BANDED_SALES"].notnull(), "BANDED_SALES"
]

# COMMAND ----------

display(df_pri_sales_by_code[df_pri_sales_by_code["DPNAME"] == "OMO LIQUID MATIC CFT SS (POU) 3.7KG"].sort_values(["YEARWEEK","MATERIAL"]))

# COMMAND ----------

df_pri_sales_by_code = df_pri_sales_by_code.merge(
    df_mapping_banded[["Banded Material", "Item Material"]],
    left_on="MATERIAL",
    right_on="Banded Material",
    how="left",
)

df_pri_sales_by_code["MATERIAL_MAPPING"] = df_pri_sales_by_code.groupby(
    ["BANNER", "CATEGORY", "DPNAME", "YEARWEEK"]
)["Item Material"].transform("max")
df_pri_sales_by_code["BANDED_DP"] = (
    df_pri_sales_by_code.groupby(["BANNER", "CATEGORY", "DPNAME", "YEARWEEK"])[
        "BANDED_SALES"
    ]
    .transform("sum")
    .astype(float)
)

df_pri_sales_by_code["ACTUALSALE_TRANSFORM"] = np.where(
    (df_pri_sales_by_code["MATERIAL"] == df_pri_sales_by_code["MATERIAL_MAPPING"])
    & (df_pri_sales_by_code["BANDED_DP"] > 0),
    df_pri_sales_by_code["ACTUALSALE_TRANSFORM"] - df_pri_sales_by_code["BANDED_DP"],
    df_pri_sales_by_code["ACTUALSALE_TRANSFORM"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Merge

# COMMAND ----------

df_promotion = df_pri_sales_by_code.copy()
df_promotion = df_promotion.drop(["ACTUALSALE","BANDED_SALES","Banded Material","Item Material","MATERIAL_MAPPING","BANDED_DP"], axis =1)
print(df_promotion.shape)

df_promotion = df_promotion.rename(columns={"ACTUALSALE_TRANSFORM": "PRI_SALES"})

df_promotion = df_promotion.merge(
    df_sec_sales_by_code[["CATEGORY", "DPNAME", "YEARWEEK", "MATERIAL", "ACTUALSALE"]],
    on=["CATEGORY", "DPNAME", "MATERIAL", "YEARWEEK"],
    how="outer",
)

df_promotion = df_promotion.rename(columns={"ACTUALSALE": "SEC_SALES"})

print(df_promotion.shape)
df_promotion.head(2)

# COMMAND ----------

# df_promotion = df_promotion.merge(df_master_product[["MATERIAL","SKU_TYPE"]], on = "MATERIAL", how = "left")
# print(df_promotion.shape)
# df_promotion["SKU_TYPE"].value_counts()

# COMMAND ----------

# df_promotion = df_promotion[df_promotion["SKU_TYPE"] == "Banded"]
df_promotion = df_promotion[df_promotion["MATERIAL"].isin(df_banded["MATERIAL"].unique())]

df_promotion = df_promotion.drop(["BANNER","REGION","PCS","TON", "CS"], axis = 1)
df_promotion["DATE"] = pd.to_datetime(df_promotion["YEARWEEK"].astype(str) + "-1", format="%G%V-%w")

df_promotion = df_promotion.merge(df_calendar_workingday, on="YEARWEEK")
df_promotion = df_promotion.merge(df_week_master, on = "YEARWEEK")
# 1/2021 -> end of 10/2023
df_promotion = df_promotion[(df_promotion["YEARWEEK"] >= 202101) & (df_promotion["YEARWEEK"] <= 202347)]

df_promotion.shape,  df_promotion["DPNAME"].nunique(), df_promotion["MATERIAL"].nunique(), df_banded["MATERIAL"].nunique()

# COMMAND ----------

df_promotion.describe()

# COMMAND ----------

df_promotion[df_promotion["PRI_SALES"] < 0].shape[0] / df_promotion.shape[0]

# COMMAND ----------

df_promotion["PRI_SALES"] = df_promotion["PRI_SALES"].fillna(0)
df_promotion["SEC_SALES"] = df_promotion["SEC_SALES"].fillna(0)

df_promotion = df_promotion[df_promotion["PRI_SALES"] >= 0]

df_promotion = df_promotion.drop_duplicates()

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
df_convert = df_convert.rename(columns={"ACTUALSALE": "PRI_SALES"})

df_convert = df_convert.sort_values(["KEY", "YEARWEEK"]).reset_index(drop=True)

df_convert["FC_PRI_BASELINE"] = 0
df_convert["FC_PRI_BASELINE_WEEKLY"] = 0
df_convert["SELLINGDAY_WEEK/MONTH RATIO_median"] = 0

df_convert = df_convert[df_convert["YEARWEEK"] <= df_promotion["YEARWEEK"].max()]
print(df_convert["YEARWEEK"].max())

# COMMAND ----------

dict_promotion_time = {}
list_year = sorted((df_promotion["YEARWEEK"] / 100).astype(int).unique())

for year_idx in list_year:
    dict_promotion_time[year_idx] = []
    for month_idx in df_convert[
        (df_convert["YEAR"] == year_idx)
        & (df_convert["YEARWEEK"] <= df_promotion["YEARWEEK"].max())
    ]["MONTH"].unique():
        dict_promotion_time[year_idx].append(month_idx)

    dict_promotion_time[year_idx] = sorted(dict_promotion_time[year_idx])
dict_promotion_time

# COMMAND ----------

for year_idx in dict_promotion_time.keys():
# for year_idx in [2023]:
    for month_idx in dict_promotion_time[year_idx]:
    # for month_idx in range(1,9):

        phasing_lower = df_convert["YEARWEEK"][
            (df_convert["YEAR"] >= (year_idx - 2)) & (df_convert["MONTH"] == month_idx)
        ].min()
        # phasing_lower = 202101

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
            input_var = "SEC_BASELINE",
            output_var = "FC_PRI_BASELINE_WEEKLY"
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

# MAGIC %md
# MAGIC
# MAGIC ## Evaluate Baseline only 1 - 8/2023

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

for month_idx in range(1, 11):
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

    df_accuracy_phase_2Y = df_accuracy_phase_2Y.rename(
        columns={
            "CATEGORY": "CATEGORY",
            "Sum_actualsale": "sum_actualsale_month_" + str(month_idx),
            "Sum_predictsale_FC_PRI_BASELINE_WEEKLY": "sum_predictsale_month_"
            + str(month_idx),
            "Accuracy_FC_PRI_BASELINE_WEEKLY": "accuracy_month_" + str(month_idx),
            "Error_FC_PRI_BASELINE_WEEKLY": "error_month_" + str(month_idx),
        }
    )

    df_accuracy_baseline_monthly2023 = df_accuracy_baseline_monthly2023.merge(
        df_accuracy_phase_2Y, on=["CATEGORY"], how="outer"
    )

# COMMAND ----------

df_accuracy_baseline_monthly2023 = df_accuracy_baseline_monthly2023.fillna(0)
df_accuracy_baseline_monthly2023 = df_accuracy_baseline_monthly2023.replace([-np.inf, np.inf], 0)

df_accuracy_baseline_monthly2023

# COMMAND ----------

df_accuracy_baseline_monthly2023[
    [
        "CATEGORY",
        "accuracy_month_1",
        "accuracy_month_2",
        "accuracy_month_3",
        "accuracy_month_4",
        "accuracy_month_5",
        "accuracy_month_6",
        "accuracy_month_7",
        "accuracy_month_8",
        "accuracy_month_9",
        "accuracy_month_10",
    ]
]

# COMMAND ----------

# df_accuracy_baseline_monthly2023.to_csv("/dbfs/mnt/adls/NMHDAT_SNOP/accuracy_baseline_monthly2023.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Rule base

# COMMAND ----------

df_convert_temp = df_convert[
    [
        "CATEGORY",
        "DPNAME",
        "YEARWEEK",
        "SEC_SALES",
        "PRI_SALES",
        "FC_PRI_BASELINE_WEEKLY",
    ]
]

df_convert_temp = df_convert_temp.rename(
    columns={
        "PRI_SALES": "PRI_SALES_DP",
        "SEC_SALES": "SEC_SALES_DP",
        # "PROPOSED_FC_WEEKLY": "SEC_BASELINE_WEEKLY",
        "FC_PRI_BASELINE_WEEKLY": "PRI_BASELINE_WEEKLY"
    }
)
print(df_convert_temp.shape)
df_convert_temp.head(2)

# COMMAND ----------

df_promotion = df_promotion.merge(
    df_convert_temp,
    on=["CATEGORY", "DPNAME", "YEARWEEK"],
    how="left",
)

# COMMAND ----------

promo_code_2023 = df_banded[df_banded["START_DATE_PROMOTION"].dt.year >= 2023][["START_DATE_PROMOTION","MATERIAL"]].drop_duplicates()
promo_code_2023 = promo_code_2023.merge(df_promotion[["CATEGORY","DPNAME","MATERIAL"]], on =["MATERIAL"] , how = "inner")

promo_code_2023 = promo_code_2023.drop_duplicates().sort_values(["DPNAME","START_DATE_PROMOTION","MATERIAL"]).reset_index(drop = True)
print(promo_code_2023["DPNAME"].nunique())
promo_code_2023

# COMMAND ----------

df_promotion = df_promotion[(df_promotion["YEARWEEK"] >= 202101) & (df_promotion["YEARWEEK"] < 202301)]

df_promotion["CONTRIBUTE_MATERIAL"] = (
    df_promotion["PRI_SALES"] / df_promotion["PRI_SALES_DP"]
)
# df_promotion["CONTRIBUTE_MATERIAL"] = df_promotion["CONTRIBUTE_MATERIAL"] / df_promotion.groupby(["CATEGORY","DPNAME","YEARWEEK"])["CONTRIBUTE_MATERIAL"].transform("sum")

df_promotion["CONTRIBUTE_MATERIAL"] = df_promotion["CONTRIBUTE_MATERIAL"].replace([-np.inf, np.inf], 0).fillna(0)

df_promotion = df_promotion.replace([-np.inf, np.inf], 0).fillna(0)
df_promotion = df_promotion.sort_values(
    ["CATEGORY", "DPNAME", "MATERIAL", "YEARWEEK"]
).reset_index(drop=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Exclude small pri_sales of SKU (1%) 

# COMMAND ----------

threshold = 2
df_new = pd.DataFrame()

for key, df_group in tqdm(df_promotion.groupby(["CATEGORY", "DPNAME", "MATERIAL"])):
    order_promo = 0
    df_group = df_group.sort_values("DATE").reset_index(drop=True)
    df_group["SUM_PRISALES_MATERIAL"] = df_group["PRI_SALES"].sum()
    df_group["CONTRIBUTE"] = df_group["PRI_SALES"] / df_group["SUM_PRISALES_MATERIAL"]
    df_group["DIFF"] = df_group[
        "DATE"
    ].diff(1) / np.timedelta64(1, "W")
    df_group["DIFF"] = df_group["DIFF"].fillna(0)

    df_group["ORDER_MATERIAL"] = 0
    for i in range(df_group.shape[0]):
        if df_group.loc[i, "DIFF"] > threshold:
            order_promo += 1
        elif (
            (df_group.loc[(i - threshold) : (i-1), ["PRI_SALES"]]).sum().sum() == 0
        ):
            order_promo += 1
        df_group.loc[i, "ORDER_MATERIAL"] = order_promo

    # df_group["CONTRIBUTE_CUMSUM"] = df_group.groupby(["MATERIAL","ORDER_MATERIAL"])["CONTRIBUTE"].cumsum().max()
    # df_group[df_group["CONTRIBUTE_CUMSUM"] < 0.01]["PRI_SALES"] = 0
    df_temp_group = pd.DataFrame()
    for _, df_order_material in df_group.groupby(["MATERIAL","ORDER_MATERIAL"]):
        if (df_order_material["CONTRIBUTE"].cumsum().max() < 0.01):
            df_order_material["PRI_SALES"] = 0
        
        df_temp_group = pd.concat([df_temp_group, df_order_material])

    df_new = pd.concat([df_new, df_temp_group])

# COMMAND ----------

df_promotion = (
    df_new.copy()
    .drop(["DIFF","SUM_PRISALES_MATERIAL","CONTRIBUTE","ORDER_MATERIAL"], axis=1)
    .sort_values(["CATEGORY", "DPNAME", "MATERIAL", "DATE"])
    .reset_index(drop=True)
)

# COMMAND ----------

df_promotion["UPLIFT_BY_DP"] = (
    df_promotion["PRI_SALES_DP"] - df_promotion["PRI_BASELINE_WEEKLY"]
)

df_promotion["UPLIFT_BY_DP"] = df_promotion["UPLIFT_BY_DP"].replace([-np.inf, np.inf], 0).fillna(0)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Detect different promotion in SKU

# COMMAND ----------

threshold = 2
df_new = pd.DataFrame()

for key, df_group in tqdm(df_promotion.groupby(["CATEGORY", "DPNAME", "MATERIAL"])):
    order_promo = 0
    df_group = df_group.sort_values("DATE").reset_index(drop=True)
    df_group["DIFF"] = df_group[
        "DATE"
    ].diff(1) / np.timedelta64(1, "W")
    df_group["DIFF"] = df_group["DIFF"].fillna(0)

    df_group["ORDER_PROMO"] = 0
    for i in range(df_group.shape[0]):
        if df_group.loc[i, "DIFF"] > threshold:
            order_promo += 1
        elif (
            (df_group.loc[(i - threshold) : (i-1), ["PRI_SALES"]]).sum().sum() == 0
        ):
            order_promo += 1
        df_group.loc[i, "ORDER_PROMO"] = order_promo

    df_new = pd.concat([df_new, df_group])

# COMMAND ----------

display(df_new[df_new["DPNAME"] == "SUNSILK CONDITIONER SOFT & SMOOTH 6G"][["YEARWEEK","MATERIAL","PRI_SALES","SEC_SALES","ORDER_PROMO","DIFF","DATE"]])

# COMMAND ----------

df_promotion = (
    df_new.copy()
    .drop("DIFF", axis=1)
    .sort_values(["CATEGORY", "DPNAME", "MATERIAL", "DATE"])
    .reset_index(drop=True)
)
df_promotion["ORDER_PROMO"].value_counts()

# COMMAND ----------

df_promotion["SUM_PRI_PROMO"] = (
    df_promotion.groupby(
        [
            "CATEGORY",
            "DPNAME",
            "MATERIAL",
            "ORDER_PROMO"
        ]
    )["PRI_SALES"]
    .transform("sum")
    .astype(float)
)

df_promotion["PERCENT_PRI_PROMO"] = df_promotion["PRI_SALES"] / df_promotion["SUM_PRI_PROMO"]
df_promotion["PERCENT_PRI_PROMO"] = df_promotion["PERCENT_PRI_PROMO"].fillna(0)

df_promotion["CUMSUM_PERCENT_PRI_PROMO"] = (
    df_promotion.groupby(
        [
            "CATEGORY",
            "DPNAME",
            "MATERIAL",
            "ORDER_PROMO"
        ]
    )["PERCENT_PRI_PROMO"]
    .transform("cumsum")
    .astype(float)
)

df_promotion["CUMSUM_SALES_PRI_PROMO"] = (
    df_promotion.groupby(
        [
            "CATEGORY",
            "DPNAME",
            "MATERIAL",
            "ORDER_PROMO"
        ]
    )["PRI_SALES"]
    .transform("cumsum")
    .astype(float)
)

df_promotion["CUMSUM_SALES_SEC_PROMO"] = (
    df_promotion.groupby(
        [
            "CATEGORY",
            "DPNAME",
            "MATERIAL",
            "ORDER_PROMO"
        ]
    )["SEC_SALES"]
    .transform("cumsum")
    .astype(float)
)

df_promotion = df_promotion.fillna(0)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Calculate Uplift Preload

# COMMAND ----------

df_promotion_idx = df_promotion.copy()
df_promotion_idx["INDEX"] = df_promotion_idx.index
df_promotion_idx = df_promotion_idx[
    [
        "CATEGORY",
        "DPNAME",
        "MATERIAL",
        "ORDER_PROMO",
        "CUMSUM_PERCENT_PRI_PROMO",
        "INDEX",
    ]
]
print(df_promotion_idx.shape)
df_promotion_idx.head(3)

# COMMAND ----------

df_preload = (
    df_promotion.groupby(
        ["CATEGORY", "DPNAME", "MATERIAL", "ORDER_PROMO"],
        as_index=False,
    )["CUMSUM_PERCENT_PRI_PROMO"]
    .first()
    .merge(
        df_promotion_idx,
        on=[
            "CATEGORY",
            "DPNAME",
            "MATERIAL",
            "ORDER_PROMO",
            "CUMSUM_PERCENT_PRI_PROMO",
        ],
    )
    .merge(
        df_promotion[df_promotion["CUMSUM_PERCENT_PRI_PROMO"] >= 0.9]
        .groupby(
            ["CATEGORY", "DPNAME", "MATERIAL", "ORDER_PROMO"],
            as_index=False,
        )["CUMSUM_PERCENT_PRI_PROMO"]
        .first()
        .merge(
            df_promotion_idx,
            on=[
                "CATEGORY",
                "DPNAME",
                "MATERIAL",
                "ORDER_PROMO",
                "CUMSUM_PERCENT_PRI_PROMO",
            ],
        ),
        on=["CATEGORY", "DPNAME", "MATERIAL", "ORDER_PROMO"],
    )
)

df_preload = df_preload.drop(
    ["CUMSUM_PERCENT_PRI_PROMO_x", "CUMSUM_PERCENT_PRI_PROMO_y"], axis=1
)
df_preload = df_preload.rename(
    columns={"INDEX_x": "INDEX_START", "INDEX_y": "INDEX_END"}
)
df_preload = df_preload.drop_duplicates(
    subset=["CATEGORY","DPNAME","MATERIAL","ORDER_PROMO"],
    keep="first",
)
df_preload = df_preload.sort_values(["CATEGORY","DPNAME","MATERIAL","ORDER_PROMO"])
df_preload

# COMMAND ----------

df_promotion = df_promotion.merge(df_preload, on = ["CATEGORY","DPNAME","MATERIAL","ORDER_PROMO"], how = "left")

# COMMAND ----------

df_no_preload = df_promotion[df_promotion["INDEX_START"].isnull()]
df_no_preload.describe()

# COMMAND ----------

print("Percent promotion that have no preload in data promotion")
for year_idx in df_promotion["YEAR"].unique():
    print(year_idx,round(df_no_preload[df_no_preload["YEAR"] == year_idx].shape[0] / df_promotion[df_promotion["YEAR"] == year_idx].shape[0] * 100, 2))

# COMMAND ----------

df_promotion = df_promotion[df_promotion["INDEX_START"].notnull()]

# COMMAND ----------

df_promotion["PRELOAD_VOLUME"] = 0
df_promotion["PRELOAD_TIMERUN"] = 0
df_promotion["PRELOAD_START"] = 0
df_promotion["PRELOAD_END"] = 0
df_promotion["PRELOAD_90%_SUM"] = 0

df_promotion["PRELOAD_VOLUME"][
    (df_promotion.index >= df_promotion["INDEX_START"])
    & (df_promotion.index <= df_promotion["INDEX_END"])
] = df_promotion["UPLIFT_BY_DP"]
# contribute_material * uplift_by_dp

df_promotion["PRELOAD_START"][
    (df_promotion.index >= df_promotion["INDEX_START"])
    & (df_promotion.index <= df_promotion["INDEX_END"])
] = df_promotion["DATE"][df_promotion.index == df_promotion["INDEX_START"]]

df_promotion["PRELOAD_END"][
    (df_promotion.index >= df_promotion["INDEX_START"])
    & (df_promotion.index <= df_promotion["INDEX_END"])
] = df_promotion["DATE"][df_promotion.index == df_promotion["INDEX_END"]]

df_promotion["PRELOAD_90%_SUM"][
    (df_promotion.index >= df_promotion["INDEX_START"])
    & (df_promotion.index <= df_promotion["INDEX_END"])
] = df_promotion["CUMSUM_SALES_PRI_PROMO"][
    df_promotion.index == df_promotion["INDEX_END"]
]

df_promotion["PRELOAD_START"] = pd.to_datetime(
    df_promotion["PRELOAD_START"], errors="coerce"
)
df_promotion["PRELOAD_END"] = pd.to_datetime(
    df_promotion["PRELOAD_END"], errors="coerce"
)

df_promotion["PRELOAD_90%_SUM"] = df_promotion.groupby(["CATEGORY","DPNAME","MATERIAL","ORDER_PROMO"])["PRELOAD_90%_SUM"].transform("max").astype(float)

df_promotion["PRELOAD_START"][
    (df_promotion.index >= df_promotion["INDEX_START"])
    & (df_promotion.index <= df_promotion["INDEX_END"])
] = df_promotion["PRELOAD_START"].ffill()

df_promotion["PRELOAD_END"][
    (df_promotion.index >= df_promotion["INDEX_START"])
    & (df_promotion.index <= df_promotion["INDEX_END"])
] = df_promotion["PRELOAD_END"].bfill()

# df_promotion["PRELOAD_TIMERUN"] = (
#     df_promotion["PRELOAD_END"] - df_promotion["PRELOAD_START"]
# ) / np.timedelta64(1, "W") + 1

# df_promotion["PRELOAD_TIMERUN"] = df_promotion["PRELOAD_TIMERUN"].fillna(0)

df_promotion["PRELOAD_TIMERUN"][
    (df_promotion.index >= df_promotion["INDEX_START"])
    & (df_promotion.index <= df_promotion["INDEX_END"])
] = (df_promotion["INDEX_END"] - df_promotion["INDEX_START"] + 1)

df_promotion["PRELOAD_VOLUME"][(df_promotion["PRELOAD_TIMERUN"] == 0) | (df_promotion["PRELOAD_TIMERUN"].isnull())] = 0

df_promotion = df_promotion.drop(["INDEX_START", "INDEX_END"], axis=1)

# COMMAND ----------

display(df_promotion[df_promotion["DPNAME"] == "CLOSE-UP PASTE GREEN 200 GR"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### SHIFT SALES OF FIRST WEEKs TO THE NEXT WEEK WHEN FIRST WEEK PRELOAD HAVE UPLIFT < 0

# COMMAND ----------

display(df_promotion[df_promotion["MATERIAL"] == 68808631])

# COMMAND ----------

df_promotion = df_promotion.sort_values(
    ["CATEGORY", "DPNAME", "MATERIAL", "YEARWEEK"]
).reset_index(drop=True)

df_temp = df_promotion.copy()
df_temp["TIMESTEP_PRELOAD"] = df_temp.groupby(
    ["CATEGORY", "DPNAME", "MATERIAL", "ORDER_PROMO", "PRELOAD_TIMERUN"]
).cumcount()

df_new = pd.DataFrame(columns=df_temp.columns)
for key, df_group in df_temp.groupby(
    ["CATEGORY", "DPNAME", "MATERIAL", "ORDER_PROMO", "PRELOAD_TIMERUN"]
):
    if df_group["PRELOAD_TIMERUN"].sum() > 1:
        df_group = df_group.reset_index(drop=True)
        if (
            (df_group.loc[0, "TIMESTEP_PRELOAD"] == 0)
            & (df_group.loc[0, "UPLIFT_BY_DP"] < 0)
            & (df_group.loc[0, "PRELOAD_TIMERUN"] > 0)
        ):
            cum_index = 0
            while (df_group.loc[cum_index, "UPLIFT_BY_DP"] < 0) & (
                df_group.loc[cum_index, "TIMESTEP_PRELOAD"]
                < df_group["TIMESTEP_PRELOAD"].max()
            ):
                cum_index += 1
            
            sum_pri_sale_shift = df_group.loc[0 : (cum_index - 1), "PRI_SALES"].sum()
            sum_sec_sale_shift = df_group.loc[0 : (cum_index - 1), "SEC_SALES"].sum()

            print(key, sum_pri_sale_shift, cum_index, sum_sec_sale_shift)
            df_group.loc[0 : (cum_index - 1), "PRI_SALES"] = 0
            df_group.loc[0 : (cum_index - 1), "SEC_SALES"] = 0
            df_group.loc[0 : (cum_index - 1), "PRELOAD_TIMERUN"] = 0
            df_group.loc[0 : (cum_index - 1), "PRELOAD_VOLUME"] = 0

            df_group.loc[cum_index, "PRI_SALES"] = (
                df_group.loc[cum_index, "PRI_SALES"] + sum_pri_sale_shift
            )
            df_group.loc[cum_index, "SEC_SALES"] = (
                df_group.loc[cum_index, "SEC_SALES"] + sum_sec_sale_shift
            )
            df_group.loc[cum_index:, "PRELOAD_TIMERUN"] = (
                df_group.loc[cum_index:, "PRELOAD_TIMERUN"] - cum_index
            )

    df_new = pd.concat([df_new, df_group])

df_new = df_new.sort_values(["CATEGORY", "DPNAME", "MATERIAL","YEARWEEK"]).reset_index(
    drop=True
)

# COMMAND ----------

display(df_new[df_new["DPNAME"] == "OMO RED 6000 GR"]), display(df_new[df_new["MATERIAL"] == 68808631])

# COMMAND ----------

df_promotion = (
    df_new.copy()
    .drop(["TIMESTEP_PRELOAD"], axis=1)
    .sort_values(["CATEGORY", "DPNAME", "MATERIAL", "YEARWEEK"])
    .reset_index(drop=True)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### SHIFT SALES OF FIRST WEEKs contribute < 5% total sales in order_promo TO THE NEXT WEEK

# COMMAND ----------

display(df_promotion[df_promotion["DPNAME"] == "OMO LIQUID MATIC FLL BEAUTY CARE (POU) 3.7KG"])

# COMMAND ----------

threshold = 0.05

df_promotion["TIMESTEP_PRELOAD"] = df_promotion.groupby(
    ["CATEGORY", "DPNAME", "MATERIAL", "ORDER_PROMO", "PRELOAD_TIMERUN"]
).cumcount()

df_new = pd.DataFrame(columns=df_promotion.columns)

for key, df_group in df_promotion.groupby(
    ["CATEGORY", "DPNAME", "MATERIAL", "ORDER_PROMO", "PRELOAD_TIMERUN"]
):
    if df_group["PRELOAD_TIMERUN"].sum() > 1:
        df_group = df_group.reset_index(drop=True)
        if (
            (df_group.loc[0, "TIMESTEP_PRELOAD"] == 0)
            & (df_group.loc[0, "CUMSUM_PERCENT_PRI_PROMO"] < threshold)
            & (df_group.loc[0, "PRELOAD_TIMERUN"] > 0)
        ):
            cum_index = 0
            while (df_group.loc[cum_index, "CUMSUM_PERCENT_PRI_PROMO"] < threshold) & (
                df_group.loc[cum_index, "TIMESTEP_PRELOAD"] < df_group["TIMESTEP_PRELOAD"].max()
            ):
                cum_index += 1
            
            sum_pri_sale_shift = df_group.loc[0 : (cum_index - 1), "PRI_SALES"].sum()
            sum_sec_sale_shift = df_group.loc[0 : (cum_index - 1), "SEC_SALES"].sum()
            print(key, sum_pri_sale_shift, cum_index, sum_sec_sale_shift)
            df_group.loc[0 : (cum_index - 1), "PRI_SALES"] = 0
            df_group.loc[0 : (cum_index - 1), "SEC_SALES"] = 0
            df_group.loc[0 : (cum_index - 1), "PRELOAD_TIMERUN"] = 0
            df_group.loc[0 : (cum_index - 1), "PRELOAD_VOLUME"] = 0

            df_group.loc[cum_index, "PRI_SALES"] = (
                df_group.loc[cum_index, "PRI_SALES"] + sum_pri_sale_shift
            )
            df_group.loc[cum_index, "SEC_SALES"] = (
                df_group.loc[cum_index, "SEC_SALES"] + sum_sec_sale_shift
            )
            df_group.loc[cum_index:, "PRELOAD_TIMERUN"] = (
                df_group.loc[cum_index:, "PRELOAD_TIMERUN"] - cum_index
            )

    df_new = pd.concat([df_new, df_group])

df_new = df_new.sort_values(["CATEGORY", "DPNAME", "MATERIAL","YEARWEEK"]).reset_index(
    drop=True
)

# COMMAND ----------

display(df_new[df_new["DPNAME"] == "OMO LIQUID MATIC FLL BEAUTY CARE (POU) 3.7KG"])

# COMMAND ----------

df_promotion = (
    df_new.copy()
    .sort_values(["CATEGORY", "DPNAME", "MATERIAL", "YEARWEEK"])
    .reset_index(drop=True)
).drop(["TIMESTEP_PRELOAD"], axis=1)

# COMMAND ----------

print(df_promotion.shape)
df_promotion = df_promotion[~((df_promotion["PRI_SALES"] == 0) & (df_promotion["SEC_SALES"] == 0))]
df_promotion = df_promotion.sort_values(["CATEGORY","DPNAME","MATERIAL","YEARWEEK"]).reset_index(drop = True)
df_promotion.shape

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### ADD QUARTER FIRST WEEK PRELOAD TO PHASING DP|QUARTERLY

# COMMAND ----------

# df_promotion["QUARTER"] = ((df_promotion["MONTH"] - 1) / 3).astype(int) + 1
# temp_groupby = (
#     df_promotion.groupby(["CATEGORY", "DPNAME", "MATERIAL", "ORDER_PROMO"])["QUARTER"]
#     .first()
#     .reset_index()
# )
# temp_groupby = temp_groupby.rename(columns={"QUARTER": "QUARTER_START_PRELOAD"})
# df_promotion = df_promotion.merge(
#     temp_groupby, on=["CATEGORY", "DPNAME", "MATERIAL", "ORDER_PROMO"], how="right"
# )
# df_promotion = df_promotion.sort_values(
#     ["CATEGORY", "DPNAME", "MATERIAL", "YEARWEEK"]
# ).reset_index(drop=True)
# display(df_promotion[df_promotion["DPNAME"] == "OMO RED 6000 GR"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Calculate Uplift Postload

# COMMAND ----------

df_promotion_idx = df_promotion.copy()
df_promotion_idx["INDEX"] = df_promotion_idx.index
df_promotion_idx = df_promotion_idx[["CATEGORY","DPNAME","MATERIAL","ORDER_PROMO","CUMSUM_SALES_SEC_PROMO","INDEX"]]
print(df_promotion_idx.shape)
df_promotion_idx.head(1)

# COMMAND ----------

df_backup = df_promotion.copy()
# 69605242

# COMMAND ----------

df_postload = (
    df_promotion.groupby(
        ["CATEGORY", "DPNAME", "MATERIAL", "ORDER_PROMO"], as_index=False
    )["CUMSUM_SALES_SEC_PROMO"]
    .first()
    .merge(
        df_promotion_idx,
        on=[
            "CATEGORY",
            "DPNAME",
            "MATERIAL",
            "ORDER_PROMO",
            "CUMSUM_SALES_SEC_PROMO",
        ],
    )
    .merge(
        df_promotion[
            df_promotion["CUMSUM_SALES_SEC_PROMO"] >= df_promotion["PRELOAD_90%_SUM"]
        ]
        .groupby(["CATEGORY", "DPNAME", "MATERIAL", "ORDER_PROMO"], as_index=False)[
            "CUMSUM_SALES_SEC_PROMO"
        ]
        .first()
        .merge(
            df_promotion_idx,
            on=[
                "CATEGORY",
                "DPNAME",
                "MATERIAL",
                "ORDER_PROMO",
                "CUMSUM_SALES_SEC_PROMO",
            ],
        ),
        on=["CATEGORY", "DPNAME", "MATERIAL", "ORDER_PROMO"],
    )
)

df_postload = df_postload.drop(
    ["CUMSUM_SALES_SEC_PROMO_x", "CUMSUM_SALES_SEC_PROMO_y"], axis=1
)
df_postload = df_postload.rename(
    columns={"INDEX_x": "INDEX_START", "INDEX_y": "INDEX_END"}
)
df_postload = df_postload.drop_duplicates(
    subset=["CATEGORY", "DPNAME", "MATERIAL", "ORDER_PROMO"], keep="first"
)
df_postload = df_postload.sort_values(["CATEGORY", "DPNAME", "MATERIAL", "ORDER_PROMO"])
df_postload

# COMMAND ----------

df_promotion = df_promotion.merge(df_postload, on =["CATEGORY","DPNAME", "MATERIAL","ORDER_PROMO"], how = "left")

# COMMAND ----------

display(df_promotion[df_promotion["INDEX_START"].isnull()])

# COMMAND ----------

df_backup = df_promotion.copy()
# df_promotion = df_backup.copy()

# COMMAND ----------

df_promotion["POSTLOAD_VOLUME"] = 0
df_promotion["POSTLOAD_TIMERUN"] = np.nan

df_promotion["POSTLOAD_TIMERUN"][
    (df_promotion.index >= df_promotion["INDEX_START"])
    & (df_promotion.index <= df_promotion["INDEX_END"])
    # (
    #     df_promotion["DATE"]
    #     == (df_promotion["PRELOAD_END"] + datetime.timedelta(weeks=1)).shift(1)
    # )
    # | (
    #     df_promotion["DATE"]
    #     == (df_promotion["PRELOAD_END"] + datetime.timedelta(weeks=2)).shift(1)
    # )
] = (df_promotion["INDEX_END"] - df_promotion["INDEX_START"] + 1)
# (
#     df_promotion["DATE"][df_promotion.index == df_promotion["INDEX_END"]]
#     - df_promotion["DATE"][df_promotion.index == df_promotion["INDEX_START"]]
# ) / np.timedelta64(1, "W") + 1

df_promotion["POSTLOAD_VOLUME"][df_promotion["POSTLOAD_TIMERUN"] > 0] = df_promotion["UPLIFT_BY_DP"]
# df_promotion["UPLIFT_BY_DP"] * df_promotion["CONTRIBUTE_MATERIAL"]

df_promotion["MASK_PRELOAD"] = df_promotion["PRELOAD_TIMERUN"].replace(0, np.nan).ffill()
df_promotion["POSTLOAD_TIMERUN"] = df_promotion["POSTLOAD_TIMERUN"] - df_promotion["MASK_PRELOAD"]
df_promotion["POSTLOAD_VOLUME"][df_promotion["PRELOAD_TIMERUN"] != 0] = 0
df_promotion["POSTLOAD_TIMERUN"][df_promotion["PRELOAD_TIMERUN"] != 0] = 0

df_promotion["POSTLOAD_VOLUME"][(df_promotion["POSTLOAD_TIMERUN"] == 0) | (df_promotion["POSTLOAD_TIMERUN"].isnull())] = 0


df_promotion = df_promotion.drop(["INDEX_START", "INDEX_END", "MASK_PRELOAD"], axis=1)

# COMMAND ----------

display(df_promotion[df_promotion["MATERIAL"] == 68631318])

# COMMAND ----------

# df_new = pd.DataFrame()

# for key, df_group in tqdm(
#     df_promotion.groupby(["CATEGORY", "DPNAME", "MATERIAL", "ORDER_PROMO"])
# ):
#     postload_timerun = df_group["POSTLOAD_TIMERUN"].max()
#     preload_timerun = df_group["PRELOAD_TIMERUN"].max()

#     df_group["ACTUAL_POSTLOAD_TIMERUN"] = np.nan
#     df_group["CUMCOUNT"] = df_group.groupby(["CATEGORY","DPNAME","MATERIAL","ORDER_PROMO"]).cumcount() 
#     df_group["FULLFILL"] = df_group["POSTLOAD_TIMERUN"].fillna(
#         df_group["POSTLOAD_TIMERUN"].sum()
#     )

#     df_group["DIFF"] = df_group["CUMCOUNT"] - preload_timerun
#     df_group["ACTUAL_POSTLOAD_TIMERUN"][
#         (df_group["DIFF"] >= 0) & (df_group["DIFF"] < postload_timerun)
#     ] = postload_timerun

#     df_new = pd.concat([df_new, df_group])

# df_new["ACTUAL_POSTLOAD_TIMERUN"][df_new["PRELOAD_TIMERUN"] > 0] = 0

# df_new["POSTLOAD_VOLUME"][df_new["ACTUAL_POSTLOAD_TIMERUN"] > 0] = df_new["UPLIFT_BY_DP"]

# COMMAND ----------

df_promotion = df_promotion.sort_values(["CATEGORY","DPNAME","MATERIAL","YEARWEEK"]).reset_index(drop = True)

# COMMAND ----------

df_check_overlap = pd.DataFrame()

for key, df_group in tqdm(df_promotion.groupby(["CATEGORY","DPNAME","YEARWEEK"])):
    # if df_group.shape[0] > 1:
    if (df_group["PRELOAD_VOLUME"].sum() != 0) & (df_group["POSTLOAD_VOLUME"].sum() != 0):
        df_group["POSTLOAD_VOLUME"] = 0
        
    check_overlap_preload = df_group["PRELOAD_VOLUME"][df_group["PRELOAD_VOLUME"] != 0].max()
    if (df_group["PRELOAD_VOLUME"].sum() != check_overlap_preload):
        df_group["PRELOAD_VOLUME"] = df_group["PRELOAD_VOLUME"] * df_group["CONTRIBUTE_MATERIAL"]
    
    check_overlap_postload = df_group["POSTLOAD_VOLUME"][df_group["POSTLOAD_VOLUME"] != 0].max()
    if (df_group["POSTLOAD_VOLUME"].sum() != check_overlap_preload):
        df_group["POSTLOAD_VOLUME"] = df_group["POSTLOAD_VOLUME"] * df_group["CONTRIBUTE_MATERIAL"]

    df_check_overlap = pd.concat([df_check_overlap, df_group])

# COMMAND ----------

df_promotion = df_check_overlap.copy().sort_values(["CATEGORY","DPNAME","MATERIAL","YEARWEEK"]).reset_index(drop = True)
df_promotion = df_promotion.drop(
    [
        # "FULLFILL",
        # "CUMCOUNT",
        # "DIFF",
        "PRELOAD_START",
        "PRELOAD_END",
        # "PRELOAD_90%_SUM",
        # "CUMSUM_SALES_PRI_PROMO",
        # "CUMSUM_SALES_SEC_PROMO",
        # "PERCENT_PRI_PROMO",
        # "CUMSUM_PERCENT_PRI_PROMO",
        # "SUM_PRI_PROMO",
        # "SKU_TYPE",
    ],
    axis=1,
)
# df_promotion = df_promotion.rename(
#     columns={"ACTUAL_POSTLOAD_TIMERUN": "POSTLOAD_TIMERUN"}
# )

# COMMAND ----------

display(df_promotion)

# COMMAND ----------

display(df_promotion[df_promotion["DPNAME"] == "CLOSE-UP PASTE WHITENING 200 GR"])

# COMMAND ----------

print(df_promotion.shape)
df_promotion = df_promotion[(df_promotion["PRELOAD_TIMERUN"] > 0) | (df_promotion["POSTLOAD_TIMERUN"] > 0)]
print(df_promotion.shape)
display(df_promotion)

# COMMAND ----------

print(df_promotion.isnull().sum())
df_promotion.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC # Phasing Preload & Postload

# COMMAND ----------

def phasing_uplift(df_promotion):
    phasing_preload = []
    phasing_postload = []

    for key, df_group in tqdm(df_promotion.groupby(["CATEGORY", "DPNAME"])):
        if (    
            df_group[df_group["PRELOAD_TIMERUN"] > 0].shape[0] > 0
        ):  # just 4 sure exist preload time
            mode_pre = (
                df_group[df_group["PRELOAD_TIMERUN"] > 0]
                .groupby(["DPNAME", "MATERIAL", "ORDER_PROMO"])["PRELOAD_TIMERUN"]
                .unique()
                # .mode()
            )
            if len(mode_pre) > 1:
                mode_pre = mode_pre.mode().astype(int).values
                if len(np.delete(mode_pre, np.where(mode_pre == 1))) > 0:
                    mode_pre = np.sort(np.delete(mode_pre, np.where(mode_pre == 1)))
                    mode_pre = mode_pre[(len(mode_pre) - 1) // 2] # median
                else:
                    mode_pre = mode_pre[0]
            else:
                mode_pre = mode_pre.astype(int)[0]

            df_preload = df_group[df_group["PRELOAD_TIMERUN"] == mode_pre]

            df_preload["PRELOAD_TIMELINE"] = (
                df_preload.groupby(["DPNAME", "MATERIAL", "ORDER_PROMO"]).cumcount() + 1
            )

            phasing_preload_DP = (
                df_preload.groupby(["CATEGORY", "DPNAME","PRELOAD_TIMELINE"])
                .agg({"PRELOAD_WEEKLY": ["mean", "median"]})
                .reset_index()
            )
            phasing_preload_DP.columns = [
                "_".join(col) for col in phasing_preload_DP.columns
            ]

            phasing_preload_DP = phasing_preload_DP.rename(
                columns={
                    "CATEGORY_": "CATEGORY",
                    "DPNAME_": "DPNAME",
                    # "QUARTER_START_PRELOAD_": "QUARTER_START_PRELOAD",
                    "PRELOAD_TIMELINE_": "PRELOAD_TIMELINE",
                }
            )

            phasing_preload_DP["PHASING_PRELOAD_MEAN"] = phasing_preload_DP[
                "PRELOAD_WEEKLY_mean"
            ] / phasing_preload_DP.groupby(["CATEGORY", "DPNAME"])[
                "PRELOAD_WEEKLY_mean"
            ].transform(
                "sum"
            ).astype(
                float
            )

            phasing_preload_DP["PHASING_PRELOAD_MEDIAN"] = phasing_preload_DP[
                "PRELOAD_WEEKLY_median"
            ] / phasing_preload_DP.groupby(["CATEGORY", "DPNAME"])[
                "PRELOAD_WEEKLY_median"
            ].transform(
                "sum"
            ).astype(
                float
            )

            phasing_preload_DP = phasing_preload_DP.fillna(0)
            phasing_preload_DP = phasing_preload_DP.replace([-np.inf, np.inf], 0)

            phasing_preload_DP["KEY"] = "|".join(key)

            phasing_preload.append(phasing_preload_DP)
        # *******************************************************************************
        if df_group[df_group["POSTLOAD_TIMERUN"] > 0].shape[0] > 0:
            mode_post = (
                df_group[df_group["POSTLOAD_TIMERUN"] > 0]
                .groupby(["DPNAME", "MATERIAL", "ORDER_PROMO"])["POSTLOAD_TIMERUN"]
                .unique()
                # .mode()
            )
            if len(mode_post) > 1:
                mode_post = mode_post.mode().astype(int).values
                if len(np.delete(mode_post, np.where(mode_post == 1))) > 0:
                    mode_post = np.sort(np.delete(mode_post, np.where(mode_post == 1)))
                    mode_post = mode_post[(len(mode_post) - 1) // 2] # median
                else:
                    mode_post = mode_post[0]
            else:
                mode_post = mode_post.astype(int)[0]

            df_postload = df_group[df_group["POSTLOAD_TIMERUN"] == mode_post]

            df_postload["POSTLOAD_TIMELINE"] = (
                df_postload.groupby(["DPNAME", "MATERIAL", "ORDER_PROMO"]).cumcount()
                + 1
            )

            phasing_postload_DP = (
                df_postload.groupby(["CATEGORY", "DPNAME","POSTLOAD_TIMELINE"])
                .agg({"POSTLOAD_WEEKLY": ["mean", "median"]})
                .reset_index()
            )
            phasing_postload_DP.columns = [
                "_".join(col) for col in phasing_postload_DP.columns
            ]

            phasing_postload_DP = phasing_postload_DP.rename(
                columns={
                    "CATEGORY_": "CATEGORY",
                    "DPNAME_": "DPNAME",
                    # "QUARTER_START_PRELOAD_": "QUARTER_START_PRELOAD",
                    "POSTLOAD_TIMELINE_": "POSTLOAD_TIMELINE",
                }
            )

            phasing_postload_DP["PHASING_POSTLOAD_MEAN"] = phasing_postload_DP[
                "POSTLOAD_WEEKLY_mean"
            ] / phasing_postload_DP.groupby(["CATEGORY", "DPNAME"])[
                "POSTLOAD_WEEKLY_mean"
            ].transform(
                "sum"
            ).astype(
                float
            )

            phasing_postload_DP["PHASING_POSTLOAD_MEDIAN"] = phasing_postload_DP[
                "POSTLOAD_WEEKLY_median"
            ] / phasing_postload_DP.groupby(["CATEGORY", "DPNAME"])[
                "POSTLOAD_WEEKLY_median"
            ].transform(
                "sum"
            ).astype(
                float
            )

            phasing_postload_DP = phasing_postload_DP.fillna(0)
            phasing_postload_DP = phasing_postload_DP.replace([-np.inf, np.inf], 0)

            phasing_postload_DP["KEY"] = "|".join(key)

            phasing_postload.append(phasing_postload_DP)

    # ********************************************************
    df_phasing_preload = pd.concat(phasing_preload)

    df_phasing_postload = pd.concat(phasing_postload)

    return df_phasing_preload, df_phasing_postload

# COMMAND ----------

df_backup = df_promotion.copy()

# COMMAND ----------

df_promotion = df_backup.copy()
df_promotion = df_promotion.dropna(subset=["POSTLOAD_TIMERUN"]).reset_index(drop=True)

df_promotion["SUM_PRI_BASE_IN_PRELOAD"] = (
    df_promotion[df_promotion["PRELOAD_TIMERUN"] > 0]
    .groupby(["CATEGORY", "DPNAME", "MATERIAL", "ORDER_PROMO"])["PRI_BASELINE_WEEKLY"]
    .transform("sum")
    .astype(float)
)
df_promotion["SUM_PRI_BASE_IN_PRELOAD"][
    df_promotion["SUM_PRI_BASE_IN_PRELOAD"].isna()
] = (
    df_promotion.groupby(["CATEGORY", "DPNAME", "MATERIAL", "ORDER_PROMO"])[
        "SUM_PRI_BASE_IN_PRELOAD"
    ]
    .transform("max")
    .astype(float)
)

df_promotion["SUM_PRELOAD_PROMO"] = (
    df_promotion.groupby(["CATEGORY", "DPNAME", "MATERIAL", "ORDER_PROMO"])[
        "PRELOAD_VOLUME"
    ]
    .transform("sum")
    .astype(float)
)
df_promotion["CONTRIBUTE_PRELOAD_PROMO"] = (
    df_promotion["SUM_PRELOAD_PROMO"] / df_promotion["SUM_PRI_BASE_IN_PRELOAD"]
)
df_promotion["CONTRIBUTE_PRELOAD_PROMO"] = (
    df_promotion["CONTRIBUTE_PRELOAD_PROMO"].replace([-np.inf, np.inf], 0).fillna(0)
)

df_promotion["SUM_PRI_BASE_IN_POSTLOAD"] = (
    df_promotion[df_promotion["POSTLOAD_TIMERUN"] > 0]
    .groupby(["CATEGORY", "DPNAME", "MATERIAL", "ORDER_PROMO"])["PRI_BASELINE_WEEKLY"]
    .transform("sum")
    .astype(float)
)
df_promotion["SUM_PRI_BASE_IN_POSTLOAD"][
    df_promotion["SUM_PRI_BASE_IN_POSTLOAD"].isna()
] = (
    df_promotion.groupby(["CATEGORY", "DPNAME", "MATERIAL", "ORDER_PROMO"])[
        "SUM_PRI_BASE_IN_POSTLOAD"
    ]
    .transform("max")
    .astype(float)
)

df_promotion["SUM_POSTLOAD_PROMO"] = (
    df_promotion.groupby(["CATEGORY", "DPNAME", "MATERIAL", "ORDER_PROMO"])[
        "POSTLOAD_VOLUME"
    ]
    .transform("sum")
    .astype(float)
)
df_promotion["CONTRIBUTE_POSTLOAD_PROMO"] = (
    df_promotion["SUM_POSTLOAD_PROMO"] / df_promotion["SUM_PRI_BASE_IN_POSTLOAD"]
)
df_promotion["CONTRIBUTE_POSTLOAD_PROMO"] = (
    df_promotion["CONTRIBUTE_POSTLOAD_PROMO"].replace([-np.inf, np.inf], 0).fillna(0)
)

df_promotion[["SUM_PRELOAD_PROMO","SUM_POSTLOAD_PROMO","SUM_PRI_BASE_IN_PRELOAD","SUM_PRI_BASE_IN_POSTLOAD","CONTRIBUTE_PRELOAD_PROMO","CONTRIBUTE_POSTLOAD_PROMO"]] = df_promotion[["SUM_PRELOAD_PROMO","SUM_POSTLOAD_PROMO","SUM_PRI_BASE_IN_PRELOAD","SUM_PRI_BASE_IN_POSTLOAD","CONTRIBUTE_PRELOAD_PROMO","CONTRIBUTE_POSTLOAD_PROMO"]].fillna(0)

df_promotion["CONTRIBUTE_UPLIFT_PROMO"] = (
    df_promotion["SUM_PRELOAD_PROMO"] + df_promotion["SUM_POSTLOAD_PROMO"]
) / (df_promotion["SUM_PRI_BASE_IN_PRELOAD"] + df_promotion["SUM_PRI_BASE_IN_POSTLOAD"])


df_promotion["PRELOAD_WEEKLY"] = df_promotion["PRI_SALES_DP"] / df_promotion[
    df_promotion["PRELOAD_TIMERUN"] > 0
].groupby(["CATEGORY", "DPNAME", "MATERIAL", "ORDER_PROMO"])["PRI_SALES_DP"].transform(
    "sum"
).astype(
    float
)
df_promotion["PRELOAD_WEEKLY"] = df_promotion["PRELOAD_WEEKLY"].fillna(0)

df_promotion["POSTLOAD_WEEKLY"] = df_promotion["PRI_SALES_DP"] / df_promotion[
    df_promotion["POSTLOAD_TIMERUN"] > 0
].groupby(["CATEGORY", "DPNAME", "MATERIAL", "ORDER_PROMO"])["PRI_SALES_DP"].transform(
    "sum"
).astype(
    float
)
df_promotion["POSTLOAD_WEEKLY"] = df_promotion["POSTLOAD_WEEKLY"].fillna(0)

# COMMAND ----------

# df_promotion.to_csv("/dbfs/mnt/adls/NMHDAT_SNOP/DT/Setup_rulebase_banded.csv", index = False)

# COMMAND ----------

display(df_promotion[df_promotion["DPNAME"] == "CF CONC. WHITE POUCH 2.8L"])

# COMMAND ----------

# df_promotion["QUARTER_START_PRELOAD"] = df_promotion["QUARTER_START_PRELOAD"].astype(str)

df_volume_uplift = (
    df_promotion.groupby(["CATEGORY", "DPNAME"])
    .agg(
        {
            "CONTRIBUTE_PRELOAD_PROMO": [(lambda x: x.unique().mean()), (lambda x: np.median(x.unique()))],
            "CONTRIBUTE_POSTLOAD_PROMO": [(lambda x: x.unique().mean()), (lambda x: np.median(x.unique()))],
            "CONTRIBUTE_UPLIFT_PROMO": [(lambda x: x.unique().mean()), (lambda x: np.median(x.unique()))],
        }
    )
    .reset_index()
)
# df_volume_uplift.columns = ["_".join(col) for col in df_volume_uplift.columns]
df_volume_uplift.columns = ["CATEGORY", "DPNAME","VOLUME_PRELOAD_MEAN","VOLUME_PRELOAD_MEDIAN","VOLUME_POSTLOAD_MEAN","VOLUME_POSTLOAD_MEDIAN","VOLUME_UPLIFT_MEAN","VOLUME_UPLIFT_MEDIAN"]

display(df_volume_uplift)

# COMMAND ----------

df_phasing_preload, df_phasing_postload = phasing_uplift(df_promotion)

# COMMAND ----------

df_phasing_preload = df_phasing_preload.sort_values(["KEY","PRELOAD_TIMELINE"]).reset_index(drop = True)
df_phasing_postload = df_phasing_postload.sort_values(["KEY","POSTLOAD_TIMELINE"]).reset_index(drop = True)

# COMMAND ----------

df_phasing_preload = df_phasing_preload.drop(
    df_phasing_preload[
        (df_phasing_preload["PHASING_PRELOAD_MEAN"] < 0.01)
        & (df_phasing_preload["PHASING_PRELOAD_MEDIAN"] < 0.01)
    ].index
).reset_index(drop = True)
df_phasing_preload["PRELOAD_TIMELINE"] = df_phasing_preload.groupby(["KEY"]).cumcount() + 1

# COMMAND ----------

print(df_phasing_preload.shape)
display(df_phasing_preload)

# COMMAND ----------

print(df_phasing_postload.shape)
display(df_phasing_postload)

# COMMAND ----------

df_phasing_preload = df_phasing_preload.rename(columns={"PRELOAD_TIMELINE": "TIMELINE"})
df_phasing_postload = df_phasing_postload.rename(
    columns={"POSTLOAD_TIMELINE": "TIMELINE"}
)

df_uplift = pd.concat([df_phasing_preload, df_phasing_postload])
df_uplift = df_uplift.sort_values(["CATEGORY", "DPNAME"])
display(df_uplift)

# COMMAND ----------

df_uplift["TYPE_UPLIFT"] = np.where(
    df_uplift["PRELOAD_WEEKLY_mean"].notnull(), "PRELOAD", "POSTLOAD"
)

# df_uplift["PRELOAD_WEEKLY_mean"][df_uplift["PRELOAD_WEEKLY_mean"].isna()] = df_uplift["POSTLOAD_WEEKLY_mean"]
# df_uplift["PRELOAD_WEEKLY_median"][df_uplift["PRELOAD_WEEKLY_median"].isna()] = df_uplift["POSTLOAD_WEEKLY_median"]

df_uplift["PHASING_PRELOAD_MEAN"][df_uplift["PHASING_PRELOAD_MEAN"].isna()] = df_uplift[
    "PHASING_POSTLOAD_MEAN"
]
df_uplift["PHASING_PRELOAD_MEDIAN"][
    df_uplift["PHASING_PRELOAD_MEDIAN"].isna()
] = df_uplift["PHASING_POSTLOAD_MEDIAN"]

df_uplift["TIMELINE"] = df_uplift.groupby(["CATEGORY", "DPNAME"]).cumcount() + 1

# COMMAND ----------

df_uplift = df_uplift[
    [
        "CATEGORY",
        "DPNAME",
        # "QUARTER_START_PRELOAD",
        "TYPE_UPLIFT",
        "TIMELINE",
        "PHASING_PRELOAD_MEAN",
        "PHASING_PRELOAD_MEDIAN",
    ]
]

df_uplift.columns = [
    "CATEGORY",
    "DPNAME",
    # "QUARTER_START_PRELOAD",
    "TYPE",
    "TIMERUN_PROMOTION",
    "PROMOTION_PHASING_MEAN",
    "PROMOTION_PHASING_MEDIAN",
]
display(df_uplift)

# COMMAND ----------

df_uplift = df_uplift.merge(df_volume_uplift, on = ["CATEGORY","DPNAME"], how = "inner")

# COMMAND ----------

display(df_uplift)

# COMMAND ----------

# df_backup.to_csv("/dbfs/mnt/adls/NMHDAT_SNOP/DT/df_banded.csv", index = False)

# df_uplift.to_csv("/dbfs/mnt/adls/NMHDAT_SNOP/DT/phasing_uplift_DP_2021_2022.csv", index = False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Evaluate baseline + uplift(banded) 1-10/2023

# COMMAND ----------

df_uplift = df_uplift[df_uplift["DPNAME"] != "SURF LIQUID FLORAL POUCH 2.9KG"]
promo_code_2023 = promo_code_2023[promo_code_2023["DPNAME"].isin(df_uplift["DPNAME"].unique())]
promo_code_2023

# COMMAND ----------

df_temp = df_uplift.merge(
    promo_code_2023.drop("MATERIAL", axis=1), on=["CATEGORY", "DPNAME"], how="inner"
)
df_temp = df_temp.sort_values(
    ["CATEGORY", "DPNAME", "START_DATE_PROMOTION", "TIMERUN_PROMOTION"]
).reset_index(drop=True)

df_temp["WEEKDAY_OVER_6"] = (7 - df_temp["START_DATE_PROMOTION"].dt.isocalendar()["day"]).astype(int)
df_temp["START_DATE_PROMOTION"][df_temp["WEEKDAY_OVER_6"].astype(int) < 1] = df_temp["START_DATE_PROMOTION"] + pd.to_timedelta(df_temp["WEEKDAY_OVER_6"] + 1, unit= "D")

df_temp["DATE_PROMOTION"] = df_temp["START_DATE_PROMOTION"] + pd.to_timedelta(
    df_temp["TIMERUN_PROMOTION"] - 1, unit="W"
)
df_temp["YEARWEEK"] = df_temp["DATE_PROMOTION"].dt.isocalendar()["year"] * 100 + df_temp["DATE_PROMOTION"].dt.isocalendar()["week"]

df_temp["VOLUME_BANDED_MEAN"] = df_temp["PROMOTION_PHASING_MEAN"] * np.where(
    df_temp["TYPE"] == "PRELOAD",
    df_temp["VOLUME_PRELOAD_MEAN"],
    df_temp["VOLUME_POSTLOAD_MEAN"],
)

df_temp["VOLUME_BANDED_MEDIAN"] = df_temp["PROMOTION_PHASING_MEDIAN"] * np.where(
    df_temp["TYPE"] == "PRELOAD",
    df_temp["VOLUME_PRELOAD_MEDIAN"],
    df_temp["VOLUME_POSTLOAD_MEDIAN"],
)

display(df_temp)

# COMMAND ----------

df_final = df_convert[df_convert["YEARWEEK"] >= 202301]
df_final = df_final.merge(df_temp, on=["CATEGORY", "DPNAME", "YEARWEEK"], how="left")

df_final["VOLUME_BANDED_MEAN"] = df_final["VOLUME_BANDED_MEAN"].fillna(0)
df_final["VOLUME_BANDED_MEDIAN"] = df_final["VOLUME_BANDED_MEDIAN"].fillna(0)

df_final["SUM_BASELINE_IN_PROMO"] = 0
df_final["SUM_BASELINE_IN_PROMO"][df_final["TYPE"] == "PRELOAD"] = (
    df_final[(df_final["TIMERUN_PROMOTION"] > 0) & (df_final["TYPE"] == "PRELOAD")]
    .groupby(["CATEGORY", "DPNAME", "START_DATE_PROMOTION"])["FC_PRI_BASELINE_WEEKLY"]
    .transform("sum")
    .astype(float)
)

df_final["SUM_BASELINE_IN_PROMO"][df_final["TYPE"] == "POSTLOAD"] = (
    df_final[(df_final["TIMERUN_PROMOTION"] > 0) & (df_final["TYPE"] == "POSTLOAD")]
    .groupby(["CATEGORY", "DPNAME", "START_DATE_PROMOTION"])["FC_PRI_BASELINE_WEEKLY"]
    .transform("sum")
    .astype(float)
)

df_final["TOTAL_SALES_MEAN"] = (
    df_final["FC_PRI_BASELINE_WEEKLY"]
    + df_final["SUM_BASELINE_IN_PROMO"] * df_final["VOLUME_BANDED_MEAN"]
)
df_final["TOTAL_SALES_MEDIAN"] = (
    df_final["FC_PRI_BASELINE_WEEKLY"]
    + df_final["SUM_BASELINE_IN_PROMO"] * df_final["VOLUME_BANDED_MEDIAN"]
)

df_final["TOTAL_SALES_MEAN"][df_final["TOTAL_SALES_MEAN"].isnull()] = df_final[
    "FC_PRI_BASELINE_WEEKLY"
]
df_final["TOTAL_SALES_MEDIAN"][df_final["TOTAL_SALES_MEDIAN"].isnull()] = df_final[
    "FC_PRI_BASELINE_WEEKLY"
]

df_final = df_final.drop_duplicates(subset=["KEY", "YEARWEEK"])

# COMMAND ----------

# df_final.to_csv("/dbfs/mnt/adls/NMHDAT_SNOP/DT/df_total_sales.csv")

# COMMAND ----------

# df_final["TOTAL_SALES_MEAN"][
#     (df_final["VOLUME_BANDED_MEAN"] != 0)
#     & (df_final["TOTAL_SALES_MEAN"] > df_final["PRI_SALES"])
#     & (df_final["FC_PRI_BASELINE_WEEKLY"] < df_final["PRI_SALES"])
# ] = df_final["FC_PRI_BASELINE_WEEKLY"]

# df_final["TOTAL_SALES_MEAN"][
#     (df_final["VOLUME_BANDED_MEAN"] != 0)
#     & (df_final["TOTAL_SALES_MEAN"] > df_final["PRI_SALES"])
#     & (df_final["FC_PRI_BASELINE_WEEKLY"] > df_final["PRI_SALES"])
#     & (df_final["FC_PRI_BASELINE_WEEKLY"] < df_final["TOTAL_SALES_MEAN"])
# ] = df_final["FC_PRI_BASELINE_WEEKLY"]

# df_final["TOTAL_SALES_MEAN"][
#     (df_final["VOLUME_BANDED_MEAN"] != 0)
#     & (df_final["TOTAL_SALES_MEAN"] < df_final["PRI_SALES"])
#     & (df_final["FC_PRI_BASELINE_WEEKLY"] < df_final["PRI_SALES"])
#     & (df_final["FC_PRI_BASELINE_WEEKLY"] > df_final["TOTAL_SALES_MEAN"])
# ] = df_final["FC_PRI_BASELINE_WEEKLY"]

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
predict_col_arr = ["TOTAL_SALES_MEAN", "TOTAL_SALES_MEDIAN"]

df_accuracy_monthly2023 = pd.DataFrame(columns=["CATEGORY"])

for month_idx in range(1, 9):
    df_pattern = df_final[
        (df_final["YEAR"] == 2023)
        & (df_final["MONTH"] == month_idx)
    ]
    df_accuracy_phase_2Y = pd.DataFrame.from_dict(
        [
            accuracy_check(key, df_group, actual_col, predict_col_arr)
            for key, df_group in df_pattern.groupby("CATEGORY")
        ]
    )

    df_accuracy_phase_2Y = df_accuracy_phase_2Y.rename(
        columns={
            "Sum_actualsale": "sum_actualsale_month_" + str(month_idx),
            "Sum_predictsale_TOTAL_SALES_MEAN": "sum_predictsale_mean_month_"
            + str(month_idx),
            "Accuracy_TOTAL_SALES_MEAN": "accuracy_mean_month_" + str(month_idx),
            "Error_TOTAL_SALES_MEAN": "error_mean_month_" + str(month_idx),
            "Sum_predictsale_TOTAL_SALES_MEDIAN": "sum_predictsale_median_month_"
            + str(month_idx),
            "Accuracy_TOTAL_SALES_MEDIAN": "accuracy_median_month_" + str(month_idx),
            "Error_TOTAL_SALES_MEDIAN": "error_median_month_" + str(month_idx),
        }
    )

    df_accuracy_monthly2023 = df_accuracy_monthly2023.merge(
        df_accuracy_phase_2Y, on=["CATEGORY"], how="outer"
    )

# COMMAND ----------

df_accuracy_monthly2023.replace([-np.inf, np.inf], 0, inplace=True)
df_accuracy_monthly2023.fillna(0, inplace=True)

df_accuracy_monthly2023[
    [
        "CATEGORY",
        "accuracy_mean_month_1",
        "accuracy_mean_month_2",
        "accuracy_mean_month_3",
        "accuracy_mean_month_4",
        "accuracy_mean_month_5",
        "accuracy_mean_month_6",
        "accuracy_mean_month_7",
        "accuracy_mean_month_8",
        # "accuracy_mean_month_9",
        # "accuracy_mean_month_10",
        "accuracy_median_month_1",
        "accuracy_median_month_2",
        "accuracy_median_month_3",
        "accuracy_median_month_4",
        "accuracy_median_month_5",
        "accuracy_median_month_6",
        "accuracy_median_month_7",
        "accuracy_median_month_8",
        # "accuracy_median_month_9",
        # "accuracy_median_month_10",
    ]
]

# COMMAND ----------

# df_accuracy_monthly2023.to_csv("/dbfs/mnt/adls/NMHDAT_SNOP/DT/accuracy_baseline&banded_monthly2023.csv", index = False)
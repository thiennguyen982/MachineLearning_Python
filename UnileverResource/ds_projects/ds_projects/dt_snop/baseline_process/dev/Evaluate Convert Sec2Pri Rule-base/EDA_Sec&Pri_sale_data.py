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

from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, median_absolute_error, mean_squared_error, r2_score

from tqdm import tqdm

%matplotlib inline
plt.rcParams['figure.figsize'] = [40, 20]
plt.rcParams['font.size'] = 20

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

fig, ax = plt.subplots(1, 3, figsize = (24, 12))

ax[0].pie(
    df_pri_sales["CATEGORY"].value_counts(),
    labels=df_pri_sales["CATEGORY"].value_counts().index,
    autopct="%.1f%%",
)
ax[0].set_title('Percentage Category of Primary ')

ax[1].pie(
    df_sec_sales["CATEGORY"].value_counts(),
    labels=df_sec_sales["CATEGORY"].value_counts().index,
    autopct="%.1f%%",
)
ax[1].set_title('Percentage Category of Secondary ')

ax[2].pie(
    df_consist_dp["CATEGORY"].value_counts(),
    labels = df_consist_dp["CATEGORY"].value_counts().index,
    autopct = "%.1f%%"
)
ax[2].set_title('Percentage Category of Data have consistence DP')
plt.tight_layout()
plt.show()

# COMMAND ----------

df_0_pri = df_consist_dp[df_consist_dp["PRI_SALES"] == 0]

t = df_0_pri.sort_values("DATE")["DATE"].value_counts()[:15]
ax = sns.barplot(x=t, y=t.index)
ax.set(xlabel="COUNT 0 SALES IN PRIMARY SALES")
plt.show()

# COMMAND ----------

df_0_pri = df_consist_dp[df_consist_dp['SEC_SALES'] == 0]

t = df_0_pri.sort_values('DATE')['DATE'].value_counts()[:20]
ax = sns.barplot(x = t, y = t.index)
ax.set(xlabel='COUNT 0 SALES IN SECONDARY SALES')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Mean Ratio TOP 20 DP largest total sale

# COMMAND ----------

with pd.ExcelWriter(
    "/Workspace/Users/ng-minh-hoang.dat@unilever.com/Mean_RATIO_20DP_largest_totalsale.xlsx"
) as writer:
    df_temp = df_consist_dp.copy()
    
    # pick pattern following 100DP largest total sale
    top_dp_largest = (
        df_temp.drop_duplicates(subset=["TOTAL_SEC_SALES"])
        .nlargest(20, columns=["TOTAL_SEC_SALES"])["DPNAME"]
        .unique()
    )

    df_temp = df_temp[df_temp["DPNAME"].isin(top_dp_largest)]
    df_temp = df_temp[df_temp["YEARWEEK"] < 202301]

    df_temp["MEAN_RATIO_WEEKLY"] = (
        df_temp.groupby(["YEAR", "MONTH", "YEARWEEK"])["RATIO_WEEKLY"]
        .transform("mean")
        .astype(float)
    )
    df_temp["MEAN_RATIO_MONTHLY"] = (
        df_temp.groupby(["YEAR", "MONTH"])["RATIO_MONTHLY"]
        .transform("mean")
        .astype(float)
    )
    df_temp["MEAN_RATIO_QUARTERLY"] = (
        df_temp.groupby(["YEAR", "QUARTER"])["RATIO_QUARTERLY"]
        .transform("mean")
        .astype(float)
    )
    df_temp = df_temp.sort_values("DATE").reset_index(drop=True)


    # **********************************************************
    quarter = pd.pivot_table(
        data=df_temp.groupby(["YEAR", "QUARTER"])["MEAN_RATIO_QUARTERLY"]
        .unique()
        .to_frame()
        .reset_index()
        .explode("MEAN_RATIO_QUARTERLY"),
        values="MEAN_RATIO_QUARTERLY",
        index="QUARTER",
        columns="YEAR",
    ).reset_index()

    quarter.replace([np.inf, -np.inf], 0, inplace=True)
    quarter.fillna(0, inplace=True)

    quarter["MEAN_RATIO_QUARTER_PHASE_ALL"] = quarter.drop("QUARTER", axis=1).T.mean()
    quarter["MEAN_RATIO_QUARTER_PHASE_1Y"] = quarter.iloc[:, 5]
    quarter["MEAN_RATIO_QUARTER_PHASE_2Y"] = quarter.iloc[:, 4:6].T.mean()
    quarter["MEAN_RATIO_QUARTER_PHASE_3Y"] = quarter.iloc[:, 3:6].T.mean()
    quarter.to_excel(writer, sheet_name="QUARTERLY")


    # **********************************************************
    month = pd.pivot_table(
        data=df_temp.groupby(["YEAR", "MONTH"])["MEAN_RATIO_MONTHLY"]
        .unique()
        .to_frame()
        .reset_index()
        .explode("MEAN_RATIO_MONTHLY"),
        values="MEAN_RATIO_MONTHLY",
        index="MONTH",
        columns="YEAR",
    ).reset_index()

    month.replace([np.inf, -np.inf], 0, inplace=True)
    month.fillna(0, inplace=True)

    month["MEAN_RATIO_MONTH_PHASE_ALL"] = month.drop("MONTH", axis=1).T.mean()
    month["MEAN_RATIO_MONTH_PHASE_1Y"] = month.iloc[:, 5]
    month["MEAN_RATIO_MONTH_PHASE_2Y"] = month.iloc[:, 4:6].T.mean()
    month["MEAN_RATIO_MONTH_PHASE_3Y"] = month.iloc[:, 3:6].T.mean()

    month.to_excel(writer, sheet_name="MONTHLY")


    # **********************************************************
    df_temp["WEEK"] = df_temp["YEARWEEK"] % 100
    week = pd.pivot_table(
        data=df_temp.groupby(["YEAR", "WEEK"])["MEAN_RATIO_WEEKLY"]
        .unique()
        .to_frame()
        .reset_index()
        .explode("MEAN_RATIO_WEEKLY"),
        values="MEAN_RATIO_WEEKLY",
        index="WEEK",
        columns="YEAR",
    ).reset_index()

    week.replace([np.inf, -np.inf], 0, inplace=True)
    week.fillna(0, inplace=True)

    week["MEAN_RATIO_WEEK_PHASE_ALL"] = week.drop("WEEK", axis=1).T.mean()
    week["MEAN_RATIO_WEEK_PHASE_1Y"] = week.iloc[:, 5]
    week["MEAN_RATIO_WEEK_PHASE_2Y"] = week.iloc[:, 4:6].T.mean()
    week["MEAN_RATIO_WEEK_PHASE_3Y"] = week.iloc[:, 3:6].T.mean()

    week.to_excel(writer, sheet_name="WEEKLY")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Weekly Sale

# COMMAND ----------

top_dp_largest = (
    df_temp.drop_duplicates(subset=["TOTAL_SEC_SALES"])
    .nlargest(20, columns=["TOTAL_SEC_SALES"])["DPNAME"]
    .unique()
)

df_top = df_consist_dp[df_consist_dp["DPNAME"].isin(top_dp_largest)]

df_top["MEAN_RATIO_WEEKLY"] = (
    df_top.groupby(["YEAR", "MONTH", "DATE"])["RATIO_WEEKLY"]
    .transform("mean")
    .astype(float)
)

df_top = df_top.sort_values("DATE").reset_index(drop=True)

print(
    "MEAN RATIO WEEKLY 20DP have largest total sales after first Quarter 2020:",
    df_top["MEAN_RATIO_WEEKLY"][df_top["YEARWEEK"] > 202013].dropna().unique().mean(),
)

fig = px.line(data_frame=df_top, x="DATE", y="MEAN_RATIO_WEEKLY")
fig.update_layout(title="MEAN RATIO WEEKLY 20DP have largest total sales")
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Monthly Sale

# COMMAND ----------

top_dp_largest = (
    df_temp.drop_duplicates(subset=["TOTAL_SEC_SALES"])
    .nlargest(20, columns=["TOTAL_SEC_SALES"])["DPNAME"]
    .unique()
)

df_top = df_consist_dp[df_consist_dp["DPNAME"].isin(top_dp_largest)]

df_top["MEAN_RATIO_MONTHLY"] = (
    df_top.groupby(["YEAR", "MONTH"])["RATIO_MONTHLY"].transform("mean").astype(float)
)

df_top = df_top.sort_values("DATE").reset_index(drop=True)

print(
    "MEAN RATIO MONTHLY 20DP have largest total sales after first Quarter 2020:",
    df_top["MEAN_RATIO_MONTHLY"][df_top["YEARWEEK"] > 202013].dropna().unique().mean(),
)

fig = px.line(data_frame=df_top, x="DATE", y="MEAN_RATIO_MONTHLY")
fig.update_layout(title="MEAN RATIO MONTHLY 20DP have largest total sales")
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Quarterly Sale

# COMMAND ----------

top_dp_largest = (
    df_temp.drop_duplicates(subset=["TOTAL_SEC_SALES"])
    .nlargest(20, columns=["TOTAL_SEC_SALES"])["DPNAME"]
    .unique()
)

df_top = df_consist_dp[df_consist_dp["DPNAME"].isin(top_dp_largest)]

df_top["MEAN_RATIO_QUARTERLY"] = (
    df_top.groupby(["YEAR", "QUARTER"])["RATIO_QUARTERLY"]
    .transform("mean")
    .astype(float)
)

df_top = df_top.sort_values("DATE").reset_index(drop=True)

print(
    "MEAN RATIO QUARTERLY 20DP have largest total sales after first Quarter 2020:",
    df_top["MEAN_RATIO_QUARTERLY"][df_top["YEARWEEK"] > 202013]
    .dropna()
    .unique()
    .mean(),
)

fig = px.line(data_frame=df_top, x="DATE", y="MEAN_RATIO_QUARTERLY")
fig.update_layout(title="MEAN RATIO QUARTERLY 20DP have largest total sales")
fig.show()

# COMMAND ----------

df_top["MEAN_RATIO_QUARTERLY"][df_top["YEARWEEK"] >202013].dropna().unique().mean()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Mean Ratio TOP 100DP largest total sale follow percent cate

# COMMAND ----------

list_cate = (df_consist_dp["CATEGORY"].value_counts(normalize=True) * 100).apply(
    lambda x: math.ceil(x)
)
print(list_cate)

fig, ax = plt.subplots(figsize = (16, 8))
ax.pie(
    df_consist_dp["CATEGORY"].value_counts(),
    labels = df_consist_dp["CATEGORY"].value_counts().index,
    autopct = "%.1f%%"
)
ax.set_title('Percentage Category of Data have consistence DP')
plt.tight_layout()
plt.show()

# COMMAND ----------

list_cate = (df_consist_dp["CATEGORY"].value_counts(normalize=True)[:5] * 100).apply(
    lambda x: math.ceil(x)
)

with pd.ExcelWriter(
    "/Workspace/Users/ng-minh-hoang.dat@unilever.com/Mean_RATIO_percent_category.xlsx"
) as writer:
    df_cate = pd.DataFrame(columns=df_consist_dp.columns)

    for i in range(len(list_cate)):
        # pick pattern following category
        df_temp = df_consist_dp[df_consist_dp["CATEGORY"] == list_cate.index[i]]

        # pick pattern following largest DP with len = percentage of category in data
        top_dp_largest = (
            df_temp.drop_duplicates(subset=["TOTAL_SEC_SALES"])
            .nlargest(list_cate[i], columns=["TOTAL_SEC_SALES"])["DPNAME"]
            .unique()
        )

        df_concat = df_temp[df_temp["DPNAME"].isin(top_dp_largest)]
        df_cate = pd.concat([df_cate, df_concat])
    df_cate = df_cate[df_cate["YEARWEEK"] < 202301]

    df_cate["MEAN_RATIO_WEEKLY"] = (
        df_cate.groupby(["YEAR", "MONTH", "YEARWEEK"])["RATIO_WEEKLY"]
        .transform("mean")
        .astype(float)
    )
    df_cate["MEAN_RATIO_MONTHLY"] = (
        df_cate.groupby(["YEAR", "MONTH"])["RATIO_MONTHLY"]
        .transform("mean")
        .astype(float)
    )
    df_cate["MEAN_RATIO_QUARTERLY"] = (
        df_cate.groupby(["YEAR", "QUARTER"])["RATIO_QUARTERLY"]
        .transform("mean")
        .astype(float)
    )
    df_cate = df_cate.sort_values("DATE").reset_index(drop=True)


    # **********************************************************
    quarter = pd.pivot_table(
        data=df_cate.groupby(["YEAR", "QUARTER"])["MEAN_RATIO_QUARTERLY"]
        .unique()
        .to_frame()
        .reset_index()
        .explode("MEAN_RATIO_QUARTERLY"),
        values="MEAN_RATIO_QUARTERLY",
        index="QUARTER",
        columns="YEAR",
    ).reset_index()

    quarter.replace([np.inf, -np.inf], 0, inplace=True)
    quarter.fillna(0, inplace=True)

    quarter["MEAN_RATIO_QUARTER_PHASE_ALL"] = quarter.drop("QUARTER", axis=1).T.mean()
    quarter["MEAN_RATIO_QUARTER_PHASE_1Y"] = quarter.iloc[:, 5]
    quarter["MEAN_RATIO_QUARTER_PHASE_2Y"] = quarter.iloc[:, 4:6].T.mean()
    quarter["MEAN_RATIO_QUARTER_PHASE_3Y"] = quarter.iloc[:, 3:6].T.mean()
    quarter.to_excel(writer, sheet_name="QUARTERLY")


    # **********************************************************
    month = pd.pivot_table(
        data=df_cate.groupby(["YEAR", "MONTH"])["MEAN_RATIO_MONTHLY"]
        .unique()
        .to_frame()
        .reset_index()
        .explode("MEAN_RATIO_MONTHLY"),
        values="MEAN_RATIO_MONTHLY",
        index="MONTH",
        columns="YEAR",
    ).reset_index()

    month.replace([np.inf, -np.inf], 0, inplace=True)
    month.fillna(0, inplace=True)

    month["MEAN_RATIO_MONTH_PHASE_ALL"] = month.drop("MONTH", axis=1).T.mean()
    month["MEAN_RATIO_MONTH_PHASE_1Y"] = month.iloc[:, 5]
    month["MEAN_RATIO_MONTH_PHASE_2Y"] = month.iloc[:, 4:6].T.mean()
    month["MEAN_RATIO_MONTH_PHASE_3Y"] = month.iloc[:, 3:6].T.mean()

    month.to_excel(writer, sheet_name="MONTHLY")


    # **********************************************************
    df_cate["WEEK"] = df_cate["YEARWEEK"] % 100
    week = pd.pivot_table(
        data=df_cate.groupby(["YEAR", "WEEK"])["MEAN_RATIO_WEEKLY"]
        .unique()
        .to_frame()
        .reset_index()
        .explode("MEAN_RATIO_WEEKLY"),
        values="MEAN_RATIO_WEEKLY",
        index="WEEK",
        columns="YEAR",
    ).reset_index()

    week.replace([np.inf, -np.inf], 0, inplace=True)
    week.fillna(0, inplace=True)

    week["MEAN_RATIO_WEEK_PHASE_ALL"] = week.drop("WEEK", axis=1).T.mean()
    week["MEAN_RATIO_WEEK_PHASE_1Y"] = week.iloc[:, 5]
    week["MEAN_RATIO_WEEK_PHASE_2Y"] = week.iloc[:, 4:6].T.mean()
    week["MEAN_RATIO_WEEK_PHASE_3Y"] = week.iloc[:, 3:6].T.mean()

    week.to_excel(writer, sheet_name="WEEKLY")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Weekly Sale

# COMMAND ----------

df_cate = pd.DataFrame(columns=df_consist_dp.columns)

for i in range(len(list_cate)):
    # pick pattern following category
    df_temp = df_consist_dp[df_consist_dp["CATEGORY"] == list_cate.index[i]]

    # pick pattern following largest DP with len = percentage of category in data
    top_dp_largest = (
        df_temp.drop_duplicates(subset=["TOTAL_SEC_SALES"])
        .nlargest(list_cate[i], columns=["TOTAL_SEC_SALES"])["DPNAME"]
        .unique()
    )

    df_concat = df_temp[df_temp["DPNAME"].isin(top_dp_largest)]

    df_cate = pd.concat([df_cate, df_concat])


df_cate = df_cate.sort_values("DATE").reset_index(drop=True)
df_cate["MEAN_RATIO_WEEKLY"] = (
    df_cate.groupby(["YEAR", "MONTH", "DATE"])["RATIO_WEEKLY"]
    .transform("mean")
    .astype(float)
)

df_cate["MEAN_RATIO_WEEKLY"].replace([np.inf, -np.inf], 0, inplace=True)

print(
    "MEAN RATIO WEEKLY 100 LARGEST DP picked by percentage of CATEGORY after first Quarter 2020:",
    df_cate["MEAN_RATIO_WEEKLY"][df_cate["YEARWEEK"] > 202013].dropna().unique().mean(),
)

fig = px.line(data_frame=df_cate, x="DATE", y="MEAN_RATIO_WEEKLY")
fig.update_layout(
    title="MEAN RATIO WEEKLY 100 LARGEST DP picked by percentage of CATEGORY"
)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Monthly Sale

# COMMAND ----------

df_cate = pd.DataFrame(columns=df_consist_dp.columns)

for i in range(len(list_cate)):
    # pick pattern following category
    df_temp = df_consist_dp[df_consist_dp["CATEGORY"] == list_cate.index[i]]

    # pick pattern following largest DP with len = percentage of category in data
    top_dp_largest = (
        df_temp.drop_duplicates(subset=["TOTAL_SEC_SALES"])
        .nlargest(list_cate[i], columns=["TOTAL_SEC_SALES"])["DPNAME"]
        .unique()
    )

    df_concat = df_temp[df_temp["DPNAME"].isin(top_dp_largest)]

    df_cate = pd.concat([df_cate, df_concat])


df_cate = df_cate.sort_values("DATE").reset_index(drop=True)
df_cate["MEAN_RATIO_MONTHLY"] = (
    df_cate.groupby(["YEAR", "MONTH"])["RATIO_MONTHLY"].transform("mean").astype(float)
)

df_cate["MEAN_RATIO_MONTHLY"].replace([np.inf, -np.inf], 0, inplace=True)

print(
    "MEAN RATIO MONTHLY 100 LARGEST DP picked by percentage of CATEGORY after first Quarter 2020:",
    df_cate["MEAN_RATIO_MONTHLY"][df_cate["YEARWEEK"] > 202013].dropna().unique().mean(),
)

fig = px.line(data_frame=df_cate, x="DATE", y="MEAN_RATIO_MONTHLY")
fig.update_layout(
    title="MEAN RATIO MONTHLY 100 LARGEST DP picked by percentage of CATEGORY"
)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Quarterly Sale

# COMMAND ----------

df_cate = pd.DataFrame(columns=df_consist_dp.columns)

for i in range(len(list_cate)):
    # pick pattern following category
    df_temp = df_consist_dp[df_consist_dp["CATEGORY"] == list_cate.index[i]]

    # pick pattern following largest DP with len = percentage of category in data
    top_dp_largest = (
        df_temp.drop_duplicates(subset=["TOTAL_SEC_SALES"])
        .nlargest(list_cate[i], columns=["TOTAL_SEC_SALES"])["DPNAME"]
        .unique()
    )

    df_concat = df_temp[df_temp["DPNAME"].isin(top_dp_largest)]

    df_cate = pd.concat([df_cate, df_concat])


df_cate = df_cate.sort_values("DATE").reset_index(drop=True)
df_cate["MEAN_RATIO_QUARTERLY"] = (
    df_cate.groupby(["YEAR", "QUARTER"])["RATIO_QUARTERLY"]
    .transform("mean")
    .astype(float)
)

print(
    "MEAN RATIO QUARTERLY 100 LARGEST DP picked by percentage of CATEGORY after first Quarter 2020:",
    df_cate["MEAN_RATIO_QUARTERLY"][df_cate["YEARWEEK"] > 202013]
    .dropna()
    .unique()
    .mean(),
)

fig = px.line(data_frame=df_cate, x="DATE", y="MEAN_RATIO_QUARTERLY")
fig.update_layout(
    title="MEAN RATIO QUARTERLY 100 LARGEST DP picked by percentage of CATEGORY"
)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Mean Ratio TOP 10DP largest total sale each category

# COMMAND ----------

list_cate = df_consist_dp["CATEGORY"].value_counts().index
for i in list_cate:
    print(i,"- have",df_consist_dp[df_consist_dp["CATEGORY"] == i]["DPNAME"].nunique())

# COMMAND ----------

list_cate = df_consist_dp["CATEGORY"].value_counts().index

# with pd.ExcelWriter(
#     "/Workspace/Users/ng-minh-hoang.dat@unilever.com/Mean_RATIO_each_category.xlsx"
# ) as writer:
    for i in list_cate:
        df_temp = df_consist_dp[df_consist_dp["CATEGORY"] == i]
        df_temp = df_temp[df_temp["YEARWEEK"] < 202301]

        top10_dp_largest = (
            df_temp.drop_duplicates(subset=["TOTAL_SEC_SALES"])
            .nlargest(10, columns=["TOTAL_SEC_SALES"])["DPNAME"]
            .unique()
        )
        df_top10 = df_temp[df_temp["DPNAME"].isin(top10_dp_largest)]

        df_top10["MEAN_RATIO_MONTHLY"] = (
            df_top10.groupby(["YEAR", "MONTH"])["RATIO_MONTHLY"]
            .transform("mean")
            .astype(float)
        )
        df_top10["MEAN_RATIO_QUARTERLY"] = (
            df_top10.groupby(["YEAR", "QUARTER"])["RATIO_QUARTERLY"]
            .transform("mean")
            .astype(float)
        )
        df_top10 = df_top10.sort_values("DATE").reset_index(drop=True)

        print(
            "MEAN RATIO QUARTERLY of " + i + " after first Quarter 2020:",
            df_top10["MEAN_RATIO_QUARTERLY"][df_top10["YEARWEEK"] > 202013]
            .dropna()
            .unique()
            .mean(),
        )

        print(
            "MEAN RATIO MONTHLY   of " + i + " after first Quarter 2020:",
            df_top10["MEAN_RATIO_MONTHLY"][df_top10["YEARWEEK"] > 202013]
            .dropna()
            .unique()
            .mean(),
        )
        quarter = pd.pivot_table(
            data=df_top10.groupby(["YEAR", "QUARTER"])["MEAN_RATIO_QUARTERLY"]
            .unique()
            .to_frame()
            .reset_index()
            .explode("MEAN_RATIO_QUARTERLY"),
            values="MEAN_RATIO_QUARTERLY",
            index="QUARTER",
            columns="YEAR",
        ).reset_index()

        quarter["MEAN_RATIO_QUARTER_PHASE_ALL"] = quarter.drop(
            "QUARTER", axis=1
        ).T.mean()
        quarter["MEAN_RATIO_QUARTER_PHASE_1Y"] = quarter.iloc[:, 5]
        quarter["MEAN_RATIO_QUARTER_PHASE_2Y"] = quarter.iloc[:, 4:6].T.mean()
        quarter["MEAN_RATIO_QUARTER_PHASE_3Y"] = quarter.iloc[:, 3:6].T.mean()
        # quarter.to_excel(writer, sheet_name=i, startrow=0, index=False)

        month = pd.pivot_table(
            data=df_top10.groupby(["YEAR", "MONTH"])["MEAN_RATIO_MONTHLY"]
            .unique()
            .to_frame()
            .reset_index()
            .explode("MEAN_RATIO_MONTHLY"),
            values="MEAN_RATIO_MONTHLY",
            index="MONTH",
            columns="YEAR",
        ).reset_index()

        month["MEAN_RATIO_MONTH_PHASE_ALL"] = month.drop("MONTH", axis=1).T.mean()
        month["MEAN_RATIO_MONTH_PHASE_1Y"] = month.iloc[:, 5]
        month["MEAN_RATIO_MONTH_PHASE_2Y"] = month.iloc[:, 4:6].T.mean()
        month["MEAN_RATIO_MONTH_PHASE_3Y"] = month.iloc[:, 3:6].T.mean()

        # month.to_excel(writer, sheet_name=i, startrow=7, index=False)

        fig = px.line(
            data_frame=df_top10,
            x="DATE",
            y=["MEAN_RATIO_MONTHLY", "MEAN_RATIO_QUARTERLY"],
        )
        fig.update_layout(title="MEAN RATIO MONTHLY & QUARTERLY 10 LARGEST DP of " + i)
        fig.show()

# COMMAND ----------

for i in list_cate:
    df_temp = df_consist_dp[df_consist_dp['CATEGORY'] == i]
    first_5dp = (
            df_temp.drop_duplicates(subset=["TOTAL_SEC_SALES"])
            .nlargest(5, columns=["TOTAL_SEC_SALES"])["DPNAME"]
            .unique()
    )
    temp_1 = df_temp[df_temp['DPNAME'] == first_5dp[0]]
    temp_2 = df_temp[df_temp['DPNAME'] == first_5dp[1]]
    temp_3 = df_temp[df_temp['DPNAME'] == first_5dp[2]]
    temp_4 = df_temp[df_temp['DPNAME'] == first_5dp[3]]
    temp_5 = df_temp[df_temp['DPNAME'] == first_5dp[4]]

    fig = go.Figure(
        data = [
            go.Scatter(x = temp_1['DATE'], y = temp_1['RATIO_QUARTERLY']),
            go.Scatter(x = temp_2['DATE'], y = temp_2['RATIO_QUARTERLY']),
            go.Scatter(x = temp_3['DATE'], y = temp_3['RATIO_QUARTERLY']),
            go.Scatter(x = temp_4['DATE'], y = temp_4['RATIO_QUARTERLY']),
            go.Scatter(x = temp_5['DATE'], y = temp_5['RATIO_QUARTERLY'])

        ]
    )
    fig.update_layout(title='RATIO QUARTERLY 5 LARGEST DP of ' + i)
    fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Data inconsistence DP

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Primary Sales

# COMMAND ----------

df_pri_outside_dp = df_pri_sales[
    ~df_pri_sales["DPNAME"].isin(df_consist_dp["DPNAME"].unique())
]
print(df_pri_outside_dp.shape, df_pri_sales.shape)
df_pri_outside_dp.shape[0] / df_pri_sales.shape[0] * 100

# COMMAND ----------

df_less_4w = df_pri_outside_dp[df_pri_outside_dp['COUNT WEEKS'] <= 3]
df_less_4w = df_less_4w.sort_values(['DPNAME','DATE']).reset_index(drop = True)
print(df_less_4w.shape, df_pri_outside_dp.shape)
df_less_4w.shape[0]/df_pri_outside_dp.shape[0]*100

# COMMAND ----------

fig, ax = plt.subplots(1, 2, figsize = (24, 12))
plt.rcParams['font.size'] = 20

ax[0].pie(
    df_pri_sales["CATEGORY"].value_counts(),
    labels=df_pri_sales["CATEGORY"].value_counts().index,
    autopct="%.2f%%",
)
ax[0].set_title('Percentage Category of Whole Primary Sale')

ax[1].pie(
    df_pri_outside_dp["CATEGORY"].value_counts(),
    labels=df_pri_outside_dp["CATEGORY"].value_counts().index,
    autopct="%.2f%%",
)
ax[1].set_title('Percentage Category of unconsistence DP in Primary Sale')
plt.tight_layout()
plt.show()

# COMMAND ----------

list_100_dp = df_pri_outside_dp["DPNAME"].value_counts()
list_100_dp = list_100_dp[list_100_dp > 100]
list_100_dp.nunique()

# COMMAND ----------

df_pri_outside_dp_more100 = df_pri_outside_dp[df_pri_outside_dp["DPNAME"].isin(list_100_dp.index)]
df_pri_outside_dp_less100 = df_pri_outside_dp[~df_pri_outside_dp["DPNAME"].isin(list_100_dp.index)]

print(df_pri_outside_dp_more100.shape, df_pri_outside_dp_less100.shape)

# COMMAND ----------

fig, ax = plt.subplots(1, 3, figsize = (40, 20))

ax[0].pie(
    df_pri_sales["CATEGORY"].value_counts(),
    labels=df_pri_sales["CATEGORY"].value_counts().index,
    autopct="%.0f%%",
)
ax[0].set_title('Percentage Category of Primary Sale')

ax[1].pie(
    df_pri_outside_dp_more100["CATEGORY"].value_counts(),
    labels=df_pri_outside_dp_more100["CATEGORY"].value_counts().index,
    autopct="%.0f%%",
)
ax[1].set_title('Percentage Category unconsistence DP > 100 Sales')

ax[2].pie(
    df_pri_outside_dp_less100["CATEGORY"].value_counts(),
    labels=df_pri_outside_dp_less100["CATEGORY"].value_counts().index,
    autopct="%.0f%%",
)
ax[2].set_title('Percentage Category unconsistence DP < 100 Sales')

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Secondary Sales

# COMMAND ----------

df_sec_outside_dp = df_sec_sales[
    ~df_sec_sales["DPNAME"].isin(df_consist_dp["DPNAME"].unique())
]
print(df_sec_outside_dp.shape, df_pri_sales.shape)
df_sec_outside_dp.shape[0] / df_pri_sales.shape[0] * 100

# COMMAND ----------

df_sec_outside_dp['DPNAME'].nunique(), df_sec_outside_dp['CATEGORY'].unique()

# COMMAND ----------

fig, ax = plt.subplots(1, 2, figsize = (24, 12))
plt.rcParams['font.size'] = 20

ax[0].pie(
    df_sec_sales["CATEGORY"].value_counts(),
    labels=df_sec_sales["CATEGORY"].value_counts().index,
    autopct="%.2f%%",
)
ax[0].set_title('Percentage Category of Whole Secondary Sale')

ax[1].pie(
    df_sec_outside_dp["CATEGORY"].value_counts(),
    labels=df_sec_outside_dp["CATEGORY"].value_counts().index,
    autopct="%.2f%%",
)
ax[1].set_title('Percentage Category of unconsistence DP in Secondary Sale')
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Check Data count Week <= 3

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Primary Sales

# COMMAND ----------

df_less_4w = df_pri_sales[df_pri_sales['COUNT WEEKS'] <= 3]
df_less_4w = df_less_4w.sort_values(['DPNAME','DATE']).reset_index(drop = True)
print(df_less_4w.shape, df_pri_sales.shape)
df_less_4w.shape[0]/df_pri_sales.shape[0]*100

# COMMAND ----------

fig, ax = plt.subplots(1, 2, figsize = (24, 12))
plt.rcParams['font.size'] = 20

ax[0].pie(
    df_pri_sales["COUNT WEEKS"].value_counts(),
    labels=df_pri_sales["COUNT WEEKS"].value_counts().index,
    autopct="%.2f%%",
)
ax[0].set_title('Percentage Week count of Whole Primary Sale Data')

ax[1].pie(
    df_less_4w["COUNT WEEKS"].value_counts(),
    labels=df_less_4w["COUNT WEEKS"].value_counts().index,
    autopct="%.2f%%",
)
ax[1].set_title('Percentage Week count of Primary Sale Data have less than 4 weeks')
plt.tight_layout()
plt.show()

# COMMAND ----------

df_less_4w[['PCS','TON','CS','ACTUALSALE','DTWORKINGDAY']].describe()

# COMMAND ----------

df_less_4w_1 = df_less_4w[df_less_4w['DPNAME'] == "DOVEGEL SOFTENING900G"]
df_less_4w_2 = df_less_4w[df_less_4w['DPNAME'] == "WALL'S SELECTION COOKIES&CRM 240G"]
df_less_4w_3 = df_less_4w[df_less_4w['DPNAME'] == "LIFEBUOYHAND WASH KITCHENFRES450G"]

fig = go.Figure(
    data = [
        go.Scatter(x = df_less_4w_1['DATE'], y = df_less_4w_1['ACTUALSALE']),
        go.Scatter(x = df_less_4w_2['DATE'], y = df_less_4w_2['ACTUALSALE']),
        go.Scatter(x = df_less_4w_3['DATE'], y = df_less_4w_3['ACTUALSALE']),
    ]
)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Secondary Sales

# COMMAND ----------

df_sec_sales["COUNT WEEKS"] = (
    df_sec_sales.groupby(["KEY", "YEAR", "MONTH"])["YEARWEEK"]
    .transform("count")
    .astype(int)
)

df_sec_sales["ORDER WEEKS"] = (
    df_sec_sales.groupby(["KEY", "YEAR", "MONTH"])["YEARWEEK"]
    .transform("rank")
    .astype(int)
)

# COMMAND ----------

df_less_4w = df_sec_sales[df_sec_sales['COUNT WEEKS'] <= 3]
df_less_4w = df_less_4w.sort_values(['DPNAME','DATE']).reset_index(drop = True)
print(df_less_4w.shape, df_sec_sales.shape)
df_less_4w.shape[0]/df_sec_sales.shape[0]*100

# COMMAND ----------

fig, ax = plt.subplots(1, 2, figsize = (24, 12))
plt.rcParams['font.size'] = 20

ax[0].pie(
    df_sec_sales["COUNT WEEKS"].value_counts(),
    labels=df_sec_sales["COUNT WEEKS"].value_counts().index,
    autopct="%.2f%%",
)
ax[0].set_title('Percentage Week count of Whole Sec Sale Data')

ax[1].pie(
    df_less_4w["COUNT WEEKS"].value_counts(),
    labels=df_less_4w["COUNT WEEKS"].value_counts().index,
    autopct="%.2f%%",
)
ax[1].set_title('Percentage Week count of Sec Sale Data have less than 4 weeks')
plt.tight_layout()
plt.show()

# COMMAND ----------

df_less_4w[['ACTUALSALE','DTWORKINGDAY']].describe()
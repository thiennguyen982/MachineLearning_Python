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
# MAGIC # DEO

# COMMAND ----------

# old data
old_path = "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/OUTPUT_FORECAST/FC_BASELINE_SEC2PRI_HIS/20231010/DEO.xlsx"

df_old = pd.read_excel(old_path)

# COMMAND ----------

df_old.describe()

# COMMAND ----------

print(df_old.shape)
print(df_old["DPNAME"].nunique())
df_old.head(2)

# COMMAND ----------

list_dpname = [
    "REX SHOWER CLEAN 40ML/24",
    "AXE DEO BODYSPRAY GOLD TMPTTN 12X150ML",
]

# COMMAND ----------

# new data 
new_path = "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/OUTPUT_FORECAST/FC_BASELINE_SEC2PRI_HIS/20231011/DEO.xlsx"

df_new = pd.read_excel(new_path)

# COMMAND ----------

df_new.describe()

# COMMAND ----------

print(df_new.shape)
print(df_new["KEY"].nunique())
df_new.head(2)

# COMMAND ----------

display(df_new[(df_new["DPNAME"].isin(list_dpname)) & (df_new["YEARWEEK"] >= 202341)])

# COMMAND ----------

temp_df_old = df_old[df_old["DPNAME"] == "AXE DEO BODYSPRAY GOLD TMPTTN 12X150ML"]
temp_df_old["DATE"] = pd.to_datetime(
    (temp_df_old["YEARWEEK"]).astype(str) + "-1", format="%G%V-%w"
)
temp_df_new = df_new[df_new["DPNAME"] == "AXE DEO BODYSPRAY GOLD TMPTTN 12X150ML"]
fig = go.Figure(
    data=[
        go.Scatter(
            x=temp_df_old["DATE"],
            y=temp_df_old["FC_PRI_BASELINE_WEEKLY"],
            name="old PRI",
        ),
        go.Scatter(
            x=temp_df_new["DATE"],
            y=temp_df_new["FC_PRI_BASELINE_WEEKLY"],
            name="new PRI",
        ),
        go.Scatter(
            x=temp_df_new["DATE"],
            y=temp_df_new["PROPOSED_FC_WEEKLY"],
            name="FC BASELINE SEC",
            marker={"color": "darkgreen"},
        ),
        # go.Scatter(x = temp_df_new["DATE"], y = temp_df_new["SEC_SALES"], name = "ACTUAL SEC")
    ]
)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # FABSEN

# COMMAND ----------

# old data
old_path = "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/OUTPUT_FORECAST/FC_BASELINE_SEC2PRI_HIS/20231010/FABSEN.xlsx"

df_old = pd.read_excel(old_path)

# COMMAND ----------

print(df_old.shape)
print(df_old["DPNAME"].nunique())
df_old.head(2)

# COMMAND ----------

list_dpname = [
    "CF. INTENSE CARE SOFIA 20ML",
    "CF CONC. WHITE 800ML",
]

# COMMAND ----------

# new data 
new_path = "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/OUTPUT_FORECAST/FC_BASELINE_SEC2PRI_HIS/20231011/FABSEN.xlsx"

df_new = pd.read_excel(new_path)

# COMMAND ----------

df_old.describe()

# COMMAND ----------

df_new.describe()

# COMMAND ----------

print(df_new.shape)
print(df_new["KEY"].nunique())
df_new.head(2)

# COMMAND ----------

display(df_new[(df_new["DPNAME"].isin(list_dpname)) & (df_new["YEARWEEK"] >= 202341)])

# COMMAND ----------

temp_df_old = df_old[df_old["DPNAME"] == "CF. INTENSE CARE SOFIA 20ML"]
temp_df_old["DATE"] = pd.to_datetime(temp_df_old["YEARWEEK"].astype(str) + "-1", format = "%G%V-%w")
temp_df_new = df_new[df_new["DPNAME"] == "CF. INTENSE CARE SOFIA 20ML"]
fig = go.Figure(
    data=[
        go.Scatter(
            x=temp_df_old["DATE"],
            y=temp_df_old["FC_PRI_BASELINE_WEEKLY"],
            name="old PRI",
        ),
        go.Scatter(
            x=temp_df_new["DATE"],
            y=temp_df_new["FC_PRI_BASELINE_WEEKLY"],
            name="new PRI",
        ),
        go.Scatter(
            x=temp_df_new["DATE"],
            y=temp_df_new["PROPOSED_FC_WEEKLY"],
            name="FC BASELINE SEC",
            marker={"color": "darkgreen"},
        ),
        # go.Scatter(x = temp_df_new["DATE"], y = temp_df_new["SEC_SALES"], name = "ACTUAL SEC")
    ]
)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # FABSOL

# COMMAND ----------

# old data
old_path = "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/OUTPUT_FORECAST/FC_BASELINE_SEC2PRI_HIS/20231010/FABSOL.xlsx"

df_old = pd.read_excel(old_path)

# COMMAND ----------

print(df_old.shape)
print(df_old["DPNAME"].nunique())
df_old.head(2)

# COMMAND ----------

list_dpname = [
    "OMO RED PRO 9000 GR",
    "OMO PINK 5500 GR",
    "COMFORT ELEGANT POUCH 3.6KG",
    "OMO LIQUID MATIC TLL CFT SS (POU) 2KG",
]

# COMMAND ----------

# new data 
new_path = "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/OUTPUT_FORECAST/FC_BASELINE_SEC2PRI_HIS/20231011/FABSOL.xlsx"

df_new = pd.read_excel(new_path)

# COMMAND ----------

df_old.describe()

# COMMAND ----------

df_new.describe()

# COMMAND ----------

print(df_new.shape)
print(df_new["KEY"].nunique())
df_new.head(2)

# COMMAND ----------

display(df_new[(df_new["DPNAME"].isin(list_dpname)) & (df_new["YEARWEEK"] >= 202341)])

# COMMAND ----------

temp_df_old = df_old[df_old["DPNAME"] == "COMFORT ELEGANT POUCH 3.6KG"]
temp_df_old["DATE"] = pd.to_datetime(temp_df_old["YEARWEEK"].astype(str) + "-1", format = "%G%V-%w")
temp_df_new = df_new[df_new["DPNAME"] == "COMFORT ELEGANT POUCH 3.6KG"]
fig = go.Figure(
    data=[
        go.Scatter(
            x=temp_df_old["DATE"],
            y=temp_df_old["FC_PRI_BASELINE_WEEKLY"],
            name="old PRI",
        ),
        go.Scatter(
            x=temp_df_new["DATE"],
            y=temp_df_new["FC_PRI_BASELINE_WEEKLY"],
            name="new PRI",
        ),
        go.Scatter(
            x=temp_df_new["DATE"],
            y=temp_df_new["PROPOSED_FC_WEEKLY"],
            name="FC BASELINE SEC",
            marker={"color": "darkgreen"},
        ),
        # go.Scatter(x = temp_df_new["DATE"], y = temp_df_new["SEC_SALES"], name = "ACTUAL SEC")
    ]
)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # HAIR

# COMMAND ----------

# old data
old_path = "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/OUTPUT_FORECAST/FC_BASELINE_SEC2PRI_HIS/20231010/HAIR.xlsx"

df_old = pd.read_excel(old_path)

# COMMAND ----------

print(df_old.shape)
print(df_old["DPNAME"].nunique())
df_old.head(2)

# COMMAND ----------

list_dpname = [
    "DOVE CONDITIONER INTENSIVE REPAIR 6G",
    "TRESEMME SHAMPOO KERATIN SMOOTH 650G",
    "DOVE SHAMPOO INTENSIVE REPAIR 6G",
]

# COMMAND ----------

# new data 
new_path = "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/OUTPUT_FORECAST/FC_BASELINE_SEC2PRI_HIS/20231011/HAIR.xlsx"

df_new = pd.read_excel(new_path)

# COMMAND ----------

df_old.describe()

# COMMAND ----------

df_new.describe()

# COMMAND ----------

print(df_new.shape)
print(df_new["KEY"].nunique())
df_new.head(2)

# COMMAND ----------

temp_df_old = df_old[df_old["DPNAME"] == "DOVE CONDITIONER INTENSIVE REPAIR 6G"]
temp_df_old["DATE"] = pd.to_datetime(temp_df_old["YEARWEEK"].astype(str) + "-1", format = "%G%V-%w")
temp_df_new = df_new[df_new["DPNAME"] == "DOVE CONDITIONER INTENSIVE REPAIR 6G"]
fig = go.Figure(
    data=[
        go.Scatter(
            x=temp_df_old["DATE"],
            y=temp_df_old["FC_PRI_BASELINE_WEEKLY"],
            name="old PRI",
        ),
        go.Scatter(
            x=temp_df_new["DATE"],
            y=temp_df_new["FC_PRI_BASELINE_WEEKLY"],
            name="new PRI",
        ),
        go.Scatter(
            x=temp_df_new["DATE"],
            y=temp_df_new["PROPOSED_FC_WEEKLY"],
            name="FC BASELINE SEC",
            marker={"color": "darkgreen"},
        ),
        # go.Scatter(x = temp_df_new["DATE"], y = temp_df_new["SEC_SALES"], name = "ACTUAL SEC")
    ]
)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # HNH

# COMMAND ----------

# old data
old_path = "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/OUTPUT_FORECAST/FC_BASELINE_SEC2PRI_HIS/20231010/HNH.xlsx"

df_old = pd.read_excel(old_path)

# COMMAND ----------

print(df_old.shape)
print(df_old["DPNAME"].nunique())
df_old.head(2)

# COMMAND ----------

list_dpname = [
    "SUNLIGHT LEMON 3600G",
    "DELISTED HNH",
    "VIM WC BLUE 880ML (BOM BAY)",
]

# COMMAND ----------

# new data 
new_path = "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/OUTPUT_FORECAST/FC_BASELINE_SEC2PRI_HIS/20231011/HNH.xlsx"

df_new = pd.read_excel(new_path)

# COMMAND ----------

df_old.describe()

# COMMAND ----------

df_new.describe()

# COMMAND ----------

print(df_new.shape)
print(df_new["KEY"].nunique())
df_new.head(2)

# COMMAND ----------

display(df_new[(df_new["DPNAME"].isin(list_dpname)) & (df_new["YEARWEEK"] >= 202341)])

# COMMAND ----------

list_dpname

# COMMAND ----------

temp_df_old = df_old[df_old["DPNAME"] == "VIM WC BLUE 880ML (BOM BAY)"]
temp_df_old["DATE"] = pd.to_datetime(temp_df_old["YEARWEEK"].astype(str) + "-1", format = "%G%V-%w")
temp_df_new = df_new[df_new["DPNAME"] == "VIM WC BLUE 880ML (BOM BAY)"]
fig = go.Figure(
    data=[
        go.Scatter(
            x=temp_df_old["DATE"],
            y=temp_df_old["FC_PRI_BASELINE_WEEKLY"],
            name="old PRI",
        ),
        go.Scatter(
            x=temp_df_new["DATE"],
            y=temp_df_new["FC_PRI_BASELINE_WEEKLY"],
            name="new PRI",
        ),
        go.Scatter(
            x=temp_df_new["DATE"],
            y=temp_df_new["PROPOSED_FC_WEEKLY"],
            name="FC BASELINE SEC",
            marker={"color": "darkgreen"},
        ),
        # go.Scatter(x = temp_df_new["DATE"], y = temp_df_new["SEC_SALES"], name = "ACTUAL SEC")
    ]
)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # SKINCLEANSING

# COMMAND ----------

# old data
old_path = "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/OUTPUT_FORECAST/FC_BASELINE_SEC2PRI_HIS/20231010/SKINCLEANSING.xlsx"

df_old = pd.read_excel(old_path)

# COMMAND ----------

print(df_old.shape)
print(df_old["DPNAME"].nunique())
df_old.head(2)

# COMMAND ----------

list_dpname = [
    "LIFEBUOYHAND WASH TOTAL180G",
    "LIFEBUOYHAND WASH TOTAL500G",
]

# COMMAND ----------

# new data 
new_path = "/dbfs/mnt/adls/DT_SNOP_TOTALFC_BASELINE/OUTPUT_FORECAST/FC_BASELINE_SEC2PRI_HIS/20231011/SKINCLEANSING.xlsx"

df_new = pd.read_excel(new_path)

# COMMAND ----------

df_old.describe()

# COMMAND ----------

df_new.describe()

# COMMAND ----------

print(df_new.shape)
print(df_new["KEY"].nunique())
df_new.head(2)

# COMMAND ----------

display(df_new[(df_new["DPNAME"].isin(list_dpname)) & (df_new["YEARWEEK"] >= 202341)])

# COMMAND ----------

list_dpname

# COMMAND ----------

temp_df_old = df_old[df_old["DPNAME"] == "LIFEBUOYHAND WASH TOTAL500G"]
temp_df_old["DATE"] = pd.to_datetime(temp_df_old["YEARWEEK"].astype(str) + "-1", format = "%G%V-%w")
temp_df_new = df_new[df_new["DPNAME"] == "LIFEBUOYHAND WASH TOTAL500G"]
fig = go.Figure(
    data=[
        go.Scatter(
            x=temp_df_old["DATE"],
            y=temp_df_old["FC_PRI_BASELINE_WEEKLY"],
            name="old PRI",
        ),
        go.Scatter(
            x=temp_df_new["DATE"],
            y=temp_df_new["FC_PRI_BASELINE_WEEKLY"],
            name="new PRI",
        ),
        go.Scatter(
            x=temp_df_new["DATE"],
            y=temp_df_new["PROPOSED_FC_WEEKLY"],
            name="FC BASELINE SEC",
            marker={"color": "darkgreen"},
        ),
        # go.Scatter(x = temp_df_new["DATE"], y = temp_df_new["SEC_SALES"], name = "ACTUAL SEC")
    ]
)
fig.show()
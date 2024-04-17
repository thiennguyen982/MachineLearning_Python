# Databricks notebook source
import pickle

# COMMAND ----------

with open("tasks.pkl", "rb") as f:
  tasks = pickle.load(f)

# COMMAND ----------

ai_result = []
for task in tasks:
    ls_task = []
    for i in task:
        ls_task.append(i)
    ai_result.extend(ls_task)

# COMMAND ----------

import pandas as pd

# COMMAND ----------

df = pd.DataFrame()

# COMMAND ----------

df[["b", "c"]] = pd.DataFrame(ai_result)
df.head()

# COMMAND ----------

import pandas as pd

# COMMAND ----------

brand_df = pd.read_parquet("/dbfs/mnt/adls/NMLUONG/TEMP_OUTPUT/BRAND_OUTPUT.parquet")
brand_df.head()

# COMMAND ----------

brand_df.to_csv("./NEW_BRAND.csv", index=False, encoding="utf-8-sig")

# COMMAND ----------


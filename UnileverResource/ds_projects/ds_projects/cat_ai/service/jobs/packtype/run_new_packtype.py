# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Import modules

# COMMAND ----------

import pandas as pd
import numpy as np
import time

# COMMAND ----------

retailer = dbutils.widgets.get("retailer")
retailer

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # EDA data

# COMMAND ----------

start = time.time()

format_output = pd.read_parquet(
    f"/dbfs/mnt/adls/AI-Item/NEW_METHOD/{retailer}/FORMAT_OUTPUT.parquet"
)[["PRODUCT_UID", "TITLE", "FORMAT_PRED"]]
format_output.head(3)

# COMMAND ----------

keywords = ["combo", "cặp", "bộ", "set", "sét", "comnbo"]

kw_series = []
for kw in keywords:
    kw_series.append(format_output["TITLE"].str.lower().str.contains(kw).values)

# COMMAND ----------

kw_arr = np.array(kw_series)

# COMMAND ----------

single_mask = ~np.any(kw_arr, axis=0)

# COMMAND ----------

combo_mask = np.all(
    (np.any(kw_arr, axis=0), format_output["FORMAT_PRED"].str.len() >= 2), axis=0
)

# COMMAND ----------

multi_mask = np.all(
    (np.any(kw_arr, axis=0), format_output["FORMAT_PRED"].str.len() < 2),
    axis=0,
)

# COMMAND ----------

format_output.loc[single_mask, "PACKTYPE_PRED"] = "single"
format_output.loc[multi_mask, "PACKTYPE_PRED"] = "multi"
format_output.loc[combo_mask, "PACKTYPE_PRED"] = "combo"

# COMMAND ----------

format_output.to_parquet(
    f"/dbfs/mnt/adls/AI-Item/NEW_METHOD/{retailer}/PACKTYPE_OUTPUT.parquet", index=False
)

print(f"Execution time: {time.time() - start} for {format_output.shape[0]} records")

# COMMAND ----------


# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Import modules

# COMMAND ----------

import pandas as pd
import xlsxwriter
import shutil

# COMMAND ----------

old_df = pd.read_parquet("/dbfs/mnt/adls/NMLUONG/TEMP_OUTPUT/INPUT.parquet")

# COMMAND ----------

old_df = old_df.drop_duplicates(subset=["SKUID"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Compare CATEGORY

# COMMAND ----------

new_df = pd.read_parquet("/dbfs/mnt/adls/NMLUONG/TEMP_OUTPUT/CATEGORY_OUTPUT.parquet")
new_df = new_df.drop(new_df.columns[new_df.columns.str.contains("PREDICT")], axis=1)

# COMMAND ----------

category_compare = new_df[
    [
        "SKUID",
        "TITLE",
        "CATEGORY_PRED",
        "DEO_PROBA",
        "ORAL_PROBA",
        "SCL_PROBA",
        "SKIN_PROBA",
        "HAIR_PROBA",
    ]
].merge(old_df[["SKUID", "TITLE", "CATEGORY_PREDICT"]])
category_compare.head(3)

# COMMAND ----------

category_compare.to_excel(
    "./CATEGORY.xlsx",
    sheet_name="in",
    encoding="utf-8-sig",
    engine="xlsxwriter"
)

# COMMAND ----------

shutil.move("./CATEGORY.xlsx", "/dbfs/mnt/adls/NMLUONG/TEMP_OUTPUT/COMPARE_RESULT/CATEGORY.xlsx")

# COMMAND ----------

(category_compare["CATEGORY_PRED"] == category_compare["CATEGORY_PREDICT"]).mean()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Compare PACKTYPE

# COMMAND ----------

new_df = pd.read_parquet("/dbfs/mnt/adls/NMLUONG/TEMP_OUTPUT/PACKTYPE_OUTPUT.parquet")
new_df = new_df.drop(new_df.columns[new_df.columns.str.contains("PREDICT")], axis=1)

new_df.head(3)

# COMMAND ----------

packtype_compare = new_df[
    ["SKUID", "TITLE", "PACKTYPE_PRED"]
    + list(new_df.columns[new_df.columns.str.contains(pat=r"_PROBA")])
].merge(old_df[["SKUID", "TITLE", "PACKTYPE_PREDICT"]])

packtype_compare.head(3)

# COMMAND ----------

(packtype_compare["PACKTYPE_PRED"] == packtype_compare["PACKTYPE_PREDICT"]).mean()

# COMMAND ----------

packtype_compare.to_excel(
    "./PACKTYPE.xlsx",
    sheet_name="in",
    index=False
)

# COMMAND ----------

shutil.move("./PACKTYPE.xlsx", "/dbfs/mnt/adls/NMLUONG/TEMP_OUTPUT/COMPARE_RESULT/PACKTYPE.xlsx")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Compare SEGMENT

# COMMAND ----------

new_df = pd.read_parquet("/dbfs/mnt/adls/NMLUONG/TEMP_OUTPUT/SEGMENT_OUTPUT.parquet")
new_df = new_df.drop(new_df.columns[new_df.columns.str.contains("PREDICT")], axis=1)

new_df.head(2)

# COMMAND ----------

segment_compare = new_df[
    ["SKUID", "TITLE", "CATEGORY_PRED", "SEGMENT_PRED"]
    + list(new_df.columns[new_df.columns.str.contains(pat=r"_PROBA")])
].merge(old_df[["SKUID", "TITLE", "CATEGORY_PREDICT", "SEGMENT_PREDICT"]])

# COMMAND ----------

segment_compare.head(3)

# COMMAND ----------

segment_compare.to_excel(
    "./SEGMENT.xlsx",
    index=False,
    sheet_name="in",
)

# COMMAND ----------

shutil.move("./SEGMENT.xlsx", "/dbfs/mnt/adls/NMLUONG/TEMP_OUTPUT/COMPARE_RESULT/SEGMENT.xlsx")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Compare FORMAT

# COMMAND ----------

new_df = pd.read_parquet("/dbfs/mnt/adls/NMLUONG/TEMP_OUTPUT/FORMAT_OUTPUT.parquet")
new_df = new_df.drop(new_df.columns[new_df.columns.str.contains("PREDICT")], axis=1)

new_df.head(2)

# COMMAND ----------

format_compare = new_df[
    ["SKUID", "TITLE", "CATEGORY_PRED", "FORMAT_PRED"]
    + list(new_df.columns[new_df.columns.str.contains(pat=r"_PROBA")])
].merge(old_df[["SKUID", "TITLE", "CATEGORY_PREDICT", "FORMAT_PREDICT"]])

# COMMAND ----------

format_compare.head()

# COMMAND ----------

format_compare.to_excel(
    "./FORMAT.xlsx",
    index=False,
    sheet_name="in",
)

# COMMAND ----------

shutil.move("./FORMAT.xlsx", "/dbfs/mnt/adls/NMLUONG/TEMP_OUTPUT/COMPARE_RESULT/FORMAT.xlsx")
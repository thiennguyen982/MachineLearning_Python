# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Import modules

# COMMAND ----------

# MAGIC %run "../preprocess_nb"

# COMMAND ----------

import pandas as pd
import numpy as np
import re

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

import os

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Read OUTPUT of each module

# COMMAND ----------

category_df = spark.read.format("parquet").load(
    "/mnt/adls/NMLUONG/TEMP_OUTPUT/CATEGORY_OUTPUT.parquet"
)
category_df = category_df.toPandas()

# COMMAND ----------

brand_df = spark.read.format("parquet").load(
    "/mnt/adls/NMLUONG/TEMP_OUTPUT/BRAND_OUTPUT.parquet"
)

brand_df = brand_df.toPandas()

# COMMAND ----------

format_df = spark.read.format("parquet").load(
    "/mnt/adls/NMLUONG/TEMP_OUTPUT/FORMAT_OUTPUT.parquet"
)

format_df = format_df.toPandas()

# COMMAND ----------

segment_df = spark.read.format("parquet").load(
    "/mnt/adls/NMLUONG/TEMP_OUTPUT/SEGMENT_OUTPUT.parquet"
)

segment_df = segment_df.toPandas()

# COMMAND ----------

packtype_df = spark.read.format("parquet").load(
    "/mnt/adls/NMLUONG/TEMP_OUTPUT/PACKTYPE_OUTPUT.parquet"
)

packtype_df = packtype_df.toPandas()

# COMMAND ----------

final_df = category_df.merge(right=brand_df, how="inner").drop_duplicates(
    subset=["SKUID"]
)

# COMMAND ----------

final_df = final_df.merge(right=format_df, how="inner").drop_duplicates(
    subset=["SKUID"]
)

# COMMAND ----------

final_df = final_df.merge(right=segment_df, how="inner").drop_duplicates(
    subset=["SKUID"]
)

# COMMAND ----------

final_df = final_df.merge(right=packtype_df, how="inner").drop_duplicates(
    subset=["SKUID"]
)

# COMMAND ----------

final_df = final_df[
    [
        "SKUID",
        "TITLE",
        "CATEGORY_PRED",
        "BRAND_PRED",
        "BRAND_SUGGEST",
        "PACKTYPE_PRED",
        "FORMAT_PRED",
        "SEGMENT_PRED",
    ]
]

# COMMAND ----------

final_df.columns = [
    "SKUID",
    "TITLE_PROC",
    "CATEGORY_PREDICT",
    "BRAND_PREDICT",
    "BRAND_SUGGESTION",
    "PACKTYPE_PREDICT",
    "FORMAT_PREDICT",
    "SEGMENT_PREDICT",
]

# COMMAND ----------

final_df["SEGMENT_PREDICT"] = final_df["SEGMENT_PREDICT"].str.upper()

# COMMAND ----------

final_df.loc[final_df["CATEGORY_PREDICT"] == "scl", "CATEGORY_PREDICT"] = "skincleansing"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Mapping more field

# COMMAND ----------

udf_main_preprocess = F.udf(lambda x: main_preprocess(x), StringType())

mapping_addition_fields_df_sql = """
WITH TEMP_SELLER AS (SELECT
  seller_id SELLER_ID,
  CASE
    WHEN seller.seller_slug is null THEN seller.seller_name
    ELSE seller.seller_slug
  END AS SELLER_NAME,
  ROW_NUMBER() OVER (
    PARTITION BY seller.seller_id
    ORDER BY
      VALIDFROM DESC,
      VALIDTO ASC
  ) as seq
FROM
  pureplay.dim_pureplay_seller seller)
SELECT
  DISTINCT prod.product_id as PRODUCT_ID,
  lower(title) TITLE,
  product_url PRODUCTURL,
  seller.seller_id SELLER_ID,
  SELLER_NAME,
  cat.segment as CATEGORY_PLATFORM,
  cat.category_l2 as CATEGORY_L2,
  price.sale_price as SALE_PRICE,
  price.offer_price as OFFER_PRICE
FROM
  pureplay.dim_pureplay_product prod
  LEFT JOIN TEMP_SELLER seller on seller.seller_id = prod.seller_id and seller.seq = 1
  LEFT JOIN pureplay.dim_pureplay_category cat on cat.product_id = prod.product_id and cat.ISVALID = TRUE
  LEFT JOIN pureplay.dim_pureplay_pricing price on price.product_id = prod.product_id and price.ISVALID = TRUE
WHERE
  prod.retailer = 'shopee'
"""
mapping_addition_fields_df = (
    spark.sql(mapping_addition_fields_df_sql)
    .select(
        "PRODUCT_ID",
        udf_main_preprocess(F.col("TITLE")).alias("TITLE"),
        "SELLER_ID",
        "SELLER_NAME",
        "CATEGORY_PLATFORM",
        "PRODUCTURL",
        "SALE_PRICE",
        "OFFER_PRICE",
    )
    .distinct()
    .toPandas()
)

# COMMAND ----------

mapping_addition_fields_df["PRODUCT_ID"] = mapping_addition_fields_df[
    "PRODUCT_ID"
].astype(str)
final_df["SKUID"] = final_df["SKUID"].astype(str)

# map more fields for AI-OUTPUT
final_df = final_df.merge(
    mapping_addition_fields_df,
    how="left",
    left_on=["SKUID", "TITLE_PROC"],
    right_on=["PRODUCT_ID", "TITLE"],
)

# COMMAND ----------

final_df["SELLER"] = final_df["SELLER_NAME"]
final_df = final_df[
    [
        "SKUID",
        "TITLE",
        "PRODUCTURL",
        "CATEGORY_PREDICT",
        "BRAND_PREDICT",
        "BRAND_SUGGESTION",
        "PACKTYPE_PREDICT",
        "FORMAT_PREDICT",
        "SEGMENT_PREDICT",
        "SELLER",
        "CATEGORY_PLATFORM",
        "TITLE_PROC",
    ]
]

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Save AI-RESULT to adls

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Function for removing SKUID with exclude keyword

# COMMAND ----------

def opend_exclude_kw():
    EXCLUDE_PATH = "/dbfs/mnt/adls/AI-Item/Dictionary/EXCLUDE_KW/"
    ls_exclude = os.listdir(EXCLUDE_PATH)
    ls_cate = ["oral", "deo", "scl"]
    dict_excludekw_cate = {}
    for cate in ls_cate:
        files = [i for i in ls_exclude if cate in i.lower()]
        df_exclude = pd.read_excel(EXCLUDE_PATH + files[0], sheet_name="Exclude")
        if cate == "scl":
            cate = "skincleansing"
        dict_excludekw_cate[cate] = list(df_exclude["ExcludeKeywords"].str.lower())
    return dict_excludekw_cate

# COMMAND ----------

dict_excludekw_cate = opend_exclude_kw()

# COMMAND ----------

def full_exclude(title):
    for keyword in exclude_keywords:
        if keyword in title:
            return keyword
    return "NO"


def partial_exclude(title):
    count = 0
    for keyword in exclude_keywords:
        keyword_split = keyword.split(" ")
        count = 0
        for kw in keyword_split:
            if kw in title:
                count = count + 1
        if count == len(keyword_split):
            return keyword
    return "NO"


def run_exclude_kw(df, exclude_keywords):
    df["FULL_EXCLUDE"] = df["TITLE_PROC"].apply(lambda title: full_exclude(title))
    df["PARTIAL_EXCLUDE"] = df["TITLE_PROC"].apply(lambda title: partial_exclude(title))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exclude keyword and save result

# COMMAND ----------

import xlsxwriter
import os
from datetime import datetime
from zoneinfo import ZoneInfo
import shutil


# Get the current timestamp
current_timestamp = datetime.now(tz=ZoneInfo("Asia/Ho_Chi_Minh"))
# Convert the timestamp to a string in a specific format
# timestamp_string = current_timestamp.strftime("%d%m%Y %H:%M:%S")
timestamp_string = current_timestamp.strftime("%Y%m%d")
# time_string = current_timestamp.strftime("%H:%M:%S")

LAST_ROUND_PATH = f"/dbfs/mnt/adls/AI-Item/last_round/"
ls_last_round_file = os.listdir(LAST_ROUND_PATH)
OUT_BIZ = (
    f"/dbfs/mnt/adls/AI-Item/NEW_METHOD/{timestamp_string}/"
)

# create folder for each timestamp
os.makedirs(name=OUT_BIZ, exist_ok=True)
DF.to_parquet(f"{OUT_BIZ}{timestamp_string}_INPUT_DATA.parquet", index=False)

final_df_output = pd.DataFrame()
for cate in ["skincleansing", "oral", "deo"]:
    df_cate = final_df[final_df["CATEGORY_PREDICT"] == cate].reset_index(drop=True)
    print(df_cate.shape)
    file_last_round = [f for f in ls_last_round_file if cate in f.lower()]
    df_last_round = pd.read_excel(
        LAST_ROUND_PATH + file_last_round[0], sheet_name="FINAL_NRM_REVIEW"
    )
    df_last_round["TYPE"] = "old"
    df_last_round = df_last_round[
        [
            "SKUID",
            "TITLE",
            "CATEGORY_PREDICT",
            "BRAND_PREDICT",
            "BRAND_SUGGESTION",
            "PACKTYPE_PREDICT",
            "FORMAT_PREDICT",
            "SEGMENT_PREDICT",
            "SELLER",
            "CATEGORY_PLATFORM",
            "FULL_EXCLUDE",
            "PARTIAL_EXCLUDE",
            "TYPE",
        ]
    ]
    print(df_last_round.shape)
    exclude_keywords = dict_excludekw_cate[cate]
    df_cate_exclude = run_exclude_kw(df_cate, exclude_keywords)
    df_cate_exclude["TYPE"] = "new"
    df_cate_exclude = df_cate_exclude[
        [
            "SKUID",
            "TITLE",
            "CATEGORY_PREDICT",
            "BRAND_PREDICT",
            "BRAND_SUGGESTION",
            "PACKTYPE_PREDICT",
            "FORMAT_PREDICT",
            "SEGMENT_PREDICT",
            "SELLER",
            "CATEGORY_PLATFORM",
            "FULL_EXCLUDE",
            "PARTIAL_EXCLUDE",
            "TYPE",
        ]
    ]
    print(cate, df_cate_exclude.shape)
    df_cate_exclude = pd.concat([df_last_round, df_cate_exclude], ignore_index=True)
    df_cate_exclude = df_cate_exclude.drop_duplicates(["SKUID", "TITLE"], keep="first")
    print(cate, df_cate_exclude.shape)
    new_filename = f"{timestamp_string}_OUTPUT_{cate.upper()}.xlsx"
    name = "/tmp/" + new_filename
    output = OUT_BIZ + new_filename

    with pd.ExcelWriter(name, engine="xlsxwriter") as writer:
        df_cate_exclude.to_excel(
            writer,
            sheet_name="RAW_RESULT",
        )
        df_cate_exclude.query("FULL_EXCLUDE != 'NO'").to_excel(
            writer, sheet_name="EXCLUDE_BY_KEYWORD_LIST", index=False
        )
        df_cate_exclude.query("FULL_EXCLUDE == 'NO'").to_excel(
            writer, sheet_name="FINAL_NRM_REVIEW", index=False
        )
        writer.close()
        shutil.copy(name, output)
    final_df_output = pd.concat([final_df_output, df_cate_exclude], ignore_index=True)

# COMMAND ----------


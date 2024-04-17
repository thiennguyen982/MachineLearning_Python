# Databricks notebook source
!pip install ray 

# COMMAND ----------

!pip install tqdm

# COMMAND ----------

!pip install unicodedata unidecode

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Import Modules

# COMMAND ----------

import numpy as np
import pandas as pd
import re
import unidecode
import unicodedata

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

import os
from glob import glob
from tqdm import tqdm

import ray

# COMMAND ----------

num_cpus = os.cpu_count()
num_processes = num_cpus * 4

# COMMAND ----------

dbutils.widgets.dropdown("retailer", "shopee", ["shopee", "lazada"])

# COMMAND ----------

retailer = dbutils.widgets.get("retailer")
retailer

# COMMAND ----------

def standardize_sent(sent):  # lower case and normalize
    sent = unicodedata.normalize("NFKC", sent).lower()
    return sent


def remove_noise_by_token(sent):  # Remove special character, packsize in sent
    """The objective of this function is remove noist text which can impact to model: packsize, non-text, text in square
    Note: Some of the text in square bracket contain importance keywork such as: combo, so, we need to detect text in square is combo or not,
    if yes: adding combo to title. See: combo_bracket function
    Some of the text contain number and text: 5nice => need to detect and keep these words"""
    title_replace_token = ['" +"', "|", "  ", "'", "xa0", ",", "+", "_", "\n"]
    title_regex_arr = ["[\[|<].*?###[]|>]", "[^\w\s]", "[^\w\s,]"]
    find_string = re.compile("[a-z]")
    for r in title_regex_arr:
        sent = re.sub(r, "", sent)
    sent = sent.split(" ")
    for r in title_replace_token:
        sent = [s.replace(r, "") for s in sent]
    sent = [s for s in sent if len(s.replace(" ", "")) > 0]
    sent = [
        s for s in sent if len(find_string.findall(unidecode.unidecode(s))) > 0
    ]  # remove text with contain number and alphabet
    sent = " ".join(sent)
    return sent


def combo_bracket(
    sent,
):  # Before remove text in []. we need to detect "combo" appear on [] or not and auto take "combo" out
    text_in = re.search(r"\[([A-Za-z0-9_]+)\]", sent)
    if text_in == None:
        text_in = ""
    else:
        text_in = text_in.group(1)
    sent = re.sub(r"\[.*?\]", "", sent)
    if "combo" in text_in:
        if "combo" in sent:
            sent = sent
        else:
            sent = "combo" + " " + sent
    else:
        sent = sent
    return sent

# COMMAND ----------

def main_preprocess(sent):
    """
    preprocessing input text or title

    Input:
      title

    Return:
      preprocessed title.
    """
    sent = standardize_sent(sent)
    sent = remove_noise_by_token(sent)
    sent = combo_bracket(sent)
    sent = sent.replace("  ", " ")
    return sent

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Post-processing

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Read previous input

# COMMAND ----------

prev_input_df = spark.read.format("parquet").load(
    f"/mnt/adls/AI-Item/NEW_METHOD/{retailer}/INPUT.parquet"
)

# COMMAND ----------

df = (
    spark.read.format("delta")
    .table("pureplay.dim_pureplay_product")
    .filter(f'retailer=="{retailer}"')
    .select(
        "product_uid",
        "product_id",
        "title",
    )
    .distinct()
    .filter(F.col("title").isNotNull())
    .withColumnRenamed("product_uid", "PRODUCT_UID")
    .withColumnRenamed("product_id", "SKUID")
    .withColumnRenamed("title", "TITLE")
)

# COMMAND ----------

prev_input_df = (
    prev_input_df.alias("input")
    .join(
        other=df.alias("total_data"),
        on=[prev_input_df.PRODUCT_UID == df.PRODUCT_UID],
        how="left",
    )
    .filter("total_data.PRODUCT_UID is not NULL")
    .select("total_data.SKUID", "input.*")
)

# COMMAND ----------

prev_input_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exclude last result

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Category

# COMMAND ----------

category_df = spark.read.format("parquet").load(
    f"/mnt/adls/AI-Item/NEW_METHOD/{retailer}/CATEGORY_OUTPUT.parquet"
)

display(category_df)

# COMMAND ----------

category_df = spark.read.format("parquet").load(
    f"/mnt/adls/AI-Item/NEW_METHOD/{retailer}/CATEGORY_OUTPUT.parquet"
)

print(f"Number of Category before exclude low proba: {category_df.count()}")

# rowmax = F.greatest(
#     *[
#         F.col(x)
#         for x in [col for col in category_df.columns if col.endswith("LOGIT")]
#     ]
# )

# category_df = category_df.withColumn('CONFIDENT', rowmax)

# category_df = (
#     category_df.withColumn(
#         "MAX_CONF",
#         F.max(F.col("CONFIDENT")).over(
#             Window.partitionBy("CATEGORY_PRED").orderBy(F.col("CONFIDENT").desc())
#         ),
#     )
#     .withColumn("CONFIDENT_SCORE", F.col("CONFIDENT") / F.col("MAX_CONF"))
#     .select("PRODUCT_UID", "TITLE", "CONFIDENT_SCORE", "CATEGORY_PRED")
#     .dropna(subset="PRODUCT_UID")
# )

category_df = (
    category_df.alias("category")
    .join(
        prev_input_df.alias("input"),
        on=[category_df.PRODUCT_UID == prev_input_df.PRODUCT_UID],
        how="inner",
    )
    .select("SKUID", "category.*")
)

prob_columns = [column for column in category_df.columns if column.endswith("_PROBA")]

category_df = category_df.withColumn(
    "CATEGORY_PROBA", F.greatest(*[F.col(column) for column in prob_columns])
)

# category_df = category_df.filter(F.col("CONFIDENT_SCORE") >= 0.7).select(
#     "SKUID", "PRODUCT_UID", "TITLE", "CATEGORY_PRED"
# )
category_df = category_df.filter(F.col("CATEGORY_PROBA") >= 0.7).select(
    "SKUID", "PRODUCT_UID", "TITLE", "CATEGORY_PRED"
)

category_df = category_df.filter(F.col("CATEGORY_PRED").isin(["oral", "deo", "scl"]))

category_df = category_df.toPandas()

print(f"Number of Category after exclude low proba: {category_df.shape[0]}")
category_df.head(3)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Brand

# COMMAND ----------

brand_df = spark.read.format("parquet").load(
    f"/mnt/adls/AI-Item/NEW_METHOD/{retailer}/BRAND_OUTPUT.parquet"
)

brand_df = (
    brand_df.alias("brand")
    .join(
        prev_input_df.alias("input"),
        on=[brand_df.PRODUCT_UID == prev_input_df.PRODUCT_UID],
        how="inner",
    )
    .select("SKUID", "brand.*")
)

brand_df = brand_df.filter(F.col("CATEGORY_PRED").isin(["oral", "deo", "scl"])).select(
    "SKUID", "PRODUCT_UID", "TITLE", "BRAND_PRED", "BRAND_MAPPED", "SCORES"
)

brand_df = brand_df.toPandas()
brand_dict = brand_df.to_dict("records")

for row in brand_dict:
    scores = row["SCORES"].tolist()
    brands = row["BRAND_MAPPED"].tolist()

    if len(scores) < 1:
        row["BRAND_PRED"] = "not-detected"
        row["BRAND_SUGGEST"] = "no suggestion"
        continue

    new_brands = []
    for score, brand in zip(scores, brands):
        if score >= 70:
            new_brands.append(brand)
    if len(new_brands) >= 1:
        row["BRAND_SUGGEST"] = "no suggestion"
        row["BRAND_PRED"] = "|".join(new_brands)
    else:
        row["BRAND_SUGGEST"] = (
            "no suggestion"
            if len(row["BRAND_PRED"]) < 1
            else "|".join([brand.lower() for brand in row["BRAND_PRED"].tolist()])
        )
        row["BRAND_PRED"] = "not-detected"
brand_df = pd.DataFrame(brand_dict)
brand_df = brand_df[["SKUID", "PRODUCT_UID", "TITLE", "BRAND_PRED", "BRAND_SUGGEST"]]

brand_df.head(3)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Format

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Not exclude

# COMMAND ----------

format_df = spark.read.format("parquet").load(
    f"/mnt/adls/AI-Item/NEW_METHOD/{retailer}/FORMAT_OUTPUT.parquet"
)

format_df = (
    format_df.alias("format")
    .join(
        prev_input_df.alias("input"),
        on=[format_df.PRODUCT_UID == prev_input_df.PRODUCT_UID],
        how="inner",
    )
    .select("SKUID", "format.*")
)

format_df = format_df.select("PRODUCT_UID", "SKUID", "TITLE", "FORMAT_PRED").toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Exclude

# COMMAND ----------

# format_df = spark.read.format("parquet").load(
#     "/mnt/adls/AI-Item/NEW_METHOD/FORMAT_OUTPUT.parquet"
# )

# format_df = (
#     format_df.alias("format")
#     .join(
#         prev_input_df.alias("input"),
#         on=[format_df.PRODUCT_UID == prev_input_df.PRODUCT_UID],
#         how="inner",
#     )
#     .select("SKUID", "format.*")
# )

# format_df = format_df.filter(F.col("CATEGORY_PRED").isin(["oral", "deo", "scl"]))

# format_df = format_df.toPandas()
# dfs = np.array_split(format_df, num_processes)


# @ray.remote
# def REMOTE_get_format(df: pd.DataFrame):
#     new_df = []

#     format_dict = df.to_dict("records")
#     for row in format_dict:
#         formats = row["FORMAT_PRED"].tolist()

#         new_formats = []
#         for format_ in formats:
#             proba_col = f"{format_}_PROBA"
#             proba = row[proba_col]

#             if proba >= 0.7:
#                 new_formats.append(format_)
#         new_df.append(
#             {
#                 "SKUID": row["SKUID"],
#                 "PRODUCT_UID": row["PRODUCT_UID"],
#                 "TITLE": row["TITLE"],
#                 "FORMAT_PRED": new_formats,
#             }
#         )
#     return new_df


# tasks = [REMOTE_get_format.remote(df) for df in tqdm(dfs)]

# tasks = ray.get(tasks)
# format_df = pd.DataFrame(sum(tasks, []))

# format_df.head(3)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Segment

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Not Exclude

# COMMAND ----------

segment_df = spark.read.format("parquet").load(
    f"/mnt/adls/AI-Item/NEW_METHOD/{retailer}/SEGMENT_OUTPUT.parquet"
)

segment_df = (
    segment_df.alias("segment")
    .join(
        prev_input_df.alias("input"),
        on=[segment_df.PRODUCT_UID == prev_input_df.PRODUCT_UID],
        how="inner",
    )
    .select("SKUID", "segment.*")
)

segment_df = segment_df.select("PRODUCT_UID", "SKUID", "TITLE", "SEGMENT_PRED").toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Exclude

# COMMAND ----------

# segment_df = spark.read.format("parquet").load(
#     "/mnt/adls/AI-Item/NEW_METHOD/SEGMENT_OUTPUT.parquet"
# )

# segment_df = (
#     segment_df.alias("segment")
#     .join(
#         prev_input_df.alias("input"),
#         on=[segment_df.PRODUCT_UID == prev_input_df.PRODUCT_UID],
#         how="inner",
#     )
#     .select("SKUID", "segment.*")
# )

# prob_columns = [column for column in segment_df.columns if column.endswith("_PROBA")]

# segment_df = segment_df.withColumn(
#     "SEGMENT_PROBA", F.greatest(*[F.col(column) for column in prob_columns])
# )

# segment_df = segment_df.filter(F.col("SEGMENT_PROBA") >= 0.7).select(
#     "SKUID", "PRODUCT_UID", "TITLE", "SEGMENT_PRED"
# )

# segment_df = segment_df.toPandas()
# segment_df.head(3)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Packtype

# COMMAND ----------

packtype_df = spark.read.format("parquet").load(
    f"/mnt/adls/AI-Item/NEW_METHOD/{retailer}/PACKTYPE_OUTPUT.parquet"
)

packtype_df = (
    packtype_df.alias("packtype")
    .join(
        prev_input_df.alias("input"),
        on=[packtype_df.PRODUCT_UID == prev_input_df.PRODUCT_UID],
        how="inner",
    )
    .select("SKUID", "packtype.PRODUCT_UID", "packtype.TITLE", "packtype.PACKTYPE_PRED")
)


packtype_df = packtype_df.toPandas()
packtype_df.head(3)

# COMMAND ----------

# packtype_df = spark.read.format("parquet").load(
#     "/mnt/adls/AI-Item/NEW_METHOD/PACKTYPE_OUTPUT.parquet"
# )

# packtype_df = (
#     packtype_df.alias("packtype")
#     .join(
#         prev_input_df.alias("input"),
#         on=[packtype_df.PRODUCT_UID == prev_input_df.PRODUCT_UID],
#         how="inner",
#     )
#     .select("SKUID", "packtype.*")
# )

# prob_columns = [column for column in packtype_df.columns if column.endswith("_PROBA")]

# packtype_df = packtype_df.withColumn(
#     "PACKTYPE_PROBA", F.greatest(*[F.col(column) for column in prob_columns])
# )

# packtype_df = packtype_df.filter(F.col("PACKTYPE_PROBA") >= 0.7).select(
#     "SKUID", "PRODUCT_UID", "TITLE", "PACKTYPE_PRED"
# )

# packtype_df = packtype_df.toPandas()
# packtype_df.head(3)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Merge all result

# COMMAND ----------

final_df = category_df.merge(
    right=brand_df, how="inner", on=["PRODUCT_UID", "SKUID", "TITLE"]
).drop_duplicates(subset=["SKUID"])

# COMMAND ----------

final_df = final_df.merge(
    right=format_df, how="inner", on=["PRODUCT_UID", "SKUID", "TITLE"]
).drop_duplicates(subset=["SKUID"])

# COMMAND ----------

final_df = final_df.merge(
    right=segment_df, how="left", on=["PRODUCT_UID", "SKUID", "TITLE"]
).drop_duplicates(subset=["SKUID"])

# COMMAND ----------

final_df = final_df.merge(
    right=packtype_df, how="inner", on=["PRODUCT_UID", "SKUID", "TITLE"]
).drop_duplicates(subset=["SKUID"])

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
    "TITLE",
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
    df["FULL_EXCLUDE"] = df["TITLE"].apply(
        lambda title: full_exclude(main_preprocess(title))
    )
    df["PARTIAL_EXCLUDE"] = df["TITLE"].apply(
        lambda title: partial_exclude(title.lower())
    )
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

LAST_ROUND_PATH = f"/dbfs/mnt/adls/AI-Item/last_round/{retailer}/"
ls_last_round_file = os.listdir(LAST_ROUND_PATH)
OUT_BIZ = f"/dbfs/mnt/adls/AI-Item/Final_after_exclude/{timestamp_string}/{retailer}/"

# create folder for each timestamp
os.makedirs(name=OUT_BIZ, exist_ok=True)
# DF.to_parquet(f"{OUT_BIZ}{timestamp_string}_INPUT_DATA.parquet", index=False)

final_df_output = pd.DataFrame()
for cate in ["skincleansing", "oral", "deo"]:
    df_cate = final_df[final_df["CATEGORY_PREDICT"] == cate].reset_index(drop=True)
    print(df_cate.shape)
    file_last_round = [f for f in ls_last_round_file if cate in f.lower()]
    if len(file_last_round) > 0:
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
                "FULL_EXCLUDE",
                "PARTIAL_EXCLUDE",
                "TYPE",
            ]
        ]
        print(df_last_round.shape)
    print(0)
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
            "FULL_EXCLUDE",
            "PARTIAL_EXCLUDE",
            "TYPE",
        ]
    ]
    print(cate, df_cate_exclude.shape)
    if len(file_last_round) > 0:
        df_cate_exclude = pd.concat([df_last_round, df_cate_exclude], ignore_index=True)
    else:
        pass
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


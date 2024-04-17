# Databricks notebook source
# MAGIC %pip install ray pytorch-crf~=0.7.2 fuzzywuzzy

# COMMAND ----------

# MAGIC %pip install "/dbfs/mnt/adls/NMLUONG/PRODUCTION/brand_detector/brand_detector-0.0.0.tar.gz"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Import modules

# COMMAND ----------

import transformers
import os
import datasets
from tqdm.notebook import tqdm
import torch
import pandas as pd
import ray
from brand_detector.module.predictor import ViTagger
from fuzzywuzzy import fuzz
import numpy as np
import math
import warnings
warnings.filterwarnings("ignore")

num_cpus = os.cpu_count()
num_gpus = torch.cuda.device_count()

os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
transformers.logging.set_verbosity_error()
datasets.logging.set_verbosity_error()

# COMMAND ----------

num_processes = math.floor(num_gpus / 0.4)
num_processes

# COMMAND ----------

retailer = dbutils.widgets.get("retailer")
retailer

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Define function for detecting Brand

# COMMAND ----------

def get_brand(row: pd.Series, ray_tagger):
    brands = ray_tagger(row["TITLE"])
    output = row
    output["BRAND_PRED"] = brands

    return output

# COMMAND ----------

@ray.remote(num_gpus=4 / 10)
def REMOTE_get_brand(ray_rows: pd.DataFrame, ray_tagger):
    outputs = []

    for idx, row in ray_rows.iterrows():
        outputs.append(get_brand(row, ray_tagger))
    return outputs

# COMMAND ----------

@ray.remote
def get_scores(chunk: pd.DataFrame, brand_list: list):
    all_outputs = []

    for idx, row in chunk.iterrows():
        new_row = row
        brands = row["BRAND_PRED"]

        scores = []
        new_brands = []

        for brand_pred in brands:
            score_dict = {
                brand: fuzz.ratio(brand, brand_pred.lower()) for brand in brand_list
            }
            score_dict = dict(
                sorted(score_dict.items(), key=lambda item: item[1], reverse=True)
            )

            brand = list(score_dict)[0]
            score = score_dict[brand]

            scores.append(score)
            new_brands.append(brand)
        
        
        new_row["BRAND_MAPPED"] = new_brands
        new_row["SCORES"] = scores

        all_outputs.append(row)

    return all_outputs

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Read INPUT DATA from adls

# COMMAND ----------

input_df = pd.read_parquet(f"/dbfs/mnt/adls/AI-Item/NEW_METHOD/{retailer}/CATEGORY_OUTPUT.parquet")
input_df = input_df[["PRODUCT_UID", "TITLE", "CATEGORY_PRED"]]

input_df.shape

# COMMAND ----------

categories = input_df["CATEGORY_PRED"].unique().tolist()
categories

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Run AI

# COMMAND ----------

import time

start = time.time()

category_list = categories  # ["deo", "oral", "scl", "skin", "hair"]
total_outputs = []

for category in category_list:
    print(20 * "=")
    print(f"Running Brand Detection on Category - {category}")

    brand_list_file = (
        f"/dbfs/mnt/adls/AI-Item/Dictionary/BRAND/BRAND_{category.upper()}_LIST.xlsx"
    )
    df_brand = pd.read_excel(brand_list_file, engine="openpyxl")
    df_brand["BRAND_FAMILY"] = df_brand["BRAND_FAMILY"].astype(str)
    brand_list = df_brand["BRAND_FAMILY"].unique().tolist()

    if category == "oral":
        load_path = "/dbfs/mnt/adls/NMLUONG/PRODUCTION/brand_detector/oral/0.9966384969453049.pt"
    elif category == "deo":
        load_path = (
            "/dbfs/mnt/adls/NMLUONG/PRODUCTION/brand_detector/deo/0.9966384969453049.pt"
        )
    elif category == "scl":
        load_path = (
            "/dbfs/mnt/adls/NMLUONG/PRODUCTION/brand_detector/scl/0.9902294224793954.pt"
        )
    elif category == "skin":
        load_path = "/dbfs/mnt/adls/NMLUONG/PRODUCTION/brand_detector/skin/0.9979712363968795.pt"
    elif category == "hair":
        load_path = "/dbfs/mnt/adls/NMLUONG/PRODUCTION/brand_detector/hair/0.9929558879085434.pt"

    print(f"Init model for Category - {category}")
    tagger = ViTagger(load_path)
    ref_tagger = ray.put(tagger)

    input_dataset = input_df[input_df["CATEGORY_PRED"] == category]
    chunks = np.array_split(input_dataset, num_processes)

    tasks = [REMOTE_get_brand.remote(ray.put(rows), ref_tagger) for rows in chunks]
    tasks = ray.get(tasks)

    print("Running Brand Standardization...\n\n")
    brand_output = pd.concat(sum(tasks, []), axis=1).T
    chunks = np.array_split(brand_output, num_cpus * 4)
    tasks = [
        get_scores.remote(chunk=ray.put(chunk), brand_list=brand_list)
        for chunk in chunks
    ]
    tasks = ray.get(tasks)

    total_outputs.extend(sum(tasks, []))

final_df: pd.DataFrame = pd.concat(total_outputs, axis=1).T
final_df.to_parquet(
    f"/dbfs/mnt/adls/AI-Item/NEW_METHOD/{retailer}/BRAND_OUTPUT.parquet", index=False
)

print(f"Execution time: {time.time() - start} for {final_df.shape[0]} records")

# COMMAND ----------

# import time

# start = time.time()

# chunks = np.array_split(input_df, num_processes)

# tasks = [REMOTE_get_brand.remote(ray.put(rows), ref_tagger) for rows in chunks]
# tasks = ray.get(tasks)
# brand_output = pd.concat(sum(tasks, []), axis=1).T
# brand_output.to_parquet("/dbfs/mnt/adls/AI-Item/NEW_METHOD/BRAND_OUTPUT.parquet")

# print(f"Execution time: {time.time() - start} for {brand_output.shape[0]} records")

# COMMAND ----------


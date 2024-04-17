# Databricks notebook source
# MAGIC %pip install ray

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

num_cpus = os.cpu_count()
num_gpus = torch.cuda.device_count()

os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
transformers.logging.set_verbosity_error()
datasets.logging.set_verbosity_error()

# COMMAND ----------

# MAGIC %run "../default_label_nb"

# COMMAND ----------

# MAGIC %run "../load_model_nb"

# COMMAND ----------

# MAGIC %run "./segment_classifier_nb"

# COMMAND ----------

# MAGIC %run "../tokenizer_nb"

# COMMAND ----------

from loguru import logger
import pandas as pd
import numpy as np
import ray
import math

# COMMAND ----------

num_processes = math.floor(num_gpus / 0.4)
num_processes

# COMMAND ----------

retailer = dbutils.widgets.get("retailer")
retailer

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Run Segment Classification

# COMMAND ----------

seg_classifier = SegmentClassifier(
    load_path="/dbfs/mnt/adls/NMLUONG/PRODUCTION/weights/segment_model/weight_title_0.99.pth"
)

# COMMAND ----------

ref_classifier = ray.put(seg_classifier)

# COMMAND ----------

@ray.remote(num_gpus=0.4)
def REMOTE_get_segment(df: pd.DataFrame, classifier: SegmentClassifier):
    return_df = classifier.get_segment(input_dataset=df, return_proba=True)

    return return_df

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Read in Category Output

# COMMAND ----------

input_df = pd.read_parquet(f"/dbfs/mnt/adls/AI-Item/NEW_METHOD/{retailer}/CATEGORY_OUTPUT.parquet")
input_df = input_df[["PRODUCT_UID", "TITLE", "CATEGORY_PRED"]]

# filter category
category_mask = input_df["CATEGORY_PRED"].isin(["deo"])
input_df = input_df.loc[category_mask, :]
input_df.shape

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Run AI

# COMMAND ----------

import time

start = time.time()

chunks = np.array_split(input_df, num_processes)
tasks = [REMOTE_get_segment.remote(ray.put(chunk), ref_classifier) for chunk in chunks]

tasks = ray.get(tasks)
final_df: pd.DataFrame = pd.concat(tasks)

final_df.to_parquet(
    f"/dbfs/mnt/adls/AI-Item/NEW_METHOD/{retailer}/SEGMENT_OUTPUT.parquet", index=False
)

print(f"Execution time: {time.time() - start} for {input_df.shape[0]} records")

# COMMAND ----------


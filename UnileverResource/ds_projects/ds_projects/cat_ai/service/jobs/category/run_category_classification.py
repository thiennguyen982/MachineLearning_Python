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

# MAGIC %run "./category_classifier_nb"

# COMMAND ----------

# MAGIC %run "../load_model_nb"

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
# MAGIC # Run Category Classification

# COMMAND ----------

cat_classifier = CategoryClassifier(
    load_path="/dbfs/mnt/adls/NMLUONG/PRODUCTION/weights/category_model/6_CAT/epoch12_test_acc0.985_train_loss0.050_train_acc0.985.pth",
    category_labels=Category.SIX_CAT.value,
)
logger.info(f"RUNNING CATEGORY CLASSIFICATION!!!")

# COMMAND ----------

@ray.remote(num_gpus=0.4)
def REMOTE_get_category(df: pd.DataFrame, classifier: CategoryClassifier):
    return_df = classifier.get_category(input_dataset=df, return_proba=True, return_logit=True)

    return return_df

# COMMAND ----------

ref_cat_classifier = ray.put(cat_classifier)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Read INPUT DATA from adls

# COMMAND ----------

input_df = spark.read.format("parquet").load(
    f"/mnt/adls/AI-Item/NEW_METHOD/{retailer}/INPUT.parquet"
)
input_df = input_df.toPandas()
input_df = input_df[["PRODUCT_UID", "TITLE"]]

# COMMAND ----------

input_df.shape

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Run AI

# COMMAND ----------

import time

start = time.time()

chunks = np.array_split(input_df, 4 * num_processes)
tasks = [
    REMOTE_get_category.remote(ray.put(chunk), ref_cat_classifier) for chunk in chunks
]

tasks = ray.get(tasks)
final_df: pd.DataFrame = pd.concat(tasks)

final_df.to_parquet(
    f"/dbfs/mnt/adls/AI-Item/NEW_METHOD/{retailer}/ALL_CATEGORY_OUTPUT.parquet", index=False
)
final_df[final_df["CATEGORY_PRED"].isin(["deo", "oral", "scl"])].to_parquet(
    f"/dbfs/mnt/adls/AI-Item/NEW_METHOD/{retailer}/CATEGORY_OUTPUT.parquet", index=False
)

print(f"Execution time: {time.time() - start} for {final_df.shape[0]} records")

# COMMAND ----------


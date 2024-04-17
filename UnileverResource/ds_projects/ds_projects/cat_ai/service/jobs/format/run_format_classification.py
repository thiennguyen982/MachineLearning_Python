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

# MAGIC %run "./format_classifier_nb"

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
# MAGIC # Run Format classification

# COMMAND ----------

@ray.remote(num_gpus=0.4)
def REMOTE_get_format(model: FormatClassifier, chunk: pd.DataFrame):
    return model.get_format(
        chunk,
        return_proba=True,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Read Category result

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
# MAGIC ## Run AI

# COMMAND ----------

import time

start = time.time()

total_outputs = []

for category in categories:
    if category == "oral":
        load_path = "/dbfs/mnt/adls/NMLUONG/PRODUCTION/weights/format_model/oral/epoch_27_test_acc_0.979_train_acc_0.979_train_loss_0.005.pth"
        labels = Format.ORAL.value
    elif category == "deo":
        load_path = "/dbfs/mnt/adls/NMLUONG/PRODUCTION/weights/format_model/deo/epoch_39_test_acc_0.995_train_acc_0.997_train_loss_0.002.pth"
        labels = Format.DEO.value
    elif category == "scl":
        load_path = "/dbfs/mnt/adls/NMLUONG/PRODUCTION/weights/format_model/scl/epoch_14_test_acc_0.990_train_acc_0.991_train_loss_0.007.pth"
        labels = Format.SCL.value
    elif category == "skin":
        load_path = "/dbfs/mnt/adls/NMLUONG/PRODUCTION/weights/format_model/skin/epoch_8_test_acc_0.990_train_acc_0.993_train_loss_0.004.pth"
        labels = Format.SKIN.value
    elif category == "hair":
        load_path = "/dbfs/mnt/adls/NMLUONG/PRODUCTION/weights/format_model/hair/epoch_11_test_acc_0.981_train_acc_0.990_train_loss_0.009.pth"
        labels = Format.HAIR.value
    else:
        continue

    print(f"Running Format Classification on Category - {category}")
    print(f"Init model for Category - {category}")
    model = FormatClassifier(format_labels=labels, load_path=load_path)
    ref_model = ray.put(model)

    input_dataset = input_df[input_df["CATEGORY_PRED"] == category]
    chunks = np.array_split(input_dataset, num_processes)

    tasks = [REMOTE_get_format.remote(ref_model, ray.put(chunk)) for chunk in chunks]
    tasks = ray.get(tasks)
    total_outputs.extend(tasks)

final_df: pd.DataFrame = pd.concat(total_outputs)
final_df.to_parquet(
    f"/dbfs/mnt/adls/AI-Item/NEW_METHOD/{retailer}/FORMAT_OUTPUT.parquet", index=False
)

print(f"Execution time: {time.time() - start} for {final_df.shape[0]} records")

# COMMAND ----------


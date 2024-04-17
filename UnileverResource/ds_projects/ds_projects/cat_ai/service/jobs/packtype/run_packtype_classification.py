# Databricks notebook source
# MAGIC %pip install ray

# COMMAND ----------

# MAGIC %md 
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

# MAGIC %run "../tokenizer_nb"

# COMMAND ----------

# MAGIC %run "./packtype_classifier_nb"

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

# MAGIC %md
# MAGIC
# MAGIC # Run PAKCTYPE Classification

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Infer Function

# COMMAND ----------

packtype_classifier = PacktypeClassifier(
    load_path="/dbfs/mnt/adls/NMLUONG/PRODUCTION/weights/packtype_model/weight_title_0.95.pth"
)

# COMMAND ----------

ref_classfier = ray.put(packtype_classifier)

# COMMAND ----------

@ray.remote(num_gpus=0.4)
def REMOTE_get_packtype(df: pd.DataFrame, classifier: PacktypeClassifier):
    return_df = classifier.get_packtype(input_dataset=df, return_proba=True)

    return return_df

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Read in Input Data

# COMMAND ----------

input_df = spark.read.format("parquet").load(
    "/mnt/adls/AI-Item/NEW_METHOD/INPUT.parquet"
)
input_df = input_df.toPandas()
input_df = input_df[["PRODUCT_UID", "TITLE"]]

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Run AI

# COMMAND ----------

import time

start = time.time()

chunks = np.array_split(input_df, num_processes)
tasks = [REMOTE_get_packtype.remote(ray.put(chunk), ref_classfier) for chunk in chunks]

tasks = ray.get(tasks)
final_df: pd.DataFrame = pd.concat(tasks)
final_df.to_parquet(
    "/dbfs/mnt/adls/AI-Item/NEW_METHOD/PACKTYPE_OUTPUT.parquet", index=False
)

print(f"Execution time: {time.time() - start} for {final_df.shape[0]} records")

# COMMAND ----------


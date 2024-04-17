# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Import modules

# COMMAND ----------

import transformers
import os
import datasets
from tqdm.notebook import tqdm

os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
transformers.logging.set_verbosity_error()
datasets.logging.set_verbosity_error()

# COMMAND ----------

# MAGIC %run "./default_label_nb"

# COMMAND ----------

# MAGIC %run "./category_classifier_nb"

# COMMAND ----------

# MAGIC %run "./load_model_nb"

# COMMAND ----------

# MAGIC %run "./tokenizer_nb"

# COMMAND ----------

from loguru import logger
import pandas as pd
import numpy as np

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Run Category Classification

# COMMAND ----------

def ai_infer(
    input_data: pd.DataFrame,
):
    # create copy of data
    final_data = input_data.copy(deep=True)

    # run category classification model
    logger.info(f"RUNNING CATEGORY CLASSIFICATION!!!")

    final_data = get_category(
        input_dataset=final_data,
        input_col="TITLE",
        ckpt_path="/dbfs/mnt/adls/NMLUONG/PRODUCTION/weights/category_model/5_CAT/weight_0.95.pth",
        category_labels=["deo", "oral", "scl", "skin", "hair"],
        return_proba=True,
    )

    # TODO: remove this line
    return final_data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Read INPUT DATA from adls

# COMMAND ----------

DF = pd.read_excel("/dbfs/mnt/adls/NMLUONG/ENTITY_OUTPUT.xlsx", sheet_name="Sheet1", engine="openpyxl")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Run AI

# COMMAND ----------

import time

start = time.time()

final_df: pd.DataFrame = ai_infer(input_data=DF)
final_df.to_csv(
    "/dbfs/mnt/adls/NMLUONG/NEW_ENTITY_OUTPUT.csv", index=False
)

print(f"Execution time: {time.time() - start} for {final_df.shape[0]} records")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


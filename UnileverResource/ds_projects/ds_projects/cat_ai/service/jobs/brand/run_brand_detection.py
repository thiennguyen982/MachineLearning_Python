# Databricks notebook source
# MAGIC %pip install black tokenize-rt xlsxwriter openpyxl loguru
# MAGIC %pip install fuzzysearch fuzzywuzzy 
# MAGIC %pip install emoji unidecode python-Levenshtein
# MAGIC %pip install tqdm openpyxl ray
# MAGIC %pip install fastparquet

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Import modules

# COMMAND ----------

# MAGIC %run "./brand_detector_nb"

# COMMAND ----------

# MAGIC %run "./preprocess_nb"

# COMMAND ----------

# MAGIC %run "./dictionary_nb"

# COMMAND ----------

import os
from loguru import logger
import pandas as pd
import numpy as np
import ray
import time

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Run Brand detection

# COMMAND ----------

def ai_infer(
    input_data: pd.DataFrame,
):
    # create copy of data
    final_data = input_data.copy(deep=True)
    dict_brand_cate, dict_ls_brand_cate = open_brand_dict(
        brand_path="/dbfs/mnt/adls/AI-Item/Dictionary/BRAND"
    )

    # run BRAND detection
    sent_list_group = np.array_split(final_data["TITLE"], os.cpu_count() * 4)
    category_list_group = np.array_split(
        final_data["CATEGORY_PRED"], os.cpu_count() * 4
    )
    skuid_list_group = np.array_split(final_data["SKUID"], os.cpu_count() * 4)

    tasks = [
        REMOTE_get_brand.remote(
            skuid_list, sent_list, dict_brand_cate, dict_ls_brand_cate, category_list
        )
        for skuid_list, sent_list, category_list in zip(
            skuid_list_group, sent_list_group, category_list_group
        )
    ]
    tasks = ray.get(tasks)

    ray.shutdown()

    import pickle

    with open("tasks.pkl", "wb") as f:
        pickle.dump(tasks, f)

    # re-structure output
    ai_result = []
    for task in tasks:
        ls_task = []
        for i in task:
            ls_task.append(i)
        ai_result.extend(ls_task)

    return_data = pd.DataFrame()
    return_data[["SKUID", "BRAND_PRED", "BRAND_SUGGEST"]] = pd.DataFrame(ai_result)
    return_data["BRAND_PRED"] = return_data["BRAND_PRED"].astype(str)
    return_data["BRAND_SUGGEST"] = return_data["BRAND_SUGGEST"].astype(str)

    # TODO: remove this line
    return return_data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Read CATEGORY output

# COMMAND ----------

DF = pd.read_parquet("/dbfs/mnt/adls/NMLUONG/TEMP_OUTPUT/CATEGORY_OUTPUT.parquet")

# filter category
category_mask = DF["CATEGORY_PRED"].isin(["oral"])
DF = DF.loc[category_mask, :]

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Run AI

# COMMAND ----------

import time

start = time.time()

final_df: pd.DataFrame = ai_infer(input_data=DF)
final_df.to_parquet(
    "/dbfs/mnt/adls/NMLUONG/TEMP_OUTPUT/BRAND_OUTPUT.parquet", index=False
)

print(f"Execution time: {time.time() - start} for {final_df.shape[0]} records")

# COMMAND ----------


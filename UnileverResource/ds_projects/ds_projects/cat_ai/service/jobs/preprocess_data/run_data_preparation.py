# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Import modules

# COMMAND ----------

# MAGIC %run "../preprocess_nb"

# COMMAND ----------

import os
import pandas as pd
import glob
from tqdm.notebook import tqdm

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Prepare input data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Read BEECOST

# COMMAND ----------

retailer = dbutils.widgets.get("retailer")
retailer

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

udf_main_preprocess = F.udf(lambda x: main_preprocess(x), StringType())
df = (
    spark.read.format("delta")
    .table("pureplay.dim_pureplay_product")
    .filter(f'retailer=="{retailer}"')
    .select(
        "product_uid", "product_id", "title" # udf_main_preprocess(F.col("title")).alias("title")
    )
    .distinct()
    .filter(F.col("title").isNotNull())
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Read last result

# COMMAND ----------

ls_last_round_file = glob.glob(f"/dbfs/mnt/adls/AI-Item/last_round/{retailer}/*.xlsx")
df_last_round_total = None
for file_last_round in tqdm(ls_last_round_file):
    df_last_round = pd.read_excel(
        file_last_round, sheet_name="RAW_RESULT"
    )
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

    if df_last_round_total is None:
        df_last_round_total = df_last_round
    else:
        df_last_round_total = pd.concat([df_last_round_total, df_last_round])

if df_last_round_total is not None:
    # because spark use iteritems instead of items
    # I need to replicate this funciton by the following line of code
    df_last_round_total.iteritems = df_last_round_total.items
    df_last_round_total = spark.createDataFrame(df_last_round_total)
else:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exclude last result

# COMMAND ----------

if df_last_round_total is not None:
    df = (
        df.alias("beecost")
        .join(
            df_last_round_total, on=[df.product_id == df_last_round_total.SKUID], how="left"
        )
        .filter("SKUID is NULL")
        .select("product_uid", "beecost.title")
    )
else:
    df = df.select("product_uid", "title")

# COMMAND ----------

df = (
    df.withColumnRenamed("title", "TITLE")
    .withColumnRenamed("product_uid", "PRODUCT_UID")
    .withColumnRenamed("product_id", "SKUID")
)
df = df.toPandas()

# COMMAND ----------

df.shape

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Save input data

# COMMAND ----------

df.to_parquet(
    f"/dbfs/mnt/adls/AI-Item/NEW_METHOD/{retailer}/INPUT.parquet", index=False
)

# COMMAND ----------


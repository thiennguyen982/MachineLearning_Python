# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Import modules

# COMMAND ----------

import os
import pandas as pd
from tqdm.notebook import tqdm
import glob

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Prepare input data

# COMMAND ----------

from pyspark.sql.types import StringType

udf_main_preprocess = F.udf(lambda x: main_preprocess(x), StringType())
df = (
    spark.read.format("delta")
    .table("pureplay.dim_pureplay_product")
    .filter('retailer=="shopee"')
    .select(
        "product_uid", "product_id", "title" # udf_main_preprocess(F.col("title")).alias("title")
    )
    .distinct()
    .filter(F.col("title").isNotNull())
)

# COMMAND ----------

df = (
    df.withColumnRenamed("title", "TITLE")
    .withColumnRenamed("product_uid", "PRODUCT_UID")
    .withColumnRenamed("product_id", "SKUID")
)
df = df.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Read last result

# COMMAND ----------

LAST_ROUND_PATH = f"/dbfs/mnt/adls/AI-Item/last_round/"
ls_last_round_file = glob.glob("/dbfs/mnt/adls/AI-Item/Final_after_exclude/20230929/20:59:39/*.xlsx")
# ls_last_round_file = ["/dbfs/mnt/adls/AI-Item/Final_after_exclude/20230929/20:59:39/*"]
df_last_round_total = None
for file_last_round in tqdm(ls_last_round_file):
    df_last_round = pd.read_excel(
        file_last_round, 
        sheet_name="FINAL_NRM_REVIEW", 
        engine="openpyxl"
    )
    df_last_round = df_last_round[
        [
            "SKUID",
            "TITLE",
            "CATEGORY_PREDICT",
            "FINAL_ENTITY",
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
    df_last_round_total = pd.concat([df_last_round_total, df_last_round])

# remove the NULL row in the subset of TITLE
df_last_round_total = df_last_round_total[df_last_round_total["TITLE"].notnull()]

# remove the the NULL row in the subset of FORMAT
# df_last_round_total = df_last_round_total[
#     df_last_round_total["FORMAT_PREDICT"].notnull()
# ]

# because spark use iteritems instead of items
# I need to replicate this funciton by the following line of code
df_last_round_total.iteritems = df_last_round_total.items
df_last_round_total = spark.createDataFrame(df_last_round_total)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Filter last run

# COMMAND ----------

# df = (
#     df.alias("beecost")
#     .join(
#         df_last_round_total, on=[df.product_id == df_last_round_total.SKUID], how="left"
#     )
#     .filter("SKUID is NULL")
#     .select("product_uid", "beecost.title")
# )

# COMMAND ----------

DF = df_last_round_total.where(F.col("TYPE") == "old").where(
    F.col("BRAND_PREDICT") == "not-detected"
)
DF = DF.toPandas()

# COMMAND ----------

DF = DF.drop_duplicates(subset=["SKUID"], keep="last")

# COMMAND ----------

DF = DF[
    [
        "SKUID",
        "TITLE",
        "CATEGORY_PREDICT",
        "FINAL_ENTITY",
        "BRAND_PREDICT",
        "BRAND_SUGGESTION",
        "PACKTYPE_PREDICT",
        "FORMAT_PREDICT",
        "SEGMENT_PREDICT",
    ]
]

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Save input data

# COMMAND ----------

DF.to_parquet(f"/dbfs/mnt/adls/AI-Item/NEW_METHOD/INPUT.parquet", index=False)

# COMMAND ----------


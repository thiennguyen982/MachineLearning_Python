# Databricks notebook source
PATH_RAW_PRODUCT = "/mnt/adls/SGC_BIGC_DS/MASTER/Product Master.xlsx"
product_df = spark.read.format("com.crealytics.spark.excel") \
        .option("header", "true") \
        .load(PATH_RAW_PRODUCT)

# COMMAND ----------

product_df.pandas_api().columns

# COMMAND ----------


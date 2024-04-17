# Databricks notebook source
dbutils.notebook.run("./00_Sec Sales_Pri Sales_Promo_Transform", timeout_seconds=10000)

# COMMAND ----------

dbutils.notebook.run("./01_SNOP_Baseline_Clean", timeout_seconds=10000)

# COMMAND ----------

dbutils.notebook.run("./02_Forecasting_Model", timeout_seconds=10000)

# COMMAND ----------

dbutils.notebook.run("./03_Convert_Sec2Pri_Baseline", timeout_seconds=10000)
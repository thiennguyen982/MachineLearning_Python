# Databricks notebook source
# MAGIC %sql
# MAGIC select 
# MAGIC to_date(billing_date) as ds,
# MAGIC cast(est_demand_cs as bigint) as est_demand
# MAGIC from mt_predicted_order.mt_sale
# MAGIC where 1=1
# MAGIC and to_date(billing_date) >='2023-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create mt_dashboard dataset
# MAGIC CREATE SCHEMA IF NOT EXISTS mt_feature_store LOCATION '/mnt/adls/mt_feature_store'

# COMMAND ----------

_sqldf.write.mode("overwrite").format("delta").saveAsTable("mt_dashboard.mt_sale")


# COMMAND ----------


# Databricks notebook source
from multiprocessing.pool import ThreadPool
pool = ThreadPool(2)
notebooks = ['f1','f2']

pool.map(lambda path: dbutils.notebook.run(path, timeout_seconds= 60, arguments={"input-data": path}),notebooks)

# COMMAND ----------

flag = dbutils.widgets.get("a")
print(flag)

# COMMAND ----------


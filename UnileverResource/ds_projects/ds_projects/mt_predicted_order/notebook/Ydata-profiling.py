# Databricks notebook source
import os
import sys
import numpy as np 
import ydata_profiling
import pyspark.pandas as ps


# mlflow
import mlflow
from mlflow import MlflowClient
from mlflow.models import infer_signature
## System metrics
os.environ["MLFLOW_ENABLE_SYSTEM_METRICS_LOGGING"] = "true"
mlflow.enable_system_metrics_logging()


from typing import Optional, Union, List, Dict, Any


# Utils function
MOUNT_DS_PROJECT = "/Workspace/Repos/dao-minh.toan@unilever.com/ds_projects/"
sys.path.append(MOUNT_DS_PROJECT)
from mt_predicted_order.common.mlflow import MlflowService


# Setup MLflow Experiment
EXPERIMENT_NAME = "/Users/dao-minh.toan@unilever.com/YDATA_PROFILING"
mlflowService: MlflowService = MlflowService(EXPERIMENT_NAME)

# COMMAND ----------

df_spark: ps.DataFrame = spark.read.format("delta").load("/mnt/adls/mt_feature_store/data_halfweek_fe")
df = df_spark.toPandas()
display(df_spark)

# COMMAND ----------


from ydata_profiling import ProfileReport


profile = ProfileReport(df)

profile.to_file("/dbfs/mnt/adls/data_profiling/report_profile.html")
with mlflow.start_run(run_id = mlflowService.run_id):
  mlflow.log_artifact("/dbfs/mnt/adls/data_profiling/report_profile.html", "html_files")
# report_html = profile.to_html()
# displayHTML(report_html)

# COMMAND ----------

profile = ProfileReport(df, tsmode=True, sortby="ds", title="Time-Series EDA")
profile.to_file("/dbfs/mnt/adls/data_profiling/report_timeseries.html")
with mlflow.start_run(run_id = mlflowService.run_id):
  mlflow.log_artifact("/dbfs/mnt/adls/data_profiling/report_timeseries.html", "html_files")
